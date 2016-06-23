(* Automatically hedge your BitMEX positions on Bitfinex/Poloniex *)

open Core.Std
open Async.Std
open Log.Global

open Bs_devkit.Core

open Bs_api

let log_bfx = Log.create ~level:`Error ~on_error:`Raise ~output:Log.Output.[stderr ()]

let bfx_key = ref ""
let bfx_secret = ref @@ Cstruct.of_string ""

let channels : BFX.Ws.Msg.chan_descr Int.Table.t = Int.Table.create ()
let subscriptions : int String.Table.t = String.Table.create ()

let bfx_positions : BFX.Ws.Priv.Position.t String.Table.t = String.Table.create ()
let bfx_hb = ref Time_ns.epoch

let maybe_hedge p = ()

type order = { price: int; qty: int }

let bfx_orders : (order Int.Table.t) String.Table.t = String.Table.create ()
let bfx_bids : (Int.t Int.Map.t) String.Table.t = String.Table.create ()
let bfx_asks : (Int.t Int.Map.t) String.Table.t = String.Table.create ()
let bfx_bid_vwaps : (int * int) String.Table.t = String.Table.create ()
let bfx_ask_vwaps : (int * int) String.Table.t = String.Table.create ()

let tickups_r, tickups_w = Pipe.create ()

let to_remote_sym = function
  | "XBTUSD" -> "BTCUSD"
  | "LTCUSD" -> "LTCUSD"
  | "ETHXBT" -> "BTC_ETH"
  | "LSKXBT" -> "BTC_LSK"
  | "DAOETH" -> "ETH_DAO"
  | "FCTXBT" -> "BTC_FCT"
  | _  -> invalid_arg "to_remote_sym"

let of_remote_sym = function
  | "BTCUSD" -> "XBTUSD"
  | "LTCUSD" -> "LTCUSD"
  | "BTC_ETH" -> "ETHXBT"
  | "BTC_LSK" -> "LSKXBT"
  | "ETH_DAO" -> "DAOETH"
  | "BTC_FCT" -> "FTCXBT"
  | _  -> invalid_arg "to_remote_sym"

let on_bfx_book_partial sym os =
  let orders = String.Table.find_exn bfx_orders sym in
  let fold_f (bids, asks) { BFX.Ws.Book.Raw.id; price; amount } =
    let price = satoshis_int_of_float_exn price in
    let qty = bps_int_of_float_exn amount in
    Int.Table.set orders id { price; qty };
    match Int.sign qty with
    | Zero -> invalid_arg "on_bfx_book_partial"
    | Pos ->
      let new_bids = Int.Map.update bids price ~f:(Option.value_map ~default:qty ~f:(fun old_qty -> old_qty + qty)) in
      new_bids, asks
    | Neg ->
      let qty = Int.neg qty in
      let new_asks = Int.Map.update asks price ~f:(Option.value_map ~default:qty ~f:(fun old_qty -> old_qty + qty)) in
      bids, new_asks
  in
  let bids, asks = List.fold_left os ~init:(Int.Map.empty, Int.Map.empty) ~f:fold_f in
  String.Table.set bfx_bids sym bids;
  String.Table.set bfx_asks sym asks;
  Log.debug log_bfx "Initialized BFX order books"

let on_bfx_book_update sym { BFX.Ws.Book.Raw.id; price = new_price; amount = new_qty } =
  let book_add_qty book price qty =
    Int.Map.update book price ~f:(function None -> qty | Some oldq -> oldq + qty)
  in
  let book_modify_qty_exn book price qty =
    Int.Map.update book price ~f:(function None -> invalid_arg "book_modify_qty" | Some oldq -> oldq + qty)
  in
  let book_modify_qty book price qty =
    try book_modify_qty_exn book price qty with _ ->
      Log.error log_bfx "update_depth: inconsistent orderbooks!";
      book
  in
  let orders = String.Table.find_exn bfx_orders sym in
  let side, books, vwaps = match Float.sign_exn new_qty with
    | Zero -> invalid_arg "on_book_update"
    | Pos -> Side.Bid, bfx_bids, bfx_bid_vwaps
    | Neg -> Ask, bfx_asks, bfx_ask_vwaps
  in
  let new_price, new_qty = if new_price = 0. then None, None else
      Some (satoshis_int_of_float_exn new_price),
      Some (bps_int_of_float_exn new_qty)
  in
  let abs_new_qty = Option.map new_qty Int.abs in
  let action = if Option.is_none new_price then BMEX.Delete else Insert in
  let old_book = String.Table.find_exn books sym in
  let new_book = match action, new_price, abs_new_qty, Int.Table.find orders id with
    | Delete, None, None, Some { price = old_price; qty = old_qty } ->
      Int.Table.remove orders id;
      book_modify_qty old_book old_price (Int.neg old_qty)
    | Insert, Some newp, Some newq, None ->
      Int.Table.set orders id { price = newp; qty = Option.value_exn new_qty };
      book_add_qty old_book newp newq
    | Insert, Some newp, Some newq, Some { price = old_price; qty = old_qty } ->
      Int.Table.set orders id { price = newp; qty = Option.value_exn new_qty };
      let new_book = book_modify_qty old_book old_price (Int.neg newq) in
      book_modify_qty new_book newp newq
    | _ ->
      Log.error log_bfx "Our internal orderbook is inconsistent with a BFX update!";
      old_book
  in
  String.Table.set books sym new_book;

  (* computation of VWAP *)
  let vlimit = String.Table.find subscriptions sym in
  let old_vwap = String.Table.find vwaps sym in
  let new_vwap, total_v = vwap ?vlimit side new_book in
  String.Table.set vwaps sym (new_vwap, total_v);
  let best_elt_f = Int.Map.(match side with Side.Bid -> max_elt | Ask -> min_elt) in
  let old_best_elt = Option.map ~f:fst @@ best_elt_f old_book in
  let new_best_elt = Option.map ~f:fst @@ best_elt_f new_book in
  begin match old_best_elt, new_best_elt, old_vwap with
    | Some old_best, Some new_best, Some (old_vwap, _) when old_best <> new_best || old_vwap <> new_vwap ->
      Pipe.write_without_pushback tickups_w @@ Protocols.OrderBook.create_ticker ~symbol:sym ~side ~best:new_best ~vwap:new_vwap ();
    | _ -> ()
  end;
  Log.debug log_bfx "BFX %s %s vwap=%.2f totalv=%.3f" sym (Side.show side) Int.(to_float new_vwap /. to_float total_v *. 1e-8) (Int.to_float total_v /. 1e4)

let subscribe_r, subscribe_w = Pipe.create ()

let bfx_ws buf =
  let open BFX in
  let on_ws_msg msg_str =
    let now = Time_ns.now () in
    let msg = Yojson.Safe.from_string ~buf msg_str in
    match Ws.Ev.of_yojson msg with
    | {Ws.Ev.name = "subscribed"; fields } ->
      let channel = String.Map.find_exn fields "channel" in
      let pair = String.Map.find_exn fields "pair" in
      let chanId = String.Map.find_exn fields "chanId" in
      begin match channel, pair, chanId with
        | `String c, `String p, `Int chanId ->
          let data =
            Ws.Msg.(create_chan_descr
                      ~chan:(channel_of_string c)
                      ~pair:p ()
                   )
          in
          Int.Table.set channels ~key:chanId ~data;
        | #Yojson.Safe.json, #Yojson.Safe.json, #Yojson.Safe.json ->
          invalid_arg "wrong subscribed msg received"
      end
    | { Ws.Ev.name; fields } -> ()
    | exception (Invalid_argument _) ->
      match Ws.Msg.of_yojson msg with
      | { Ws.Msg.chan; msg = [`String "hb"] } -> bfx_hb := now
      | { chan = 0; msg = (`String typ) :: tl } -> begin
          let msg_type, update_type = Ws.Priv.types_of_msg typ in
          let tl = match tl with
            | [] -> []
            | [`List []] -> []
            | [`List (( `List _ :: _) as e)] -> e
            | [`List msg] -> [`List msg]
            | #Yojson.Safe.json :: _ ->
              invalid_arg "client_ws: on_ws_msg: got unexpected msg payload"
          in
          Log.debug log_bfx "-> %s %s"
            (Ws.Priv.show_msg_type msg_type)
            (Ws.Priv.show_update_type update_type);
          match msg_type, update_type with
          | Position, _ ->
            List.iter tl ~f:(fun json ->
                let p = Ws.Priv.Position.of_yojson json in
                String.Table.set bfx_positions ~key:p.pair ~data:p;
              )
          | _msg_type, _update_type -> ()
        end
      | { chan; msg } ->
        match Int.Table.find_exn channels chan with
        | { chan = Book; pair } ->
          let updates =
            match msg with
            | (`List msg') :: _ ->
              List.map msg'
                ~f:(function
                    | `List update -> Ws.Book.Raw.of_yojson update
                    | #Yojson.Safe.json -> invalid_arg "update")
            | _ ->
              [Ws.Book.Raw.of_yojson msg]
          in
          let sym = of_remote_sym pair in
          begin match updates with
            | [] -> ()
            | [u] ->
              (* debug "<- %s" @@ Ws.Book.Raw.show u; *)
              on_bfx_book_update sym u
            | partial -> on_bfx_book_partial sym partial
          end
        | _ -> ()
  in
  Ws.with_connection ~log:(Lazy.force log) ~auth:(!bfx_key, !bfx_secret) ~to_ws:subscribe_r ~on_ws_msg ()

let rpc_server port =
  let module P = Protocols in
  let orderbook_f state query writer =
    match query with
    | P.OrderBook.Subscribe symbols ->
      debug "RPC <- Subscribe";
      let handle_sub (sym, max_pos) =
        let open Rpc.Pipe_rpc in
        match String.Table.find subscriptions sym with
        | Some _ -> String.Table.set subscriptions sym max_pos
        | None ->
          String.Table.set subscriptions sym max_pos;
          String.Table.set bfx_orders sym (Int.Table.create ());
          String.Table.set bfx_bids sym (Int.Map.empty);
          String.Table.set bfx_asks sym (Int.Map.empty);
          Pipe.write_without_pushback subscribe_w @@
          BFX.Ws.Ev.create "subscribe" ["channel", `String "book";
                                        "pair", `String (to_remote_sym sym);
                                        "prec", `String "R0"] ();
      in
      List.iter symbols ~f:handle_sub;
      begin try_with ~here:[%here]
          (fun () ->
             Pipe.iter tickups_r ~f:(fun t ->
                 if String.Table.mem subscriptions t.symbol then begin
                   match Rpc.Pipe_rpc.Direct_stream_writer.write writer (P.OrderBook.Ticker t) with
                   | `Closed -> Deferred.unit (* TODO: Unsubscribe *)
                   | `Flushed v ->
                     debug "tickup sent";
                     v
                 end
                 else Deferred.unit
               )
          ) >>= function
        | Ok () -> Deferred.Or_error.return ()
        | Error exn ->
          error "rpc_server %s" Exn.(to_string exn);
          Deferred.Or_error.of_exn exn
      end
    | Unsubscribe symbols ->
      (* TODO: Unsubscribe *)
      debug "RPC <- Unsubscribe";
      List.iter symbols ~f:(String.Table.remove subscriptions);
      Deferred.Or_error.return ()
  in
  let position_f state query =
    debug "RPC <- position";
    ()
  in
  let orderbook_impl = Rpc.Pipe_rpc.implement_direct P.OrderBook.t orderbook_f in
  let position_impl = Rpc.Rpc.implement' P.Position.t position_f in
  let implementations = Rpc.Implementations.create_exn [orderbook_impl; position_impl] `Raise in
  let state_of_addr_conn addr conn = () in
  Rpc.Connection.serve implementations state_of_addr_conn (Tcp.on_port port) ()

let main cfg port daemon pidfile logfile loglevel () =
  let cfg = Yojson.Safe.from_file cfg |> Cfg.of_yojson |> presult_exn in
  let { Cfg.key; secret } = List.Assoc.find_exn cfg "BFX" in
  bfx_key := key;
  bfx_secret := Cstruct.of_string secret;
  let buf = Bi_outbuf.create 4096 in
  if daemon then Daemon.daemonize ~cd:"." ();
  set_output Log.Output.[stderr (); file `Text ~filename:logfile];
  Log.set_output log_bfx Log.Output.[stderr (); file `Text ~filename:logfile];
  set_level (match loglevel with 2 -> `Info | 3 -> `Debug | _ -> `Error);
  Log.set_level log_bfx (match loglevel with 2 -> `Info | 3 -> `Debug | _ -> `Error);
  Out_channel.write_all pidfile ~data:(Unix.getpid () |> Pid.to_string);
  don't_wait_for @@ bfx_ws buf;
  don't_wait_for @@ Deferred.ignore @@ rpc_server port;
  never_returns @@ Scheduler.go ()

let command =
  let default_cfg = Filename.concat (Option.value_exn (Sys.getenv "HOME")) ".virtu" in
  let spec =
    let open Command.Spec in
    empty
    +> flag "-cfg" (optional_with_default default_cfg string) ~doc:"path Filepath of config file (default: ~/.virtu)"
    +> flag "-port" (optional_with_default 53232 int) ~doc:"int port to listen to (default: 53232)"
    +> flag "-daemon" no_arg ~doc:" Run as a daemon"
    +> flag "-pidfile" (optional_with_default "run/hedger.pid" string) ~doc:"filename Path of the pid file (run/hedger.pid)"
    +> flag "-logfile" (optional_with_default "log/hedger.log" string) ~doc:"filename Path of the log file (log/hedger.log)"
    +> flag "-loglevel" (optional_with_default 1 int) ~doc:"1-3 loglevel"
  in
  Command.basic ~summary:"Hedger bot" spec main

let () = Command.run command
