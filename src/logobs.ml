open Core
open Async
open Log.Global

open Bs_devkit
module Yojson_encoding = Json_encoding.Make(Json_repr.Yojson)

let bmex_dbs = String.Table.create ()
let plnx_dbs = String.Table.create ()

let make_store () =
  let key = String.create 8 in
  let buf = Bigbuffer.create 128 in
  let scratch = Bigstring.create 128 in
  fun ?sync db seq evts ->
    Bigbuffer.clear buf;
    Binary_packing.pack_signed_64_int_big_endian ~buf:key ~pos:0 seq;
    let nb_of_evts = List.length evts |> Bin_prot.Nat0.of_int in
    let nb_written = Bin_prot.Write.bin_write_nat0 scratch ~pos:0 nb_of_evts in
    let scratch_shared = Bigstring.sub_shared scratch ~len:nb_written in
    Bigbuffer.add_bigstring buf scratch_shared;
    List.iter evts ~f:begin fun e ->
      let nb_written = Bs_devkit.DB.bin_write_t scratch ~pos:0 e in
      let scratch_shared = Bigstring.sub_shared scratch ~len:nb_written in
      Bigbuffer.add_bigstring buf scratch_shared;
    end;
    LevelDB.put ?sync db key @@ Bigbuffer.contents buf

module BMEX = struct
  open Bmex
  open Bmex_ws

  let update_of_l2 { OrderBook.L2.id ; side ; size ; price } =
    let price = Option.value_map price ~default:0 ~f:satoshis_int_of_float_exn in
    let qty = Option.value ~default:0 size in
    match side with
    | Some `Buy ->  Some DB.{ side = `Buy ; price ; qty }
    | Some `Sell -> Some DB.{ side = `Sell ; price ; qty }
    | None -> None

  let trade_of_bmex { Trade.symbol ; timestamp = ts ; side ; price ; size } =
    let price = satoshis_int_of_float_exn price in
    match side with
    | Some `Buy -> Some DB.(Trade { ts ; side = `Buy ; price ; qty = size })
    | Some `Sell -> Some DB.(Trade { ts ; side = `Sell ; price ; qty = size })
    | None -> None

  let evt_of_update_action up = function
  | Response.Update.Partial | Insert | Update -> DB.BModify up
  | Delete -> BRemove up

  let make_on_evt () =
    let store = make_store () in
    fun kind action data ->
      let now = Time_ns.(now () |> Time_ns.to_int_ns_since_epoch) in
      match kind with
      | `L2 ->
        (* debug "<- %s" (Yojson.Safe.to_string (`List data)); *)
        let data = List.map data ~f:(Yojson_encoding.destruct OrderBook.L2.encoding) in
        let groups = List.group data ~break:(fun u u' -> u.symbol <> u'.symbol) in
        let iter_f = function
        | [] -> ()
        | { OrderBook.L2.symbol } :: t as ups ->
          let db = String.Table.find_exn bmex_dbs symbol in
          let data = List.filter_map ups ~f:begin fun u ->
              update_of_l2 u |>
              Option.map ~f:(fun u -> evt_of_update_action u action)
            end in
          debug "%s" (DB.sexp_of_t_list data |> Sexp.to_string);
          store db now data
        in
        List.iter groups ~f:iter_f
      | `Trade ->
        let data = List.map data ~f:(Yojson_encoding.destruct Trade.encoding) in
        let iter_f t =
          let db = String.Table.find_exn bmex_dbs t.Trade.symbol in
          let data = List.filter_map data ~f:trade_of_bmex in
          debug "%s" (DB.sexp_of_t_list data |> Sexp.to_string);
          store db now data
        in
        List.iter data ~f:iter_f

  let record symbols =
    let topics = List.(map symbols ~f:(fun s -> ["orderBookL2:" ^ s; "trade:" ^ s]) |> concat) in
    let on_evt = make_on_evt () in
    let ws = open_connection ~md:false ~testnet:false ~topics () in
    let on_ws_msg msg_json =
      match Yojson_encoding.destruct Response.encoding msg_json with
      | Update { table; action; data } -> begin
        match table with
        | "orderBookL2" -> on_evt `L2 action data
        | "trade" -> on_evt `Trade action data
        | _ -> error "Invalid table %s" table
      end
      | _ -> ()
    in
    Monitor.handle_errors
      (fun () -> Pipe.iter_without_pushback ~continue_on_error:true ws ~f:on_ws_msg)
      (fun exn -> error "%s" @@ Exn.to_string exn)
end

module PLNX = struct
  module Rest = Plnx_rest
  open Plnx_ws

  let polo_of_symbol = function
  | "ETHXBT" -> "BTC_ETH"
  | "MAIDXBT" -> "BTC_MAID"
  | _ -> invalid_arg "polo_of_symbol"

  let record symbols =
    let syms_polo = List.map symbols ~f:polo_of_symbol in
    let store = make_store () in
    let to_ws, to_ws_w = Pipe.create () in
    let symbols_of_req_ids = ref [] in
    let symbols_of_sub_ids = Int.Table.create () in
    let on_ws_msg = function
    | Wamp.Welcome _ ->
      M.subscribe to_ws_w syms_polo >>| fun reqids ->
      symbols_of_req_ids := Option.value_exn ~message:"Ws.subscribe" (List.zip reqids symbols)
    | Subscribed { reqid; id } ->
      let sym = List.Assoc.find_exn ~equal:Int.(=) !symbols_of_req_ids reqid in
      let sym_polo = polo_of_symbol sym in
      Int.Table.set symbols_of_sub_ids id sym;
      let db = String.Table.find_exn plnx_dbs sym in
      let rec loop () =
        Rest.books sym_polo >>= function
        | Ok { Rest.Books.asks; bids; seq } ->
          let evts = List.map (bids @ asks) ~f:begin fun { side; price; qty } ->
              let price = satoshis_int_of_float_exn price in
              let qty = satoshis_int_of_float_exn qty in
              DB.BModify { side ; price ; qty }
            end in
          store db seq evts;
          info "stored %d %s partial" (List.length evts) sym;
          Deferred.unit
        | Error err ->
          error "%s" (Plnx_rest.Http_error.to_string err);
          Clock_ns.after @@ Time_ns.Span.of_int_sec 10 >>=
          loop
      in
      don't_wait_for @@ loop ();
      debug "subscribed %s" sym;
      Deferred.unit
    | Event { pubid; subid; details; args; kwArgs } ->
      let sym = Int.Table.find_exn symbols_of_sub_ids subid in
      let db = String.Table.find_exn plnx_dbs sym in
      let seq = match List.Assoc.find_exn ~equal:String.(=) kwArgs "seq" with
      | Msgpck.Int i -> i
      | _ -> failwith "seq" in
      let map_f msg =
        match of_msgpck msg with
        | Error msg -> failwith msg
        | Ok { typ="newTrade"; data } ->
          let trade = M.read_trade data in
          let price = satoshis_int_of_float_exn trade.price in
          let qty = satoshis_int_of_float_exn trade.qty in
          let trade = { DB.ts = trade.ts ; side = trade.side ; price ; qty } in
          debug "%d T %s" seq @@ Fn.compose Sexp.to_string  DB.sexp_of_trade trade;
          DB.Trade trade
        | Ok { typ="orderBookModify"; data } ->
          let update = M.read_book data in
          let price = satoshis_int_of_float_exn update.price in
          let qty = satoshis_int_of_float_exn update.qty in
          let update = { DB.side = update.side ; price ; qty } in
          debug "%d M %s" seq @@ Fn.compose Sexp.to_string DB.sexp_of_book_entry update;
          DB.BModify update
        | Ok { typ="orderBookRemove"; data } ->
          let update = M.read_book data in
          let price = satoshis_int_of_float_exn update.price in
          let qty = satoshis_int_of_float_exn update.qty in
          let update = { DB.side = update.side ; price ; qty } in
          debug "%d D %s" seq @@ Fn.compose Sexp.to_string DB.sexp_of_book_entry update;
          DB.BRemove update
        | Ok { typ } -> failwithf "unexpected message type %s" typ ()
      in
      store db seq @@ List.map args ~f:map_f;
      Deferred.unit
    | msg ->
      (* error "unknown message: %s" (Wamp.sexp_of_msg Msgpck.sexp_of_t msg |> Sexplib.Sexp.to_string); *)
      Deferred.unit
    in
    let ws = open_connection to_ws in
    Monitor.handle_errors
      (fun () -> Pipe.iter ~continue_on_error:true ws ~f:on_ws_msg)
      (fun exn -> error "%s" @@ Exn.to_string exn)
end

let (//) = Filename.concat
let record daemon rundir logdir datadir loglevel instruments () =
  let instruments = List.fold_left instruments
      ~init:String.Map.empty ~f:begin fun a (exchange, symbol) ->
      String.Map.add_multi a exchange symbol
    end in
  let executable_name = Sys.executable_name |> Filename.basename |> String.split ~on:'.' |> List.hd_exn in
  let pidfile = Filename.concat rundir @@ executable_name ^ ".pid" in
  let logfile = Filename.concat logdir @@ executable_name ^ ".log" in
  Signal.handle Signal.terminating ~f:begin fun _ ->
    info "OB logger stopping.";
    String.Table.iter bmex_dbs ~f:LevelDB.close;
    String.Table.iter plnx_dbs ~f:LevelDB.close;
    info "Saved %d dbs." @@ String.Table.(length bmex_dbs + length plnx_dbs);
    don't_wait_for (Shutdown.exit 0)
    end;
  if daemon then Daemon.daemonize ~cd:"." ();
  stage begin fun `Scheduler_started ->
    Unix.mkdir ~p:() rundir >>= fun () ->
    Unix.mkdir ~p:() logdir >>= fun () ->
    let bitmex_dir = datadir // "bitmex" in
    let plnx_dir = datadir // "poloniex" in
    begin if String.Map.mem instruments "BMEX" then
        Unix.mkdir ~p:() bitmex_dir
      else Deferred.unit
    end >>= fun () ->
    begin if String.Map.mem instruments "PLNX" then
      Unix.mkdir ~p:() plnx_dir
      else Deferred.unit
    end >>= fun () ->
    Lock_file.create_exn pidfile >>= fun () ->
    let log_outputs filename = Log.Output.[stderr (); file `Text ~filename] in
    set_output @@ log_outputs logfile;
    set_level @@ loglevel_of_int loglevel;
    info "logobs starting";
    let bmex_record = Option.value_map
        (String.Map.find instruments "BMEX")
        ~default:Deferred.unit
        ~f:begin fun syms ->
          List.iter syms ~f:begin fun s ->
            let path = bitmex_dir // s in
            String.Table.set bmex_dbs s (LevelDB.open_db path)
          end;
          BMEX.record syms
        end in
    let plnx_record = Option.value_map
        (String.Map.find instruments "PLNX")
        ~default:Deferred.unit
        ~f:begin fun syms ->
          List.iter syms ~f:begin fun s ->
            let path = plnx_dir // s in
            String.Table.set plnx_dbs s (LevelDB.open_db path)
          end;
          PLNX.record syms
        end in
    Deferred.all_unit [ bmex_record ; plnx_record ]
  end

let record =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-daemon" no_arg ~doc:" Run as a daemon"
    +> flag "-rundir" (optional_with_default "run" string) ~doc:"filename Path of the run directory (default: run)"
    +> flag "-logdir" (optional_with_default "log" string) ~doc:"filename Path of the log directory (default: log)"
    +> flag "-datadir" (optional_with_default "data" string) ~doc:"filename Path of the data directory (default: data)"
    +> flag "-loglevel" (optional_with_default 1 int) ~doc:"1-3 loglevel"
    +> anon (sequence (t2 ("exchange" %: string) ("symbol" %: string)))
  in
  Command.Staged.async ~summary:"Log exchanges order books" spec record

let show datadir rev_iter max_ticks symbol () =
  let nb_read = ref 0 in
  let buf = Bigbuffer.create 4096 in
  let iter_f seq data =
    Bigbuffer.clear buf;
    Bigbuffer.add_string buf data;
    let seq = Binary_packing.unpack_signed_64_int_big_endian ~buf:seq ~pos:0 in
    let update = DB.bin_read_t_list ~pos_ref:(ref 0) @@ Bigbuffer.big_contents buf in
    begin if List.length update > 100 then
        Format.printf "%d (Partial)@." seq
      else
      Format.printf "%d %a@." seq Sexp.pp @@ DB.sexp_of_t_list update
    end;
    incr nb_read;
    Option.value_map max_ticks
      ~default:true ~f:(fun max_ticks -> not (!nb_read = max_ticks))
  in
  let iter_f = (if rev_iter then LevelDB.rev_iter else LevelDB.iter) iter_f in
  let dbpath = Filename.concat datadir symbol in
  Core.(match Sys.is_directory dbpath with
  | `No | `Unknown -> invalid_argf "No DB for %s" symbol ()
  | `Yes -> ());
  let db = LevelDB.open_db dbpath in
  Exn.protectx db ~finally:LevelDB.close ~f:iter_f

let show =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-datadir" (optional_with_default "data/bitmex" string) ~doc:"path Where to store DBs (data/bitmex)"
    +> flag "-rev-iter" no_arg ~doc:" Show latest records"
    +> flag "-n" (optional int) ~doc:"n Number of ticks to display (default: all)"
    +> anon ("symbol" %: string)
  in
  Command.basic ~summary:"Show LevelDB order book databases" spec show

let command =
  Command.group ~summary:"Manage order books logging" [
    "record", record;
    "show", show;
  ]

let () = Command.run command
