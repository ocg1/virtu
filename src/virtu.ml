(* Virtu: market making bot for BitMEX *)

open Core.Std
open Async.Std
open Log.Global

open Bs_devkit.Core
open Bs_api.BMEX

let api_key = ref ""
let api_secret = ref @@ Cstruct.of_string ""
let testnet = ref true
let dry_run = ref false
let hedger_port = ref 0
let quoted_instruments : int String.Table.t = String.Table.create ()

let quotes : Quote.t String.Table.t = String.Table.create ()
let instruments : RespObj.t String.Table.t = String.Table.create ()
let orders : RespObj.t Uuid.Table.t = Uuid.Table.create ()
let executions : RespObj.t Uuid.Table.t = Uuid.Table.create ()
let margin = ref @@ RespObj.of_json (`Assoc [])
let positions : RespObj.t String.Table.t = String.Table.create ()
let ticksizes : int String.Table.t = String.Table.create ()

let current_bids : Uuid.t String.Table.t = String.Table.create ()
let current_asks : Uuid.t String.Table.t = String.Table.create ()

let current_orders = function
  | Side.Bid -> current_bids
  | Ask -> current_asks

module RemoteOB = struct
  type ticker = { best: int; vwap: int }

  let best_bids : ticker String.Table.t = String.Table.create ()
  let best_asks : ticker String.Table.t = String.Table.create ()

  let tickers = function Side.Bid -> best_bids | Ask -> best_asks

  let mid_price symbol kind =
    let best_bid = String.Table.find best_bids symbol in
    let best_ask = String.Table.find best_asks symbol in
    Option.map2 best_bid best_ask ~f:(fun { best=bb; vwap=vb } { best=ba; vwap=va } ->
        match kind with
        | `Best -> (bb + ba) / 2
        | `Vwap -> (vb + vb) / 2
      )
end

module OB = struct
  type order = { price: int; qty: int }
  let orders : (order Int.Table.t) String.Table.t = String.Table.create ()

  let bids : (Int.t Int.Map.t) String.Table.t = String.Table.create ()
  let asks : (Int.t Int.Map.t) String.Table.t = String.Table.create ()
  let book_of_side = function Side.Bid -> bids | Ask -> asks

  let best_price side symbol =
    Option.Monad_infix.(String.Table.find (book_of_side side) symbol >>= Int.Map.max_elt)

  let bid_vwaps : Int.t String.Table.t = String.Table.create ()
  let ask_vwaps : Int.t String.Table.t = String.Table.create ()
  let vwaps_of_side = function Side.Bid -> bid_vwaps | Ask -> ask_vwaps

  let vwap side symbol =
    let vwaps = vwaps_of_side side in
    String.Table.find vwaps symbol

  let mid_price symbol = function
    | `Best ->
      let best_bid = best_price Side.Bid symbol in
      let best_ask = best_price Ask symbol in
      Option.map2 best_bid best_ask (fun (bb,_) (ba,_) -> (bb + ba) / 2)
    | `Vwap ->
      let vwap_bid = vwap Side.Bid symbol in
      let vwap_ask = vwap Ask symbol in
      Option.map2 vwap_bid vwap_ask (fun vb va -> (vb + va) / 2)
end

module Order = struct
  let submit orders =
    if !dry_run then begin
      info "[Sim] submit %s" Yojson.Safe.(to_string @@ `List orders);
      Deferred.Or_error.return ""
    end
    else
    Rest.Order.submit ~testnet:!testnet ~log:Lazy.(force log) ~key:!api_key ~secret:!api_secret orders

  let update orders =
    if !dry_run then begin
      info "[Sim] update %s" Yojson.Safe.(to_string @@ `List orders);
      Deferred.Or_error.return ""
    end
    else
    Rest.Order.update ~testnet:!testnet ~log:Lazy.(force log) ~key:!api_key ~secret:!api_secret orders

  let cancel orderID = Rest.Order.cancel ~testnet:!testnet ~log:Lazy.(force log) ~key:!api_key ~secret:!api_secret orderID
  let cancel_all ?symbol ?filter = Rest.Order.cancel_all ~testnet:!testnet ~log:Lazy.(force log) ~key:!api_key ~secret:!api_secret ?symbol ?filter
  let cancel_all_after timeout = Rest.Order.cancel_all_after ~testnet:!testnet ~log:Lazy.(force log) ~key:!api_key ~secret:!api_secret timeout
end

let dead_man's_switch timeout period =
  let rec loop () =
    let send_keepalive () =
      Order.cancel_all_after timeout >>| function
      | Error err ->
        error "%s" @@ Error.to_string_hum err;
      | Ok _ -> ()
    in try_with ~name:"dead man's switch" send_keepalive >>= function
    | Error exn ->
      error "%s" @@ Exn.to_string exn;
      loop ()
    | Ok () -> after @@ Time.Span.of_sec period >>= loop
  in loop ()

let mk_new_market_order ~symbol ~qty : Yojson.Safe.json =
  `Assoc [
    "symbol", `String symbol;
    "orderQty", `Int qty;
    "ordType", `String "Market";
  ]

let mk_new_limit_order ~symbol ~side ~price ~qty : Yojson.Safe.json =
  let price = Float.of_int price /. 1e8 in
  `Assoc [
    "clOrdID", `String Uuid.(create () |> to_string);
    "symbol", `String symbol;
    "side", `String (bmex_of_buy_sell side);
    "price", `Float price;
    "orderQty", `Int qty;
    "execInst", `String "ParticipateDoNotInitiate"
  ]

let mk_amended_limit_order ?price ?qty orderID : Yojson.Safe.json =
  let price = Option.map price ~f:(fun price -> Float.of_int price /. 1e8) in
  `Assoc (List.filter_opt [
    Some ("orderID", `String orderID);
    Option.map qty ~f:(fun qty -> "leavesQty", `Int qty);
    Option.map price ~f:(fun price -> "price", `Float price);
    ])

let compute_orders symbol side price order newQty =
  let orderID = Option.map order ~f:(fun o -> RespObj.string_exn o "orderID") in
  let leavesQty = Option.value_map ~default:0 order ~f:(fun o -> RespObj.int64_exn o "leavesQty" |> Int64.to_int_exn) in
  debug "compute_orders %s %d %d %d" symbol price leavesQty newQty;
  match leavesQty, newQty with
  | 0, 0 -> [], []
  | 0, qty -> [mk_new_limit_order ~symbol ~side ~price ~qty], []
  | qty, qty' -> [], [mk_amended_limit_order ~price ~qty:qty' Option.(value_exn orderID)]

let position_r, position_w = Pipe.create ()

let on_position_update (symbol: string) p =
  let max_pos_size = String.Table.find_exn quoted_instruments symbol in
  let currentQty = RespObj.int64_exn p "currentQty" |> Int64.to_int_exn in
  Pipe.write_without_pushback position_w (symbol, currentQty);
  let fw_p_to_hedger c = Rpc.Rpc.dispatch Protocols.Position.t c (symbol, currentQty) in
  don't_wait_for @@ Deferred.ignore @@ Rpc.Connection.with_client ~host:"localhost" ~port:!hedger_port fw_p_to_hedger;
  let tickSize = String.Table.find_exn ticksizes symbol in
  match OB.mid_price symbol `Best with
  | None -> ()
  | Some midPrice ->
    let bidPrice = midPrice - 5 * tickSize in
    let askPrice = midPrice + 5 * tickSize in
    let bidOrderID = String.Table.find current_bids symbol in
    let bidOrder = Option.bind bidOrderID (Uuid.Table.find orders) in
    let askOrderID = String.Table.find current_asks symbol in
    let askOrder = Option.bind askOrderID (Uuid.Table.find orders) in
    let currentBidQty = Option.value ~default:0 (RespObj.int64 p "openOrderBuyQty" |> Option.map ~f:Int64.to_int_exn) in
    let currentAskQty = Option.value ~default:0 (RespObj.int64 p "openOrderSellQty" |> Option.map ~f:Int64.to_int_exn) in
    let newBidQty = Int.max (max_pos_size - currentQty) 0 in
    let newAskQty = Int.max (max_pos_size + currentQty) 0 in
    if currentBidQty <> newBidQty || currentAskQty <> newAskQty then begin
      info "Updating orders to match current position: bid_qty=%d ask_qty=%d" newBidQty newAskQty;
      info "Found bid: %b, ask: %b" (Option.is_some bidOrderID) (Option.is_some askOrderID);
      let bidSubmit, bidAmend = compute_orders symbol `Buy bidPrice bidOrder newBidQty in
      let askSubmit, askAmend = compute_orders symbol `Sell askPrice askOrder newAskQty in
      let submit = bidSubmit @ askSubmit in
      let amend = bidAmend @ askAmend in
      if submit <> [] then don't_wait_for @@ Deferred.ignore @@ Order.submit submit;
      if amend <> [] then don't_wait_for @@ Deferred.ignore @@ Order.update amend
    end

let on_ticker_update { Protocols.OrderBook.symbol; side; best; vwap } =
  debug "on_ticker_update";
  let rtickers = RemoteOB.tickers side in
  match String.Table.find_or_add ~default:(fun () -> { best; vwap }) rtickers symbol with
  | { vwap=old_vwap } when old_vwap <> vwap -> begin
      String.Table.set rtickers symbol { best; vwap };
      let dvwap = vwap - old_vwap in
      match OB.best_price side symbol with
      | None -> Deferred.unit (* begin *)
        (*   let buy_sell = match side with Bid -> `Buy | Ask -> `Sell in *)
        (*   let qty = String.Table.find_exn quoted_instruments symbol in *)
        (*   Order.submit @@ [mk_new_limit_order ~symbol ~side:buy_sell ~price:vwap ~qty] >>| function *)
        (*   | Ok _ -> info "on_quote_change: update %s %s %d" symbol (Side.show side) vwap *)
        (*   | Error err -> error "%s" @@ Error.to_string_hum err *)
        (* end *)
      | Some best_price ->
        let oid = String.Table.find (current_orders side) symbol in
        let order = Option.bind oid (Uuid.Table.find orders) in
        let order = Option.bind order (fun order ->
            RespObj.(Option.bind
                       (bool order "workingIndicator")
                       (function true -> Some order | false -> None)
                    )
          )
        in
        Order.update @@ List.filter_opt [
          Option.map order ~f:(fun o ->
              let oid = RespObj.string_exn o "orderID" in
              let old_price = satoshis_int_of_float_exn @@ RespObj.float_exn o "price" in
              let new_price = old_price + dvwap in
              mk_amended_limit_order ~price:new_price oid);
        ] >>| function
        | Ok _ -> info "on_quote_change: update %s %s %d" symbol (Side.show side) vwap
        | Error err -> error "%s" @@ Error.to_string_hum err
    end
  | _ -> Deferred.unit

let instruments_initialized = Ivar.create ()
let orders_initialized = Ivar.create ()
let positions_initialized = Ivar.create ()
let bid_set = Ivar.create ()
let ask_set = Ivar.create ()
let all_initialized = Deferred.List.iter ~f:Ivar.read [instruments_initialized; orders_initialized; bid_set; ask_set]

let on_instrument action data =
  let on_partial_insert i =
    let i = RespObj.of_json i in
    let sym = RespObj.string_exn i "symbol" in
    String.Table.set instruments sym i;
    String.Table.set ticksizes ~key:sym ~data:(RespObj.float_exn i "tickSize" |> satoshis_int_of_float_exn);
    String.Table.set OB.orders sym (Int.Table.create ());
    String.Table.set OB.bids sym (Int.Map.empty);
    String.Table.set OB.asks sym (Int.Map.empty)
  in
  let on_update i =
    let i = RespObj.of_json i in
    let sym = RespObj.string_exn i "symbol" in
    let oldInstr = String.Table.find_exn instruments sym in
    String.Table.set instruments ~key:sym ~data:(RespObj.merge oldInstr i)
  in
  match action with
  | Partial | Insert ->
    List.iter data ~f:on_partial_insert;
    Ivar.fill_if_empty instruments_initialized ()
  | Update -> List.iter data ~f:on_update
  | _ -> error "instrument: got action %s" (show_update_action action)

let on_order action data =
  let on_partial o =
    let o = RespObj.of_json o in
    let oid_str = RespObj.string_exn o "orderID" in
    let clOrdID_str = RespObj.string_exn o "clOrdID" in
    debug "<- partial order %s (%s)" oid_str clOrdID_str;
    Uuid.Table.set orders Uuid.(of_string oid_str) o;
    Deferred.unit
  in
  let on_insert_update action o =
    let o = RespObj.of_json o in
    let sym = RespObj.string_exn o "symbol" in
    let oid_str = RespObj.string_exn o "orderID" in
    let oid = Uuid.of_string oid_str in
    let old_o = Uuid.Table.find orders oid |> Option.value ~default:o in
    let o = RespObj.merge old_o o in
    Uuid.Table.set orders ~key:oid ~data:o;
    let clOrdID_str = RespObj.string_exn o "clOrdID" in
    debug "<- %s order %s (%s)" (show_update_action action) oid_str clOrdID_str;
    let side_str = RespObj.string_exn o "side" in
    let side = buy_sell_of_bmex side_str in
    let workingIndicator = RespObj.bool_exn o "workingIndicator" in
    let current_table, current_ivar = match side with
      | `Buy -> current_bids, bid_set
      | `Sell -> current_asks, ask_set
    in
    if clOrdID_str <> "" then begin
      if not workingIndicator then
        String.Table.remove current_table sym
      else begin
        debug "set %s as current %s" oid_str side_str;
        String.Table.set current_table sym oid;
        Ivar.fill_if_empty current_ivar ()
      end
    end
  in
  let on_delete o =
    let o = RespObj.of_json o in
    let oid_str = RespObj.string_exn o "orderID" in
    let clOrdID_str = RespObj.string_exn o "clOrdID" in
    debug "<- delete order %s (%s)" oid_str clOrdID_str;
    Uuid.Table.remove orders Uuid.(of_string oid_str)
  in
  match action with
  | Partial ->
    don't_wait_for begin
      Ivar.read instruments_initialized >>= fun () ->
      Deferred.List.iter data ~f:on_partial >>| fun () ->
      Ivar.fill_if_empty orders_initialized ()
    end
  | Insert | Update -> if Ivar.is_full orders_initialized then List.iter data ~f:(on_insert_update action)
  | Delete -> if Ivar.is_full orders_initialized then List.iter data ~f:on_delete

let on_execution action data =
  let on_exec e =
    let e = RespObj.of_json e in
    let eid = RespObj.string_exn e "execID" |> Uuid.of_string  in
    Uuid.Table.set executions eid e
  in
  List.iter data ~f:on_exec

let on_margin action data =
  let on_margin m =
    let m = RespObj.of_json m in
    margin := m
  in
  List.iter data ~f:on_margin

let on_position action data =
  let on_partial_insert p =
    let p = RespObj.of_json p in
    let sym = RespObj.string_exn p "symbol" in
    if String.Table.mem quoted_instruments sym then
      let currentQty = RespObj.int64_exn p "currentQty" in
      debug "<- partial/insert position %s %Ld" sym currentQty;
      String.Table.set positions sym p;
      if action = Partial then Ivar.fill_if_empty positions_initialized ();
      on_position_update sym p
  in
  let on_update p =
    let p = RespObj.of_json p in
    let sym = RespObj.string_exn p "symbol" in
    if String.Table.mem quoted_instruments sym then
      let old_p = String.Table.find_exn positions sym in
      let p = RespObj.merge old_p p in
      String.Table.set positions sym p;
      let currentQty = RespObj.int64_exn p "currentQty" in
      let oldQty = RespObj.int64_exn old_p "currentQty" in
      debug "<- update position %s %Ld" sym currentQty;
      if currentQty <> oldQty then on_position_update sym p
  in
  let on_delete p =
    let p = RespObj.of_json p in
    let sym = RespObj.string_exn p "symbol" in
    if String.Table.mem quoted_instruments sym then begin
      debug "<- delete position %s" sym;
      String.Table.remove positions sym
    end
  in
  match action with
  | Partial | Insert -> List.iter data ~f:on_partial_insert
  | Update -> List.iter data ~f:on_update
  | Delete -> List.iter data ~f:on_delete

let on_orderbook action data =
  let action_str = show_update_action action in
  debug "<- orderbook %s" action_str;
  let of_json json =
    match OrderBook.L2.of_yojson json with
    | `Ok u -> u
    | `Error reason ->
      failwithf "%s: %s (%s)" reason Yojson.Safe.(to_string json) action_str ()
  in
  let book_add_qty book price qty =
    Int.Map.update book price ~f:(function None -> qty | Some oldq -> oldq + qty)
  in
  let book_modify_qty_exn book price qty =
    Int.Map.update book price ~f:(function None -> invalid_arg "book_modify_qty" | Some oldq -> oldq + qty)
  in
  let book_modify_qty book price qty =
    try book_modify_qty_exn book price qty with _ ->
      error "update_depth: inconsistent orderbooks!";
      book
  in
  let update_depth action old_book { OrderBook.L2.symbol; id; side = side_str; price = new_price; size = new_qty } =
    let new_price = Option.map new_price ~f:satoshis_int_of_float_exn in
    let orders = String.Table.find_exn OB.orders symbol in
    let old_order = Int.Table.find orders id in
    let old_price = Option.map old_order ~f:(fun { price } -> price) in
    let old_qty = Option.map old_order ~f:(fun { qty } -> qty) in
    match action, old_price, old_qty, new_price, new_qty with
    | Delete, Some oldp, Some oldq, _, _ ->
      Int.Table.remove orders id;
      book_modify_qty old_book oldp (Int.neg oldq)
    | Partial, None, None, Some newp, Some newq
    | Insert, None, None, Some newp, Some newq ->
      Int.Table.set orders id { price = newp; qty = newq };
      book_add_qty old_book newp newq
    | Update, Some oldp, Some oldq, Some newp, Some newq ->
      Int.Table.set orders id { price = newp; qty = newq };
      if oldp = newp then
        book_modify_qty old_book oldp (newq - oldq)
      else
        let new_book = book_modify_qty old_book oldp (Int.neg oldq) in
        book_modify_qty new_book newp newq
    | _ ->
      error "update_depth: %s %d %d %d %d"
        action_str
        (Option.value ~default:Int.max_value old_price)
        (Option.value ~default:Int.max_value old_qty)
        (Option.value ~default:Int.max_value new_price)
        (Option.value ~default:Int.max_value new_qty)
      ;
      old_book
  in
  let data = List.map data ~f:of_json in
  let data = List.group data ~break:(fun u u' -> u.OrderBook.L2.symbol <> u'.symbol || u.side <> u'.side) in
  let iter_f = function
    | (h :: t) as us when String.Table.mem quoted_instruments h.OrderBook.L2.symbol ->
      let max_pos_size = String.Table.find_exn quoted_instruments h.symbol in
      let side = buy_sell_of_bmex h.OrderBook.L2.side in
      debug "iter_depths %s %s" h.symbol h.side;
      let books = match side with `Buy -> OB.bids | `Sell -> OB.asks in
      let old_book = String.Table.find_exn books h.symbol in
      let new_book = List.fold_left us ~init:old_book ~f:(update_depth action) in
      String.Table.set books h.symbol new_book;
      let side = match side with `Buy -> Side.Bid | `Sell -> Ask in
      let vwap, total_v = vwap ~vlimit:max_pos_size side new_book in
      let side_str = Side.show side in
      info "BMEX %s %s vwap=%.2f totalv=%d" h.symbol side_str (Float.of_int vwap /. Float.of_int total_v *. 1e-8) total_v
    | _ -> ()
  in
  don't_wait_for begin
    Ivar.read instruments_initialized >>| fun () ->
    List.iter data ~f:iter_f
  end

let market_make buf =
  let on_ws_msg msg_str =
    let msg_json = Yojson.Safe.from_string ~buf msg_str in
    match Ws.update_of_yojson msg_json with
    | `Error _ -> begin
        match Ws.response_of_yojson msg_json with
        | `Error _ ->
          error "%s" msg_str
        | `Ok response ->
          info "%s" @@ Ws.show_response response
      end
    | `Ok { table; action; data } ->
      let action = update_action_of_string action in
      match table with
      | "instrument" -> on_instrument action data
      | "orderBookL2" -> on_orderbook action data
      | "order" -> on_order action data
      | "position" -> on_position action data
      | "execution" -> on_execution action data
      | "margin" -> on_margin action data
      | _ -> error "Invalid table %s" table
  in
  (* Cancel all orders *)
  Order.cancel_all () >>= function
  | Error err ->
    let err_str = Error.to_string_hum err in
    error "%s" err_str;
    failwith err_str
  | Ok msg ->
    info "all orders canceled: %s" msg;
    Ws.with_connection
      ~testnet:!testnet
      ~auth:(!api_key, !api_secret)
      ~topics:["instrument"; "orderBookL2"; "order"; "execution"; "margin"; "position"]
      ~on_ws_msg ()

let rpc_client port =
  let open Rpc in
  let open Protocols in
  let on_response msg =
    debug "rpc_client: on_response";
    let open Pipe_rpc in match msg with
    | Pipe_message.Closed (`Error err) ->
      error "rpc_client: closed with error %s" @@ Error.to_string_hum err;
      Pipe_response.Continue
    | Closed `By_remote_side ->
      error "rpc_client: closed by remote side";
      Continue
    | Update (OrderBook.Subscribed (sym, max_pos)) ->
      info "hedger subscribed to %s %d" sym max_pos;
      Continue
    | Update (Ticker t) -> Wait (on_ticker_update t);
  in
  let client_f c =
    let quoted_instrs = String.Table.to_alist quoted_instruments in
    don't_wait_for @@
    Pipe.iter position_r ~f:(fun position_update ->
        Deferred.ignore @@ Rpc.dispatch Position.t c position_update
      );
    Pipe_rpc.dispatch_iter OrderBook.t c
      (OrderBook.Subscribe quoted_instrs) ~f:on_response
  in
  let rec loop () =
    debug "rpc starting";
    Connection.with_client ~host:"localhost" ~port client_f >>= function
    | Ok _ ->
      error "rpc terminated";
      loop ()
    | Error exn ->
      error "rpc_client crashed: %s" @@ Exn.(to_string exn);
      after @@ Time.Span.of_int_sec 5 >>= loop
  in loop ()

let main cfg port daemon pidfile logfile loglevel mainnet dry_run' instruments () =
  dry_run := dry_run';
  testnet := not mainnet;
  let cfg = Yojson.Safe.from_file cfg |> Cfg.of_yojson |> presult_exn in
  let { Cfg.key; secret; quote } = List.Assoc.find_exn cfg (if mainnet then "BMEX" else "BMEXT") in
  api_key := key;
  api_secret := Cstruct.of_string secret;
  let instruments = if instruments = [] then quote else instruments in
  let buf = Bi_outbuf.create 4096 in
  if daemon then Daemon.daemonize ~cd:"." ();
  set_output Log.Output.[stderr (); file `Text ~filename:logfile];
  set_level (match loglevel with 2 -> `Info | 3 -> `Debug | _ -> `Error);
  Out_channel.write_all pidfile ~data:(Unix.getpid () |> Pid.to_string);
  hedger_port := port;
  List.iter instruments ~f:(fun (k, v) -> String.Table.set quoted_instruments k v);
  don't_wait_for @@ dead_man's_switch 60000 15.;
  don't_wait_for @@ Deferred.ignore @@ rpc_client port;
  don't_wait_for @@ market_make buf;
  never_returns @@ Scheduler.go ()

let command =
  let default_cfg = Filename.concat (Option.value_exn (Sys.getenv "HOME")) ".virtu" in
  let spec =
    let open Command.Spec in
    empty
    +> flag "-cfg" (optional_with_default default_cfg string) ~doc:"path Filepath of config file (default: ~/.virtu)"
    +> flag "-port" (optional_with_default 53232 int) ~doc:"int hedger port to connect to"
    +> flag "-daemon" no_arg ~doc:" Run as a daemon"
    +> flag "-pidfile" (optional_with_default "run/virtu.pid" string) ~doc:"filename Path of the pid file (run/virtu.pid)"
    +> flag "-logfile" (optional_with_default "log/virtu.log" string) ~doc:"filename Path of the log file (log/virtu.log)"
    +> flag "-loglevel" (optional_with_default 1 int) ~doc:"1-3 loglevel"
    +> flag "-mainnet" no_arg ~doc:" Use mainnet"
    +> flag "-dry" no_arg ~doc:" Simulation mode"
    +> anon (sequence (t2 ("instrument" %: string) ("quote" %: int)))
  in
  Command.basic ~summary:"Market maker bot" spec main

let () = Command.run command
