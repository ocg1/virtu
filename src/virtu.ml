(* Virtu: market making bot for BitMEX *)

open Core.Std
open Async.Std
open Log.Global

open Util
open Bs_devkit.Core
open Bs_api.BMEX

let log_ws = Log.(create ~level:`Error ~on_error:`Raise ~output:[Output.stderr ()])

let api_key = ref ""
let api_secret = ref @@ Cstruct.of_string ""
let testnet = ref true

let order_cfg = ref @@ Order.create_cfg ()

let hedger_port = ref 0

let quoted_instruments : instrument_info String.Table.t = String.Table.create ()

let instruments : RespObj.t String.Table.t = String.Table.create ()
let orders : RespObj.t Uuid.Table.t = Uuid.Table.create ()
let positions : RespObj.t String.Table.t = String.Table.create ()

let instruments_initialized = Ivar.create ()

let feed_initialized =
  Ivar.read instruments_initialized >>= fun () ->
  String.Table.fold quoted_instruments ~init:[]
    ~f:(fun ~key ~data:{ bids_initialized; asks_initialized } a ->
        bids_initialized :: asks_initialized :: a
      ) |>
  Deferred.List.iter ~f:Ivar.read

let ticksizes : ticksize String.Table.t = String.Table.create ()

let current_bids : RespObj.t String.Table.t = String.Table.create ()
let current_asks : RespObj.t String.Table.t = String.Table.create ()

module OB = struct
  type order = { price: int; qty: int }
  let orders : (order Int.Table.t) String.Table.t = String.Table.create ()

  let bids : (Int.t Int.Map.t) String.Table.t = String.Table.create ()
  let asks : (Int.t Int.Map.t) String.Table.t = String.Table.create ()
  let book_of_side = function Side.Bid -> bids | Ask -> asks

  let best_price ?remove_order side symbol =
    let open Option.Monad_infix in
    String.Table.find (book_of_side side) symbol >>= fun book ->
    let book = Option.value_map remove_order ~default:book
        ~f:(fun (price, qty) -> book_modify_qty book price @@ Int.neg qty)
    in
    best_of_side side book

  type vwap = {
    max_pos_p: int;
    total_v: int;
  } [@@deriving create]

  let bid_vwaps : vwap String.Table.t = String.Table.create ()
  let ask_vwaps : vwap String.Table.t = String.Table.create ()
  let vwaps_of_side = function Side.Bid -> bid_vwaps | Ask -> ask_vwaps

  let vwap side symbol =
    let vwaps = vwaps_of_side side in
    String.Table.find vwaps symbol

  let mid_price ?remove_bid ?remove_ask symbol = function
    | Best ->
      let best_bid = best_price ?remove_order:remove_bid Side.Bid symbol in
      let best_ask = best_price ?remove_order:remove_ask Ask symbol in
      Option.map2 best_bid best_ask (fun (bb,_) (ba,_) ->
          let midPrice =  (bb + ba) / 2 in
          midPrice, ba - midPrice
        )
    | Vwap ->
      let vwap_bid = vwap Side.Bid symbol in
      let vwap_ask = vwap Ask symbol in
      Option.map2 vwap_bid vwap_ask
        (fun { max_pos_p=mp; total_v=tv } { max_pos_p=mp'; total_v=tv' } ->
           let vb = mp / tv in
           let va = mp' / tv' in
           let midPrice = (vb + va) / 2 in
           midPrice, va - midPrice
        )

  let xbt_value sym = match sym with
  | "XBTUSD" ->
    Option.map (mid_price sym Vwap)
      ~f:(fun (midPrice, _) -> 10_000_000_000_000_000 / midPrice)
  | "ETHXBT" -> Option.map ~f:fst @@ mid_price sym Vwap
  | "LTCXBT" -> Option.map ~f:fst @@ mid_price sym Vwap
  | "LSKXBT" -> Option.map ~f:fst @@ mid_price sym Vwap
  | "FCTXBT" -> Option.map ~f:fst @@ mid_price sym Vwap
  | _ -> invalid_arg "xbt_value"
end

module S = Strategy.Blanket(struct
    let log = Lazy.(force log)
    let order_cfg = lazy(!order_cfg)
    let current_bids = current_bids
    let current_asks = current_asks
    let quoted_instruments = quoted_instruments
    let ticksizes = ticksizes
    let local_best_price = OB.best_price
  end)

let dead_man's_switch timeout period =
  let rec loop () =
    Order.cancel_all_after !order_cfg timeout >>= function
    | Error err ->
      error "%s" @@ Error.to_string_hum err;
      loop ()
    | Ok _ -> Clock_ns.after @@ Time_ns.Span.of_int_sec period >>= loop
  in loop ()

let compute_orders ?order symbol side price newQty =
  let leavesQty = Option.map order ~f:(fun o -> RespObj.int64_exn o "leavesQty" |> Int64.to_int_exn) in
  let ticksize = String.Table.find_exn ticksizes symbol in
  let leavesQtySexp = Option.sexp_of_t Int.sexp_of_t leavesQty |> Sexp.to_string_hum in
  debug "compute_orders %s %s %d %s %d" symbol (Side.show side) (price / ticksize.divisor) leavesQtySexp newQty;
  match leavesQty with
  | None when newQty > 0 ->
    let clOrdID = Uuid.(create () |> to_string) in
    Some (clOrdID, mk_new_limit_order ~symbol ~side ~ticksize ~price ~qty:newQty clOrdID), None
  | Some leavesQty when leavesQty > 0 || leavesQty = 0 && newQty > 0 ->
    let orig, oid = Order.oid_of_respobj (Option.value_exn order) in
    None, Some (oid, mk_amended_limit_order ~symbol ~ticksize ~price ~qty:newQty orig oid)
  | _ -> None, None

let string_of_order = function
| None -> "()"
| Some o -> match RespObj.(string o "orderID", string o "clOrdID") with
| Some oid, Some clOrdID -> Printf.sprintf "(a %s)" clOrdID
| None, Some clOrdID -> Printf.sprintf "(s %s)" clOrdID
| _ -> invalid_arg "string_of_order"

let on_position_update action symbol oldp p =
  let is_in_flight o = Option.is_none @@ RespObj.string o "orderID" in
  let { max_pos_size } = String.Table.find_exn quoted_instruments symbol in
  let oldQty = Option.value_map oldp ~default:0 ~f:(fun oldp -> RespObj.int64_exn oldp "currentQty" |> Int64.to_int_exn) in
  let currentQty = RespObj.int64_exn p "currentQty" |> Int64.to_int_exn in
  debug "[P] %s %s %d -> %d" (show_update_action action) symbol oldQty currentQty;
  let currentBid = String.Table.find current_bids symbol in
  let currentAsk = String.Table.find current_asks symbol in
  begin match currentBid, currentAsk with
  | Some o, _
  | _, Some o when is_in_flight o -> failwithf "%s in flight" (RespObj.string_exn o "clOrdID") ()
  | _ -> ()
  end;
  (* let fw_p_to_hedger c = Rpc.Rpc.dispatch Protocols.Position.t c (symbol, currentQty) in *)
  (* don't_wait_for @@ Deferred.ignore @@ Rpc.Connection.with_client ~host:"localhost" ~port:!hedger_port fw_p_to_hedger; *)
  let currentBidQty = Option.value_map (RespObj.int64 p "openOrderBuyQty") ~default:0 ~f:Int64.to_int_exn in
  let currentAskQty = Option.value_map (RespObj.int64 p "openOrderSellQty") ~default:0 ~f:Int64.to_int_exn in
  let newBidQty = Int.max (max_pos_size - currentQty) 0 in
  let newAskQty = Int.max (max_pos_size + currentQty) 0 in
  if currentBidQty = newBidQty && currentAskQty = newAskQty then failwithf "position unchanged" ();
  info "orders needed: bid %d -> %d, ask %d -> %d" currentBidQty newBidQty currentAskQty newAskQty;
  info "current orders: %s %s" (string_of_order currentBid) (string_of_order currentAsk);
  let currentBid = Option.bind currentBid (fun o -> if is_in_flight o then None else Some o) in
  let currentAsk = Option.bind currentAsk (fun o -> if is_in_flight o then None else Some o) in
  let bidPrice = match Option.(currentBid >>= price_qty_of_order), OB.best_price Bid symbol with
  | Some (price, _qty), _ -> price
  | None, Some (price, _qty) -> price
  | _ -> failwith "No suitable bid price found"
  in
  let askPrice = match Option.(currentAsk >>= price_qty_of_order), OB.best_price Ask symbol with
  | Some (price, _qty), _ -> price
  | None, Some (price, _qty) -> price
  | _ -> failwith "No suitable ask price found"
  in
  let bidSubmit, bidAmend = compute_orders ?order:currentBid symbol Bid bidPrice newBidQty in
  let askSubmit, askAmend = compute_orders ?order:currentAsk symbol Ask askPrice newAskQty in
  let submit_th = match
    List.fold_left [bidSubmit; askSubmit] ~init:[]
      ~f:(fun a -> function None -> a | Some (_, o) -> o :: a)
  with
  | [] -> Deferred.unit
  | orders -> Order.submit !order_cfg orders >>| function
    | Ok _ -> ()
    | Error err -> error "on_position_update: %s: %s" (Yojson.Safe.to_string (`List orders)) (Error.to_string_hum err)
  in
  let amend_th = match
    List.fold_left [bidAmend; askAmend] ~init:[]
      ~f:(fun a -> function None -> a | Some (_, o) -> o :: a)
  with
  | [] -> Deferred.unit
  | orders -> Order.update !order_cfg orders >>| function
    | Ok _ -> ()
    | Error err -> error "on_position_update: %s: %s" (Yojson.Safe.to_string (`List orders)) (Error.to_string_hum err)
  in
  Deferred.all_unit [submit_th; amend_th]

let on_instrument action data =
  let on_partial_insert i =
    let i = RespObj.of_json i in
    let sym = RespObj.string_exn i "symbol" in
    String.Table.set instruments sym i;
    String.Table.set ticksizes ~key:sym
      ~data:(RespObj.float_exn i "tickSize" |> fun multiplier ->
             let mult_exponent, divisor = exponent_divisor_of_tickSize multiplier in
             create_ticksize ~multiplier ~mult_exponent ~divisor ());
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
  begin match action with
  | Partial | Insert ->
    List.iter data ~f:on_partial_insert;
    Ivar.fill_if_empty instruments_initialized ()
  | Update -> List.iter data ~f:on_update
  | _ -> error "instrument: got action %s" (show_update_action action)
  end

let on_order action data =
  let on_partial o =
    let o = RespObj.of_json o in
    let sym = RespObj.string_exn o "symbol" in
    let side_str = RespObj.string_exn o "side" in
    let side = buy_sell_of_bmex side_str in
    let current_table = match side with `Buy -> current_bids | `Sell -> current_asks in
    let ordOrig, oid_str = Order.oid_of_respobj o in
    debug "[O] %s %s %s %s" (show_update_action action) sym side_str (String.sub oid_str 0 8);
    Uuid.(Table.set orders (of_string oid_str) o);
    String.Table.set current_table sym o
  in
  let on_insert_update action o_json =
    let o = RespObj.of_json o_json in
    let sym = RespObj.string_exn o "symbol" in
    let ordOrig, oid_str = Order.oid_of_respobj o in
    let oid = Uuid.of_string oid_str in
    let o = match Uuid.Table.find orders oid with None -> o | Some old_o -> RespObj.merge old_o o in
    Uuid.Table.set orders oid o;
    let side_str = RespObj.string_exn o "side" in
    debug "[O] %s %s %s %s" (show_update_action action) sym side_str (String.sub oid_str 0 8);
    let side = buy_sell_of_bmex side_str in
    let current_table = match side with `Buy -> current_bids | `Sell -> current_asks in
    if ordOrig = `C then begin
      debug "[O] %s %s := %s" sym side_str @@ String.sub oid_str 0 8;
      String.Table.set current_table sym o;
    end
  in
  let on_delete o =
    let o = RespObj.of_json o in
    let sym = RespObj.string_exn o "symbol" in
    let ordOrig, oid_str = Order.oid_of_respobj o in
    let oid = Uuid.of_string oid_str in
    match Uuid.Table.find orders oid with
    | None -> ()
    | Some o ->
      let side_str = RespObj.string_exn o "side" in
      let side = buy_sell_of_bmex side_str in
      let current_table = match side with `Buy -> current_bids | `Sell -> current_asks in
      debug "[O] %s %s %s" (show_update_action action) side_str (String.sub oid_str 0 8);
      Uuid.Table.remove orders oid;
      String.Table.remove current_table sym;
  in
  begin match action with
  | Partial -> List.iter data ~f:on_partial
  | Insert | Update -> List.iter data ~f:(on_insert_update action)
  | Delete -> List.iter data ~f:on_delete
  end

let on_position action data =
  let on_not_delete action p =
    let p = RespObj.of_json p in
    let sym = RespObj.string_exn p "symbol" in
    if not @@ String.Table.mem quoted_instruments sym then Deferred.unit
    else
    let oldp = String.Table.find positions sym in
    let p = Option.value_map oldp
        ~default:p ~f:(fun oldp -> RespObj.merge oldp p)
    in
    String.Table.set positions sym p;
    Monitor.try_with ~name:"on_position_update"
      (fun () -> on_position_update action sym oldp p) >>| function
    | Ok () -> ()
    | Error exn -> match Monitor.extract_exn exn with
    | Failure reason -> info "[P] %s" reason
    | _ -> error "%s" @@ Exn.(to_string exn)
  in
  let on_delete p =
    let p = RespObj.of_json p in
    let sym = RespObj.string_exn p "symbol" in
    if String.Table.mem quoted_instruments sym then begin
      debug "[P] %s %s" (show_update_action action) sym;
      String.Table.remove positions sym
    end
  in
  match action with
  | Delete -> List.iter data ~f:on_delete; Deferred.unit
  | _ ->
    if Deferred.is_determined feed_initialized then
      Deferred.List.iter data ~how:`Sequential ~f:(on_not_delete action)
    else begin
      don't_wait_for begin
        feed_initialized >>= fun () ->
        Deferred.List.iter data ~how:`Sequential ~f:(on_not_delete action)
      end;
      Deferred.unit
    end

let on_orderbook action data =
  (* debug "<- %s" (Yojson.Safe.to_string (`List data)); *)
  let action_str = show_update_action action in
  let of_json json =
    match OrderBook.L2.of_yojson json with
    | `Ok u -> u
    | `Error reason -> failwithf "%s: %s (%s)" reason Yojson.Safe.(to_string json) action_str ()
  in
  let update_depth action old_book { OrderBook.L2.symbol; id; side = side_str; price = new_price; size = new_qty } =
    let orders = String.Table.find_exn OB.orders symbol in
    let old_order = Int.Table.find orders id in
    let old_price = Option.map old_order ~f:(fun { price } -> price) in
    let old_qty = Option.map old_order ~f:(fun { qty } -> qty) in
    let new_price = match new_price with
    | Some new_price -> Option.some @@ satoshis_int_of_float_exn new_price
    | None -> old_price
    in
    let new_qty = match new_qty with
    | Some qty -> Some qty
    | None -> old_qty
    in
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
        let intermediate_book = book_modify_qty old_book oldp (Int.neg oldq) in
        book_modify_qty ~allow_empty:true intermediate_book newp newq
    | _ ->
      failwithf "update_depth: %s %d %d %d %d"
        action_str
        (Option.value ~default:Int.max_value old_price)
        (Option.value ~default:Int.max_value old_qty)
        (Option.value ~default:Int.max_value new_price)
        (Option.value ~default:Int.max_value new_qty) ()
  in
  let data = List.map data ~f:of_json in
  let data = List.group data ~break:(fun u u' -> u.OrderBook.L2.symbol <> u'.symbol || u.side <> u'.side) in
  let iter_f = function
  | (h :: t) as us when String.Table.mem quoted_instruments h.OrderBook.L2.symbol ->
    let side, best_elt_f, books, vwaps = match buy_sell_of_bmex h.OrderBook.L2.side with
    | `Buy -> Side.Bid, Int.Map.max_elt, OB.bids, OB.bid_vwaps
    | `Sell -> Ask, Int.Map.min_elt, OB.asks, OB.ask_vwaps
    in
    let { max_pos_size } = String.Table.find_exn quoted_instruments h.symbol in
    let old_book = if action = Partial then Int.Map.empty else
      String.Table.find_exn books h.symbol
    in
    let new_book = List.fold_left us ~init:old_book ~f:(update_depth action) in
    String.Table.set books h.symbol new_book;

    (* compute vwap *)
    let max_pos_p, total_v = vwap ~vlimit:max_pos_size side new_book in
    String.Table.set vwaps ~key:h.symbol ~data:(OB.create_vwap ~max_pos_p ~total_v ());
    if total_v > 0 then
      let { divisor } = String.Table.find_exn ticksizes h.symbol in
      info "[OB] %s %s %s %d %d"
        (show_update_action action) h.symbol (Side.show side)
        (Option.value_map (best_elt_f new_book) ~default:0 ~f:(fun (v, _) -> v / divisor))
        (max_pos_p / total_v / divisor);

    if action = Partial then begin
      debug "[OB] %s %s initialized" h.symbol (Side.show side);
      let { bids_initialized; asks_initialized } = String.Table.find_exn quoted_instruments h.symbol in
      match side with
      | Bid -> Ivar.fill_if_empty bids_initialized ()
      | Ask -> Ivar.fill_if_empty asks_initialized ()
    end

  | _ -> ()
  in
  List.iter data ~f:iter_f

let market_make buf strategy instruments =
  let on_ws_msg msg_str =
    let msg_json = Yojson.Safe.from_string ~buf msg_str in
    match Ws.update_of_yojson msg_json with
    | `Error _ -> begin
        match Ws.response_of_yojson msg_json with
        | `Error _ ->
          error "%s" msg_str
        | `Ok response ->
          info "%s" @@ Ws.show_response response
      end; Deferred.unit
    | `Ok { table; action; data } ->
      let action = update_action_of_string action in
      match table with
      | "instrument" -> on_instrument action data; Deferred.unit
      | "orderBookL2" ->
        if Ivar.is_full instruments_initialized then on_orderbook action data
        else don't_wait_for begin
            Ivar.read instruments_initialized >>| fun () ->
            on_orderbook action data
          end;
        Deferred.unit
      | "order" ->
        if Ivar.is_full instruments_initialized then on_order action data
        else don't_wait_for begin
            Ivar.read instruments_initialized >>| fun () ->
            on_order action data
          end;
        Deferred.unit
      | "position" -> on_position action data
      | _ -> error "Invalid table %s" table; Deferred.unit
  in
  (* Cancel all orders *)
  Order.cancel_all !order_cfg >>= function
  | Error err ->
    let err_str = Error.to_string_hum err in
    error "%s" err_str;
    failwith err_str
  | Ok _ ->
    info "all orders canceled";
    let topics = "order" :: "position" :: List.(map instruments ~f:(fun i -> ["instrument:" ^ i; "orderBookL2:" ^ i]) |> concat) in
    don't_wait_for @@ dead_man's_switch 60000 15;
    don't_wait_for (Ivar.read instruments_initialized >>= fun () -> S.update_orders strategy);
    let ws = Ws.open_connection ~log:log_ws ~testnet:!testnet ~auth:(!api_key, !api_secret) ~topics () in
    Monitor.handle_errors
      (fun () -> Pipe.iter ~continue_on_error:true ws ~f:on_ws_msg)
      (fun exn -> error "%s" @@ Exn.to_string exn)

let rpc_client port =
  let open Rpc in
  let open Protocols in
  let on_msg = function
  | OrderBook.Subscribed (sym, max_pos) ->
    info "hedger subscribed to %s %d" sym max_pos;
  | Ticker t -> ()
  in
  let client_f c =
    let quoted_instrs = String.Table.to_alist quoted_instruments in
    Deferred.List.iter quoted_instrs
      ~f:(fun (sym, { bids_initialized; asks_initialized }) ->
          Deferred.all_unit @@ Ivar.[read bids_initialized;
                                     read asks_initialized]
        )
    >>= fun () ->
    let quoted_instrs =
    List.filter_map quoted_instrs ~f:(fun (sym, { max_pos_size }) ->
        Option.map (OB.xbt_value sym)
          ~f:(fun xbt_v -> sym, max_pos_size * xbt_v)
        )
    in
    Pipe_rpc.dispatch OrderBook.t c
      (OrderBook.Subscribe quoted_instrs) >>= function
    | Ok (Ok (feed, meta)) ->
      List.iter quoted_instrs ~f:(fun (sym, xbt_v) ->
          debug "-> [RPC] subscribe %s %d" sym xbt_v
        );
      Pipe.iter_without_pushback feed ~f:on_msg
    | Ok (Error err) ->
      error "[RPC] dispatch returned error: %s" (Error.to_string_hum err);
      Deferred.unit
    | Error err ->
      error "[RPC] dispatch raised: %s" Error.(to_string_hum err);
      Deferred.unit
  in
  let rec loop () =
    debug "rpc starting";
    Connection.with_client ~host:"localhost" ~port client_f >>= function
    | Ok () ->
        error "rpc returned, restarting";
        Clock_ns.after @@ Time_ns.Span.of_int_sec 5 >>= loop
    | Error exn ->
        error "rpc_client crashed: %s" @@ Exn.(to_string exn);
        Clock_ns.after @@ Time_ns.Span.of_int_sec 5 >>= loop
  in loop ()

let tickers_of_instrument = function
| "XBTUSD" ->
  Pipe.map (Ws.Kaiko.tickers ()) ~f:(fun { index={ bid; ask } } ->
      debug "[T] Kaiko %s %s" bid ask;
      satoshis_int_of_float_exn @@ Float.of_string bid,
      satoshis_int_of_float_exn @@ Float.of_string ask
    )
| "ETHXBT" ->
  let open Bs_api.PLNX in
  Pipe.filter_map (Ws.open_connection ~topics:["ticker"] ()) ~f:(fun msg ->
      let event = Wamp.event_of_msg msg in
      let { Ws.symbol; bid; ask } = Ws.ticker_of_json @@ `List event.args in
      if symbol <> to_remote_sym "ETHXBT" then None
      else begin
        debug "[T] PLNX %s %f %f" symbol bid ask;
        Some (satoshis_int_of_float_exn bid, satoshis_int_of_float_exn ask)
      end
    )
| _ -> invalid_arg "tickers_of_instrument"

let main cfg port daemon pidfile logfile loglevel wsllevel main dry fixed remfixed instruments () =
  don't_wait_for begin
    Lock_file.create_exn pidfile >>= fun () ->
    let cfg = Yojson.Safe.from_file cfg |> Cfg.of_yojson |> presult_exn in
    let { Cfg.key; secret; quote } = List.Assoc.find_exn cfg (if main then "BMEX" else "BMEXT") in
    let secret_cstruct = Cstruct.of_string secret in
    testnet := not main;
    api_key := key;
    api_secret := secret_cstruct;
    order_cfg := Order.create_cfg ~dry_run:dry ~orders_t:orders ~current_bids ~current_asks ~testnet:(not main) ~key ~secret:secret_cstruct ();
    let instruments = if instruments = [] then quote else instruments in
    let buf = Bi_outbuf.create 4096 in
    if daemon then Daemon.daemonize ~cd:"." ();
    let log_outputs = Log.Output.[stderr (); file `Text ~filename:logfile] in
    set_output log_outputs;
    Log.set_output log_ws log_outputs;
    set_level @@ loglevel_of_int loglevel;
    Log.set_level log_ws @@ loglevel_of_int wsllevel;
    hedger_port := port;
    List.iter instruments ~f:(fun (symbol, max_pos_size) ->
        let data = create_instrument_info
            ~max_pos_size
            ~ticker:(tickers_of_instrument symbol)
            ()
        in
        String.Table.set quoted_instruments ~key:symbol ~data
      );
    info "Virtu starting";
    let strategy = match fixed, remfixed with
    | None, None -> `Fixed 1
    | Some fixed, _ -> `Fixed fixed
    | _, Some remfixed -> `FixedRemote remfixed
    in
    Deferred.(all_unit [
        (* ignore @@ rpc_client port; *)
        market_make buf strategy @@ String.Table.keys quoted_instruments;
      ]);
  end;
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
    +> flag "-wsllevel" (optional_with_default 1 int) ~doc:"1-3 loglevel"
    +> flag "-main" no_arg ~doc:" Use mainnet"
    +> flag "-dry" no_arg ~doc:" Simulation mode"
    +> flag "-fixed" (optional int) ~doc:"tick Post bid/ask with a fixed spread"
    +> flag "-fixed-remote" (optional int) ~doc:"tick Post bid/ask with a fixed spread"
    +> anon (sequence (t2 ("instrument" %: string) ("quote" %: int)))
  in
  Command.basic ~summary:"Market maker bot" spec main

let () = Command.run command
