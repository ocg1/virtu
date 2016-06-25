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

type instrument_info = {
  bids_initialized: unit Ivar.t;
  asks_initialized: unit Ivar.t;
  max_pos_size: int;
} [@@deriving create]

let quoted_instruments : instrument_info String.Table.t = String.Table.create ()

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
        | `Best ->
          let midPrice = (bb + ba) / 2 in
          midPrice, ba - midPrice
        | `Vwap ->
          let midPrice = (vb + va) / 2 in
          midPrice, va - midPrice
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

  let mid_price symbol = function
    | `Best ->
      let best_bid = best_price Side.Bid symbol in
      let best_ask = best_price Ask symbol in
      Option.map2 best_bid best_ask (fun (bb,_) (ba,_) ->
          let midPrice =  (bb + ba) / 2 in
          midPrice, ba - midPrice
        )
    | `Vwap ->
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
    Option.map (mid_price sym `Vwap)
      ~f:(fun (midPrice, _) -> 10_000_000_000_000_000 / midPrice)
  | "ETHXBT" -> Option.map ~f:fst @@ mid_price sym `Vwap
  | "LTCXBT" -> Option.map ~f:fst @@ mid_price sym `Vwap
  | "LSKXBT" -> Option.map ~f:fst @@ mid_price sym `Vwap
  | "FCTXBT" -> Option.map ~f:fst @@ mid_price sym `Vwap
  | _ -> invalid_arg "xbt_value"
end

module Order = struct
  let submit orders =
    if orders = [] then Deferred.unit
    else if !dry_run then begin
      info "[Sim] submit %s" Yojson.Safe.(to_string @@ `List orders);
      Deferred.unit
    end
    else
    Rest.Order.submit ~testnet:!testnet ~log:Lazy.(force log) ~key:!api_key ~secret:!api_secret orders >>| function
    | Ok msg -> debug "<- submit %s" msg
    | Error err -> error "<- submit %s" @@ Error.to_string_hum err

  let update orders =
    if orders = [] then Deferred.unit
    else if !dry_run then begin
      info "[Sim] update %s" Yojson.Safe.(to_string @@ `List orders);
      Deferred.unit
    end
    else
    Rest.Order.update ~testnet:!testnet ~log:Lazy.(force log) ~key:!api_key ~secret:!api_secret orders >>| function
    | Ok msg -> debug "<- update %s" msg
    | Error err -> error "<- update %s" @@ Error.to_string_hum err

  let cancel orderID =
    if !dry_run then begin
      info "[Sim] cancel %s" @@ Uuid.to_string orderID;
      Deferred.unit
    end
    else
    Rest.Order.cancel ~testnet:!testnet ~log:Lazy.(force log) ~key:!api_key ~secret:!api_secret orderID >>| function
    | Ok msg -> debug "<- cancel %s" msg
    | Error err -> error "<- cancel %s" @@ Error.to_string_hum err

  let cancel_all ?symbol ?filter () =
    if !dry_run then begin
      info "[Sim] cancel_all";
      Deferred.Or_error.return ""
    end
    else
    Rest.Order.cancel_all ~testnet:!testnet ~log:Lazy.(force log) ~key:!api_key ~secret:!api_secret ?symbol ?filter ()

  let cancel_all_after timeout =
    if !dry_run then begin
      info "[Sim] cancel_all_after";
      Deferred.unit
    end
    else
    Rest.Order.cancel_all_after ~testnet:!testnet ~log:Lazy.(force log) ~key:!api_key ~secret:!api_secret timeout >>| function
    | Ok msg -> debug "<- cancel_all_after %s" msg
    | Error err -> error "<- cancel_all_after %s" @@ Error.to_string_hum err
end

let dead_man's_switch timeout period =
  let rec loop () =
    try_with ~name:"dead man's switch" (fun () -> Order.cancel_all_after timeout) >>= function
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

let on_position_update (symbol: string) p oldp =
  let { max_pos_size } = String.Table.find_exn quoted_instruments symbol in
  let currentQty = RespObj.int64_exn p "currentQty" |> Int64.to_int_exn in
  let oldQty = Option.value_map oldp ~default:0 ~f:(fun oldp -> RespObj.int64_exn p "currentQty" |> Int64.to_int_exn) in
  let bidOrderID = String.Table.find current_bids symbol in
  let askOrderID = String.Table.find current_asks symbol in
  if currentQty <> oldQty || Option.is_none bidOrderID || Option.is_none askOrderID then
    let fw_p_to_hedger c = Rpc.Rpc.dispatch Protocols.Position.t c (symbol, currentQty) in
    don't_wait_for @@ Deferred.ignore @@ Rpc.Connection.with_client ~host:"localhost" ~port:!hedger_port fw_p_to_hedger;
    match OB.mid_price symbol `Best, RemoteOB.mid_price symbol `Vwap with
    | Some (midPrice, spread), Some (rMidPrice, rSpread) when rSpread < spread ->
      info "on_position_update: %d %d %d %d" midPrice spread rMidPrice rSpread;
      let bidPrice = midPrice - rSpread in
      let askPrice = midPrice + rSpread in
      let bidOrder = Option.bind bidOrderID (Uuid.Table.find orders) in
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
    | Some (midPrice, spread), Some (rMidPrice, rSpread) ->
      info "on_position_update: %d %d %d %d" midPrice spread rMidPrice rSpread
    | Some (midPrice, spread), None ->
      info "on_position_update: %d %d" midPrice spread
    | None, _ ->
      info "on_position_update: waiting for %s orderBookL2" symbol

let on_ticker_update { Protocols.OrderBook.symbol; side; best; vwap } =
  debug "<- tickup %s %s %d %d" symbol (Side.show side) best vwap;
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
      | Some _ ->
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
        ] >>| fun () ->
        info "on_quote_change: update %s %s %d" symbol (Side.show side) vwap
    end
  | _ -> Deferred.unit

let instruments_initialized = Ivar.create ()
let orders_initialized = Ivar.create ()
let bid_set = Ivar.create ()
let ask_set = Ivar.create ()

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
  let on_update p =
    let p = RespObj.of_json p in
    let sym = RespObj.string_exn p "symbol" in
    if String.Table.mem quoted_instruments sym then
      let oldp = String.Table.find positions sym in
      let p = Option.value_map oldp
          ~default:p ~f:(fun oldp -> RespObj.merge oldp p)
      in
      String.Table.set positions sym p;
      let currentQty = RespObj.int64_exn p "currentQty" in
      debug "<- update position %s %Ld" sym currentQty;
      on_position_update sym p oldp
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
  | Delete -> List.iter data ~f:on_delete
  | _ -> List.iter data ~f:on_update

let on_orderbook action data =
  let action_str = show_update_action action in
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
    let side, best_elt, books, vwaps = match buy_sell_of_bmex h.OrderBook.L2.side with
    | `Buy -> Side.Bid, Int.Map.max_elt, OB.bids, OB.bid_vwaps
    | `Sell -> Ask, Int.Map.min_elt, OB.asks, OB.ask_vwaps
    in
    let { max_pos_size } = String.Table.find_exn quoted_instruments h.symbol in
    let old_book = String.Table.find_exn books h.symbol in
    let new_book = List.fold_left us ~init:old_book ~f:(update_depth action) in
    String.Table.set books h.symbol new_book;

    (* compute vwap *)
    let max_pos_p, total_v = vwap ~vlimit:max_pos_size side new_book in
    String.Table.set vwaps ~key:h.symbol ~data:(OB.create_vwap ~max_pos_p ~total_v ());
    info "BMEX %s %s best=%d vwap=%d totalv=%d"
      h.symbol (Side.show side)
      (Option.value_map (best_elt new_book) ~default:0 ~f:(fun (v, _) -> v))
      (max_pos_p / total_v)
      total_v;

    if action = Partial then begin
      debug "%s %s initialized" h.symbol (Side.show side);
      let { bids_initialized; asks_initialized } = String.Table.find_exn quoted_instruments h.symbol in
      match side with
      | Bid -> Ivar.fill_if_empty bids_initialized ()
      | Ask -> Ivar.fill_if_empty asks_initialized ()
    end

  | _ -> ()
  in
  don't_wait_for begin
    Ivar.read instruments_initialized >>| fun () ->
    List.iter data ~f:iter_f
  end

let market_make buf instruments =
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
  | Ok _ ->
    info "all orders canceled";
    let topics =
      "instrument" ::
      List.map instruments ~f:(fun i -> "orderBookL2:" ^ i) @
      ["order"; "execution"; "margin"; "position"]
    in
    don't_wait_for @@ dead_man's_switch 60000 15.;
    after @@ Time.Span.of_ms 50. >>= fun () ->
    Ws.with_connection
      ~testnet:!testnet
      ~auth:(!api_key, !api_secret)
      ~topics
      ~on_ws_msg ()

let rpc_client port =
  let open Rpc in
  let open Protocols in
  let on_msg = function
  | OrderBook.Subscribed (sym, max_pos) ->
    info "hedger subscribed to %s %d" sym max_pos;
    Deferred.unit
  | Ticker t -> on_ticker_update t
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
      Pipe.iter feed ~f:on_msg
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
        after @@ Time.Span.of_int_sec 5 >>= loop
    | Error exn ->
        error "rpc_client crashed: %s" @@ Exn.(to_string exn);
        after @@ Time.Span.of_int_sec 5 >>= loop
  in loop ()

let main cfg port daemon pidfile logfile loglevel main dry_run' instruments () =
  don't_wait_for begin
    Lock_file.create_exn pidfile >>= fun () ->
    dry_run := dry_run';
    testnet := not main;
    let cfg = Yojson.Safe.from_file cfg |> Cfg.of_yojson |> presult_exn in
    let { Cfg.key; secret; quote } = List.Assoc.find_exn cfg (if main then "BMEX" else "BMEXT") in
    api_key := key;
    api_secret := Cstruct.of_string secret;
    let instruments = if instruments = [] then quote else instruments in
    let buf = Bi_outbuf.create 4096 in
    if daemon then Daemon.daemonize ~cd:"." ();
    set_output Log.Output.[stderr (); file `Text ~filename:logfile];
    set_level (match loglevel with 2 -> `Info | 3 -> `Debug | _ -> `Error);
    hedger_port := port;
    List.iter instruments ~f:(fun (k, v) ->
        String.Table.set quoted_instruments ~key:k
          ~data:Ivar.(create_instrument_info (create ()) (create ()) v ())
      );
    Deferred.(all_unit [
        ignore @@ rpc_client port;
        market_make buf @@ String.Table.keys quoted_instruments;
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
    +> flag "-main" no_arg ~doc:" Use mainnet"
    +> flag "-dry" no_arg ~doc:" Simulation mode"
    +> anon (sequence (t2 ("instrument" %: string) ("quote" %: int)))
  in
  Command.basic ~summary:"Market maker bot" spec main

let () = Command.run command
