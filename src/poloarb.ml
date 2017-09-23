open Core
open Async
open Log.Global

open Bs_devkit
open Graph

module CycleTbl = Hashtbl.Make (struct
    type t = (string * float * string) list [@@deriving sexp, hash]
    let compare = [%compare: t]

    let hash_fold_t state pairs =
      let len = List.fold_left pairs ~init:0 ~f:begin fun a (q,_,b) ->
          a + String.length q + String.length b
        end
      in
      let state = ref (hash_fold_int state len) in
      List.iter pairs ~f:begin fun (q,_,b) ->
        state := hash_fold_string !state q;
        state := hash_fold_string !state b
      end;
      !state
  end)

let string_of_cycle buf cycle =
  Buffer.clear buf;
  let gain = List.fold_left cycle ~init:1. ~f:begin fun a (f, v, _) ->
      Buffer.(add_string buf f; add_string buf " -> ");
      a *. v
    end
  in
  Buffer.add_string buf @@ Printf.sprintf "%.3f" gain;
  Buffer.contents buf

let string_of_cycle = string_of_cycle (Buffer.create 128)

let cycles : unit CycleTbl.t = CycleTbl.create ()

type entry = { qty: Float.t ; seq: int }
type book = entry Float.Map.t
type books = { bids: book; asks: book }

let arbitrable_symbols = ref String.Set.empty
let books : books String.Table.t = String.Table.create ()
let balances : Float.t String.Table.t = String.Table.create ()

type vwap = { bid: float; bid_qty: float; ask: float; ask_qty: float }
let vwaps : vwap String.Table.t = String.Table.create ()

let key = ref ""
let secret = ref ""
let fees = ref Float.one
let threshold = ref 0.

let top_changed ~side ~oldb ~newb = match side with
| `Buy -> Float.Map.max_elt oldb <> Float.Map.max_elt newb
| `Sell -> Float.Map.min_elt oldb <> Float.Map.min_elt newb

let get_entry ~symbol ~side ~price =
  let book = String.Table.find books symbol in
  Option.bind book ~f:begin fun { bids; asks } ->
    Float.Map.find (match side with `Buy -> bids | `Sell -> asks) price
  end

let best_entry ~symbol ~side =
  let book = String.Table.find books symbol in
  Option.bind book ~f:begin fun { bids; asks } ->
    match side with
    | `Buy -> Float.Map.max_elt bids
    | `Sell -> Float.Map.min_elt asks
  end

exception Result of float * float
let return_result ~qty ~vwap = raise (Result (vwap, qty))

let vwap ~symbol ~side ~threshold =
  let fold_f_exn ~threshold ~key:price ~data:{ qty } (vwap, oldq) =
    (* info "vwap %s %d %d %d %f %d" symbol price qty oldq vwap threshold; *)
    if oldq +. qty <= threshold
    then
      vwap +. price *. qty, oldq +. qty
    else
    let rem_qty = threshold -. oldq in
    return_result ~qty:(oldq +. rem_qty) ~vwap:(vwap +. price *. rem_qty)
  in
  let open Option.Monad_infix in
  best_entry ~symbol ~side >>= fun (price, _qty) ->
  let threshold = threshold /. price in
  String.Table.find books symbol >>| fun { bids; asks } ->
  match side with
  | `Buy -> begin try
      Float.Map.fold_right bids ~init:(0., 0.) ~f:(fold_f_exn ~threshold)
    with Result (vwap, q) -> (vwap, q)
    end
  | `Sell -> begin try
      Float.Map.fold asks ~init:(0., 0.) ~f:(fold_f_exn ~threshold)
    with Result (vwap, q) -> (vwap, q)
    end

let symbol_of_currs ~quote ~base = quote ^ "_" ^ base

type symbol = { quote: string; base: string }

let currs : symbol String.Table.t = String.Table.create ()

let currs_of_symbol sym =
  match String.split sym ~on:'_' with
  | [quote; base] -> { quote ; base }
  | _ -> invalid_argf "currs_of_symbol: %s" sym ()

let execute_arbitrage ?(dry_run=true) ?buf cycle =

  let iter_f (c1, _, c2) =
    let symbol = symbol_of_currs c1 c2 in
    let symbol, inverted =
      if String.Table.mem books symbol then symbol, false else symbol_of_currs c2 c1, true
    in
    let side = if inverted then `Buy else `Sell in
    match String.Table.find vwaps symbol with
    | None -> failwithf "No vwaps for %s" symbol ()
    | Some { bid; bid_qty; ask; ask_qty } ->
      let price, qty = match side with
      | `Buy -> bid, bid_qty
      | `Sell -> ask, ask_qty in
      let c1_balance = String.Table.find_exn balances c1 in
      let pricef' = if inverted then 1. /. price else price in
      let q2 = if inverted then qty *. price else qty in
      let q2 = Float.min q2 (c1_balance /. pricef') in
      match dry_run with
      | true ->
        let q1 = q2 *. pricef' in
        String.Table.update balances c1 ~f:(function None -> assert false | Some v -> v -. q1);
        String.Table.update balances c2 ~f:(function None -> assert false | Some v -> v +. q2);
        info "[Sim] %s (%f) -> %s (%f) [%f]"
          c1 (String.Table.find_exn balances c1)
          c2 (String.Table.find_exn balances c2)
          pricef';
        Deferred.unit
      | false ->
        Plnx_rest.submit_order ?buf ~key:!key ~secret:!secret
          ~side:`buy
          ~tif:`Fill_or_kill
          ~symbol
          ~price
          ~qty:q2 () >>| function
        | Error err -> failwith (Plnx_rest.Http_error.to_string err)
        | Ok { id; trades; amount_unfilled } ->
          let trades =
            List.fold_left trades ~init:[] ~f:begin fun a (_, trades) ->
              List.rev_append trades a
            end in
          let q1, q2 =
            List.fold_left trades
              ~init:(0., 0.)
              ~f:begin fun (q1, q2) { Plnx.Trade.ts ; side ; price ; qty } ->
                q1 -. qty *. price, q2 +. qty
              end
          in
          String.Table.update balances c1 ~f:(function None -> assert false | Some v -> v +. q1);
          String.Table.update balances c2 ~f:(function None -> assert false | Some v -> v +. q2);
          info "[Trade] %s (%f) -> %s (%f)" c1 q1 c2 q2
  in
  match CycleTbl.find cycles cycle with
  | Some _ -> Deferred.unit
  | None ->
    CycleTbl.set cycles cycle ();
    info "Processing %s" @@ string_of_cycle cycle;
    Monitor.try_with_or_error (fun () ->
        Monitor.protect
          (fun () -> Deferred.List.iter cycle ~f:iter_f)
          ~finally:(fun () -> return @@ CycleTbl.remove cycles cycle))
    >>| function
    | Ok () -> info "Done processing %s" @@ string_of_cycle cycle
    | Error err -> error "Error processing %s: %s" (string_of_cycle cycle) (Error.to_string_hum err)

module G = Imperative.Digraph.ConcreteBidirectionalLabeled (String) (struct include Float let default = 0. end)
module BF = Path.BellmanFord (G)
    (struct
      type edge = G.E.t
      include Float
      type label = G.E.label
      let weight (_, label, _) = label
      let add e e' = -. Float.abs (e * e')
      let zero = minus_one
    end)

let g = G.create ()
type edge_list = (string * float * string) list [@@deriving sexp]

let rec fix_cycle res = function
| [] -> []
| ("BTC", _, _) :: t as rest -> rest @ List.rev res
| h::t -> fix_cycle (h :: res) t

let find_negative_cycle g ~symbol ~bid ~ask =
  let { quote; base } = String.Table.find_exn currs symbol in
  debug "find_negative_cycle %s %f %f" symbol bid ask;
  G.remove_edge g quote base;
  G.remove_edge g base quote;
  G.(add_edge_e g @@ E.create quote ((1. /. ask) *. !fees) base);
  G.(add_edge_e g @@ E.create base (bid *. !fees) quote);
  List.iter ["BTC"; "ETH"; "XMR"] ~f:begin fun c -> try
    let cycle = fix_cycle [] @@ BF.find_negative_cycle_from g c in
    if List.length cycle > 2 then begin
      don't_wait_for @@ execute_arbitrage cycle;
      info "ARB %s" @@ string_of_cycle cycle
    end
  with Not_found -> ()
  end

let find_negative_cycle ~symbol ~threshold =
  let bid = vwap ~symbol ~side:`Buy ~threshold in
  let ask = vwap ~symbol ~side:`Sell ~threshold in
  match bid, ask with
  | Some (bpaid, bqty), Some (apaid, aqty) ->
    let bvwap = bpaid /. bqty in
    let avwap = apaid /. aqty in
    (* info "find_negative_cycle: %s %f %d %f %d %f %f" symbol bpaid bqty apaid aqty bvwap avwap; *)
    String.Table.set vwaps ~key:symbol ~data:begin
      { bid = bpaid ; bid_qty = bqty ; ask = apaid ; ask_qty = aqty }
    end ;
    find_negative_cycle g ~symbol ~bid:bvwap ~ask:avwap
  | _ -> ()

let set_entry ~symbol ~side ~seq ~price ~qty =
  let { bids; asks } = String.Table.find_exn books symbol in
  match side with
  | `Buy ->
    let bids' =
      match Float.Map.find bids price with
      | None -> Float.Map.add bids price { seq; qty }
      | Some entry ->
        if entry.seq > seq then bids else Float.Map.add bids price { seq; qty }
    in
    String.Table.set books symbol { bids=bids'; asks };
    top_changed side bids bids'
  | `Sell ->
    let asks' =
      match Float.Map.find asks price with
      | None -> Float.Map.add asks price { seq; qty }
      | Some entry ->
        if entry.seq > seq then asks else Float.Map.add asks price { seq; qty }
    in
    String.Table.set books symbol { bids; asks=asks' };
    top_changed side asks asks'

let del_entry ~symbol ~side ~seq ~price =
  let { bids; asks } = String.Table.find_exn books symbol in
  match side with
  | `Buy ->
    let bids' =
      match Float.Map.find bids price with
      | None -> bids
      | Some entry ->
        if entry.seq > seq then bids else Float.Map.remove bids price
    in
    String.Table.set books symbol { bids=bids'; asks };
    top_changed side bids bids'
  | `Sell ->
    let asks' =
      match Float.Map.find asks price with
    | None -> asks
    | Some entry ->
      if entry.seq > seq then asks else Float.Map.remove asks price
    in
    String.Table.set books symbol { bids; asks=asks' };
    top_changed side asks asks'

let ws buf ws_init ob_initialized =
  let module W = Plnx_ws_new in
  let to_ws, to_ws_w = Pipe.create () in
  let subid_to_sym = Int.Table.create () in
  let initialized = ref false in
  let latest_ts = ref Time_ns.epoch in
  let timeout = Time_ns.Span.of_int_sec 30 in
  let on_trade ~id ~symbol ~ts ~price ~qty ~side =
    let price = satoshis_int_of_float_exn price in
    let qty = satoshis_int_of_float_exn qty in
    let trade = { DB.ts = ts ; side = side ; price ; qty } in
    debug "%s %d T %s" symbol id @@ Fn.compose Sexp.to_string DB.sexp_of_trade trade
  in
  let on_update ~id ~symbol ~side ~price ~qty =
    if qty = 0. then begin
      let _changed = del_entry ~symbol ~side ~seq:id ~price in
      if !ob_initialized then find_negative_cycle ~symbol ~threshold:!threshold
    end else begin
      let _changed = set_entry ~symbol ~side ~seq:id ~price ~qty in
      if !ob_initialized then find_negative_cycle ~symbol ~threshold:!threshold ;
    end
  in
  let on_event subid id = function
  | W.Repr.Snapshot { symbol ; bid ; ask } ->
    let { base } = String.Table.find_exn currs symbol in
    if String.Set.mem !arbitrable_symbols base then begin
      Int.Table.set subid_to_sym subid symbol ;
      if Int.Table.length subid_to_sym = String.Table.length books then
        Ivar.fill_if_empty ws_init () ;
      Float.Map.iteri bid ~f:(fun ~key ~data -> on_update ~id ~symbol ~side:`Buy ~price:key ~qty:data) ;
      Float.Map.iteri ask ~f:(fun ~key ~data -> on_update ~id ~symbol ~side:`Sell ~price:key ~qty:data)
    end
  | Update { side; price; qty } ->
    let symbol = Int.Table.find_exn subid_to_sym subid in
    let side = match side with | `buy -> `Buy | `sell -> `Sell | `buy_sell_unset -> invalid_arg "`buy_sell_unset" in
    let { base } = String.Table.find_exn currs symbol in
    if String.Set.mem !arbitrable_symbols base then
      on_update ~id ~symbol ~side ~price ~qty
  | Trade { gid; id; ts; side; price; qty } ->
    let symbol = Int.Table.find_exn subid_to_sym subid in
    let side = match side with | `buy -> `Buy | `sell -> `Sell | `buy_sell_unset -> invalid_arg "`buy_sell_unset" in
    let { base } = String.Table.find_exn currs symbol in
    if String.Set.mem !arbitrable_symbols base then
      on_trade ~id ~symbol ~ts ~price ~qty ~side
  in
  let on_ws_msg = function
  | W.Repr.Error msg ->
    error "%s" msg
  | Event { subid ; id ; events } ->
    if not !initialized then begin
      let symbols = String.Table.keys books in
      List.iter symbols ~f:begin fun symbol ->
        Pipe.write_without_pushback to_ws_w (W.Repr.Subscribe symbol)
      end ;
      initialized := true
    end ;
    List.iter events ~f:(on_event subid id)
  in
  let connected = Condition.create () in
  let restart, ws = W.open_connection ~connected to_ws in
  let rec handle_init () =
    Condition.wait connected >>= fun () ->
    initialized := false ;
    handle_init () in
  don't_wait_for (handle_init ()) ;
  let watchdog () =
    let now = Time_ns.now () in
    let diff = Time_ns.diff now !latest_ts in
    if Time_ns.(!latest_ts <> epoch) && Time_ns.Span.(diff > timeout) then
      Condition.signal restart () in
  Clock_ns.every timeout watchdog ;
  Monitor.handle_errors begin fun () ->
    Pipe.iter_without_pushback ~continue_on_error:true ws ~f:on_ws_msg
  end begin fun exn ->
    error "%s" @@ Exn.to_string exn
  end

let load_books_for_symbol ?depth buf symbol =
  Plnx_rest.books ~buf ?depth symbol >>| function
  | Error err -> ()
  | Ok { asks; bids; isFrozen; seq } ->
    let bids =
      List.fold_left bids ~init:Float.Map.empty ~f:begin fun a { side; price; qty } ->
        Float.Map.add a ~key:price ~data:{ qty; seq }
      end
    in
    let asks =
      List.fold_left asks ~init:Float.Map.empty ~f:begin fun a { side; price; qty } ->
        Float.Map.add a ~key:price ~data:{ qty; seq }
      end
    in
    String.Table.set books symbol { bids; asks }

let update_books_for_symbol ?depth buf symbol =
  Plnx_rest.books ~buf ?depth symbol >>| function
  | Error _ -> ()
  | Ok { asks; bids; isFrozen; seq } ->
    List.iter bids ~f:begin fun { side; price; qty } ->
      let side = match side with | `buy -> `Buy | `sell -> `Sell | `buy_sell_unset -> invalid_arg "`buy_sell_unset" in
      let (_:bool) = set_entry ~symbol ~side ~price ~qty ~seq in ()
    end;
    List.iter asks ~f:begin fun { side; price; qty } ->
      let side = match side with | `buy -> `Buy | `sell -> `Sell | `buy_sell_unset -> invalid_arg "`buy_sell_unset" in
      let (_:bool) = set_entry ~symbol ~side ~price ~qty ~seq in ()
    end

let select_vertices syms =
  let btc, eth, xmr, usdt =
    List.fold_left syms ~init:String.Set.(empty, empty, empty, empty) ~f:begin fun (btc, eth, xmr, usdt) sym ->
      let ({ quote; base } as c) = currs_of_symbol sym in
      String.Table.set currs sym c;
      match quote with
      | "BTC" -> String.Set.add btc base, eth, xmr, usdt
      | "ETH" -> btc, String.Set.add eth base, xmr, usdt
      | "XMR" -> btc, eth, String.Set.add xmr base, usdt
      | "USDT" -> btc, eth, xmr, String.Set.add usdt base
      | _ -> invalid_arg "unknown quote currency"
    end
  in
  let init = String.Set.of_list ["BTC"; "ETH"; "XMR"; "USDT"] in
  let init = String.Set.fold btc ~init ~f:(fun a c -> if String.Set.(mem eth c || mem xmr c || mem usdt c) then String.Set.add a c else a) in
  let init = String.Set.fold eth ~init ~f:(fun a c -> if String.Set.(mem xmr c || mem usdt c || mem btc c) then String.Set.add a c else a) in
  let init = String.Set.fold xmr ~init ~f:(fun a c -> if String.Set.(mem usdt c || mem btc c || mem eth c) then String.Set.add a c else a) in
  String.Set.fold usdt ~init ~f:(fun a c -> if String.Set.(mem btc c || mem eth c || mem xmr c) then String.Set.add a c else a)

let main cfg daemon pidfile logfile loglevel fees' min_qty () =
  threshold := min_qty;
  fees := 1. -. fees' // 10_000;
  if daemon then Daemon.daemonize ~cd:"." ();
  stage begin fun `Scheduler_started ->
    Lock_file.create_exn pidfile >>= fun () ->
    set_output Log.Output.[stderr (); file `Text ~filename:logfile];
    set_level (match loglevel with 2 -> `Info | 3 -> `Debug | _ -> `Error);
    let buf = Bi_outbuf.create 4096 in
    let ws_init = Ivar.create () in
    let cfg = begin match Sexplib.Sexp.load_sexp_conv cfg Cfg.t_of_sexp with
    | `Error (exn, _) -> raise exn
    | `Result cfg -> cfg
    end in
    let cfg = List.Assoc.find_exn cfg ~equal:String.(=) "PLNX" in
    key := cfg.key;
    secret := cfg.secret;
    info "poloarb starting";
    Plnx_rest.balances ~buf ~key:!key ~secret:!secret () >>= function
    | Error _ -> Deferred.unit
    | Ok bs ->
      List.iter bs ~f:begin fun (symbol, { available; on_orders; btc_value }) ->
        if symbol = "BTC" then info "%s balance %f XBt" symbol available;
        String.Table.set balances ~key:symbol ~data:available
      end;
      info "Loaded %d balances" @@ List.length bs;
      Plnx_rest.symbols ~buf () >>= function
      | Error _ -> Deferred.unit
      | Ok syms ->
        let vs = select_vertices syms in
        arbitrable_symbols := vs;
        info "Currencies %s" (String.Set.sexp_of_t vs |> Sexplib.Sexp.to_string);
        String.Set.iter vs ~f:(G.add_vertex g);
        Deferred.List.iter
          ~how:`Parallel syms ~f:(load_books_for_symbol ~depth:0 buf) >>= fun () ->
        let ob_initialized = ref false in
        let ws_t = ws buf ws_init ob_initialized in
        Ivar.read ws_init >>= fun () ->
        Deferred.List.iter
          ~how:`Parallel syms ~f:(update_books_for_symbol buf) >>= fun () ->
        ob_initialized := true;
        ws_t
  end

let command =
  let default_cfg = Filename.concat (Option.value_exn (Sys.getenv "HOME")) ".virtu" in
  let spec =
    let open Command.Spec in
    empty
    +> flag "-cfg" (optional_with_default default_cfg string) ~doc:"path Filepath of config file (default: ~/.virtu)"
    +> flag "-daemon" no_arg ~doc:" Daemonize"
    +> flag "-pidfile" (optional_with_default "run/poloarb.pid" string) ~doc:"filename Path of the pid file (run/poloarb.pid)"
    +> flag "-logfile" (optional_with_default "log/poloarb.log" string) ~doc:"filename Path of the log file (log/poloarb.log)"
    +> flag "-loglevel" (optional_with_default 1 int) ~doc:"1-3 loglevel"
    +> flag "-fees" (optional_with_default 25 int) ~doc:"bps fees (default: 25)"
    +> flag "-min-qty" (optional_with_default 0.5 float) ~doc:"sats Min qty to arbitrage (default: 0.5 XBT)"
  in
  Command.Staged.async ~summary:"Poloniex arbitrageur" spec main

let () = Command.run command
