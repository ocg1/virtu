open Core.Std
open Async.Std
open Log.Global

open Dtc.Dtc
open Bs_devkit.Core
open Bs_api.PLNX
open Graph

module CycleTbl = Hashtbl.Make (struct
    type t = (string * float * string) list [@@deriving sexp]
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

    let hash = [%hash: t]
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

type entry = { qty: int; seq: int }
type book = entry Int.Map.t
type books = { bids: book; asks: book }

let arbitrable_symbols = ref String.Set.empty
let books : books String.Table.t = String.Table.create ()
let balances : Float.t String.Table.t = String.Table.create ()

type vwap = { bid: float; bid_qty: int; ask: float; ask_qty: int } [@@deriving create]
let vwaps : vwap String.Table.t = String.Table.create ()

let key = ref ""
let secret = ref @@ Cstruct.of_string ""
let fees = ref Float.one
let threshold = ref 0

let top_changed ~side ~oldb ~newb = match side with
| Buy -> Int.Map.max_elt oldb <> Int.Map.max_elt newb
| Sell -> Int.Map.min_elt oldb <> Int.Map.min_elt newb

let get_entry ~symbol ~side ~price =
  let book = String.Table.find books symbol in
  Option.bind book ~f:begin fun { bids; asks } ->
    Int.Map.find (match side with Buy -> bids | Sell -> asks) price
  end

let best_entry ~symbol ~side =
  let book = String.Table.find books symbol in
  Option.bind book ~f:begin fun { bids; asks } ->
    match side with
    | Buy -> Int.Map.max_elt bids
    | Sell -> Int.Map.min_elt asks
  end

exception Result of float * int
let return_result ~qty ~vwap = raise (Result (vwap, qty))

let vwap ~symbol ~side ~threshold =
  let fold_f_exn ~threshold ~key:price ~data:{ qty } (vwap, oldq) =
    (* info "vwap %s %d %d %d %f %d" symbol price qty oldq vwap threshold; *)
    let pricef = price // 100_000_000 in
    if oldq + qty <= threshold
    then
      let qtyf = qty // 100_000_000 in
      vwap +. pricef *. qtyf, oldq + qty
    else
    let rem_qty = threshold - oldq in
    let rem_qtyf = rem_qty // 100_000_000 in
    return_result ~qty:(oldq + rem_qty) ~vwap:(vwap +. pricef *. rem_qtyf)
  in
  let open Option.Monad_infix in
  best_entry ~symbol ~side >>= fun (price, _qty) ->
  let threshold = ((threshold // price) *. 1e8) |> Int.of_float in
  String.Table.find books symbol >>| fun { bids; asks } ->
  match side with
  | Buy -> begin try
      Int.Map.fold_right bids ~init:(0., 0) ~f:(fold_f_exn ~threshold)
    with Result (vwap, q) -> (vwap, q)
    end
  | Sell -> begin try
      Int.Map.fold asks ~init:(0., 0) ~f:(fold_f_exn ~threshold)
    with Result (vwap, q) -> (vwap, q)
    end

let symbol_of_currs ~quote ~base = quote ^ "_" ^ base

type symbol = { quote: string; base: string } [@@deriving create]

let currs : symbol String.Table.t = String.Table.create ()

let currs_of_symbol sym =
  match String.split sym ~on:'_' with
  | [quote; base] -> create_symbol ~quote ~base ()
  | _ -> invalid_argf "currs_of_symbol: %s" sym ()

let execute_arbitrage ?(dry_run=true) ?buf cycle =
  let iter_f (c1, _, c2) =
    let symbol = symbol_of_currs c1 c2 in
    let symbol, inverted =
      if String.Table.mem books symbol then symbol, false else symbol_of_currs c2 c1, true
    in
    let side = if inverted then Buy else Sell in
    match String.Table.find vwaps symbol with
    | None -> failwithf "No vwaps for %s" symbol ()
    | Some { bid; bid_qty; ask; ask_qty } ->
      let price, qty = match side with Buy -> bid, bid_qty // 100_000_000 | Sell -> ask, ask_qty // 100_000_000 in
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
        Rest.order ?buf ~key:!key ~secret:!secret ~side:Buy
          ~tif:Fill_or_kill ~symbol ~price:(satoshis_int_of_float_exn price) ~qty:(satoshis_int_of_float_exn q2) () >>| fun res ->
        let { Rest.id; trades; amount_unfilled } = Or_error.ok_exn res in
        let q1, q2 = List.fold_left trades ~init:(0., 0.) ~f:begin fun (q1, q2) { gid; id; trade={ ts; side; price; qty } } ->
            let price = price // 100_000_000 in
            let qty = qty // 100_000_000 in
            q1 -. qty *. price, q2 +. qty
          end
        in
        String.Table.update balances c1 ~f:(function None -> assert false | Some v -> v +. q1);
        String.Table.update balances c2 ~f:(function None -> assert false | Some v -> v +. q2);
        info "[Trade] %s (%f) -> %s (%f)" c1 q1 c2 q2;
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
  let bid = vwap ~symbol ~side:Buy ~threshold in
  let ask = vwap ~symbol ~side:Sell ~threshold in
  match bid, ask with
  | Some (bpaid, bqty), Some (apaid, aqty) ->
    let bvwap = bpaid /. (bqty // 100_000_000) in
    let avwap = apaid /. (aqty // 100_000_000) in
    (* info "find_negative_cycle: %s %f %d %f %d %f %f" symbol bpaid bqty apaid aqty bvwap avwap; *)
    String.Table.set vwaps ~key:symbol ~data:(create_vwap ~bid:bpaid ~bid_qty:bqty ~ask:apaid ~ask_qty:aqty ());
    find_negative_cycle g ~symbol ~bid:bvwap ~ask:avwap
  | _ -> ()

let set_entry ~symbol ~side ~seq ~price ~qty =
  let { bids; asks } = String.Table.find_exn books symbol in
  match side with
  | Buy ->
    let bids' = match Int.Map.find bids price with
    | None -> Int.Map.add bids price { seq; qty }
    | Some entry -> if entry.seq > seq then bids else Int.Map.add bids price { seq; qty }
    in
    String.Table.set books symbol { bids=bids'; asks };
    top_changed side bids bids'
  | Sell ->
    let asks' = match Int.Map.find asks price with
    | None -> Int.Map.add asks price { seq; qty }
    | Some entry -> if entry.seq > seq then asks else Int.Map.add asks price { seq; qty }
    in
    String.Table.set books symbol { bids; asks=asks' };
    top_changed side asks asks'

let del_entry side symbol ~seq ~price =
  let { bids; asks } = String.Table.find_exn books symbol in
  match side with
  | Buy ->
    let bids' = match Int.Map.find bids price with
    | None -> bids
    | Some entry -> if entry.seq > seq then bids else Int.Map.remove bids price
    in
    String.Table.set books symbol { bids=bids'; asks };
    top_changed side bids bids'
  | Sell ->
    let asks' = match Int.Map.find asks price with
    | None -> asks
    | Some entry -> if entry.seq > seq then asks else Int.Map.remove asks price
    in
    String.Table.set books symbol { bids; asks=asks' };
    top_changed side asks asks'

let ws buf ws_init ob_initialized =
  let to_ws, to_ws_w = Pipe.create () in
  let symbols_of_req_ids = ref [] in
  let symbols_of_sub_ids = Int.Table.create () in
  let on_ws_msg = function
  | Wamp.Welcome _ ->
    let symbols = String.Table.keys books in
    Ws.Msgpck.subscribe to_ws_w @@ symbols >>| fun reqids ->
    symbols_of_req_ids := Option.value_exn ~message:"Ws.subscribe" (List.zip reqids symbols)
  | Subscribed { reqid; id } ->
    let sym = List.Assoc.find_exn !symbols_of_req_ids reqid in
    Int.Table.set symbols_of_sub_ids id sym;
    debug "subscribed %s" sym;
    if Int.Table.length symbols_of_sub_ids = String.Table.length books then Ivar.fill_if_empty ws_init ();
    Deferred.unit
  | Event { Wamp.pubid; subid; details; args; kwArgs } ->
    let symbol = Int.Table.find_exn symbols_of_sub_ids subid in
    let { quote; base } = String.Table.find_exn currs symbol in
    if not @@ String.Set.mem !arbitrable_symbols base then Deferred.unit else
    let seq = match List.Assoc.find_exn kwArgs "seq" with Msgpck.Int i -> i | _ -> failwith "seq" in
    let iter_f msg =
      let open Ws.Msgpck in
      match of_msgpck msg with
      | Error msg -> failwith msg
      | Ok { typ="newTrade"; data } ->
        let trade = trade_of_msgpck data in
        debug "%s %d T %s" symbol seq @@ Fn.compose Sexp.to_string DB.sexp_of_trade trade;
      | Ok { typ="orderBookModify"; data } ->
        let ({ side; price; qty } as update): DB.book_entry = book_of_msgpck data in
        debug "%s %d M %s" symbol seq @@ Fn.compose Sexp.to_string DB.sexp_of_book_entry update;
        let _changed = set_entry ~symbol ~side ~seq ~price ~qty in
        if !ob_initialized then find_negative_cycle ~symbol ~threshold:!threshold
      | Ok { typ="orderBookRemove"; data } ->
        let ({ side; price; qty } as update): DB.book_entry = book_of_msgpck data in
        debug "%s %d D %s" symbol seq @@ Fn.compose Sexp.to_string DB.sexp_of_book_entry update;
        let _changed = del_entry side symbol ~seq ~price in
        if !ob_initialized then find_negative_cycle ~symbol ~threshold:!threshold
      | Ok { typ } -> failwithf "unexpected message type %s" typ ()
    in
    List.iter args ~f:iter_f;
    Deferred.unit
  | msg ->
    error "unknown message: %s" (Wamp.sexp_of_msg Msgpck.sexp_of_t msg |> Sexplib.Sexp.to_string);
    Deferred.unit
  in
  let ws = Ws.open_connection to_ws in
  Monitor.handle_errors
    (fun () -> Pipe.iter ~continue_on_error:true ws ~f:on_ws_msg)
    (fun exn -> error "%s" @@ Exn.to_string exn)

let load_books ?depth buf =
  Deferred.Or_error.ok_exn (Rest.books ~buf ?depth ()) >>| fun bs ->
  List.iter bs ~f:begin fun (symbol, { asks; bids; isFrozen; seq }) ->
    let bids =
      List.fold_left bids ~init:Int.Map.empty ~f:begin fun a { side; price; qty } ->
        Int.Map.add a ~key:price ~data:{ qty; seq }
      end
    in
    let asks =
      List.fold_left asks ~init:Int.Map.empty ~f:begin fun a { side; price; qty } ->
        Int.Map.add a ~key:price ~data:{ qty; seq }
      end
    in
    String.Table.set books symbol { bids; asks }
  end;
  info "Loaded %d books" @@ List.length bs

let update_books ?depth buf =
  Deferred.Or_error.ok_exn (Rest.books ~buf ?depth ()) >>| fun bs ->
  List.iter bs ~f:begin fun (symbol, { asks; bids; isFrozen; seq }) ->
    List.iter bids ~f:begin fun { side; price; qty } ->
      let (_:bool) = set_entry ~symbol ~side ~price ~qty ~seq in ()
    end;
    List.iter asks ~f:begin fun { side; price; qty } ->
      let (_:bool) = set_entry ~symbol ~side ~price ~qty ~seq in ()
    end;
  end;
  info "Updated %d books" @@ List.length bs

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
  don't_wait_for begin
    Lock_file.create_exn pidfile >>= fun () ->
    set_output Log.Output.[stderr (); file `Text ~filename:logfile];
    set_level (match loglevel with 2 -> `Info | 3 -> `Debug | _ -> `Error);
    let buf = Bi_outbuf.create 4096 in
    let ws_init = Ivar.create () in
    let cfg = Yojson.Safe.from_file cfg |> Cfg.of_yojson |> Result.ok_or_failwith in
    let cfg = List.Assoc.find_exn cfg "PLNX" in
    key := cfg.key;
    secret := Cstruct.of_string cfg.secret;
    info "poloarb starting";
    Deferred.Or_error.ok_exn (Rest.balances ~buf ~key:!key ~secret:!secret ()) >>= fun bs ->
    List.iter bs ~f:begin fun (symbol, { available; on_orders; btc_value }) ->
      if symbol = "BTC" then info "%s balance %d XBt" symbol available;
      String.Table.set balances ~key:symbol ~data:(available // 100_000_000)
    end;
    info "Loaded %d balances" @@ List.length bs;
    Deferred.Or_error.ok_exn (Rest.symbols ~buf ()) >>= fun syms ->
    let vs = select_vertices syms in
    arbitrable_symbols := vs;
    info "Currencies %s" (String.Set.sexp_of_t vs |> Sexplib.Sexp.to_string);
    String.Set.iter vs ~f:(G.add_vertex g);
    load_books ~depth:0 buf >>= fun () ->
    let ob_initialized = ref false in
    let ws_t = ws buf ws_init ob_initialized in
    Ivar.read ws_init >>= fun () ->
    update_books buf >>= fun () ->
    ob_initialized := true;
    ws_t
  end;
  never_returns @@ Scheduler.go ()

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
    +> flag "-min-qty" (optional_with_default 50_000_000 int) ~doc:"sats Min qty to arbitrage (default: 0.5 XBT)"
  in
  Command.basic ~summary:"Poloniex arbitrageur" spec main

let () = Command.run command
