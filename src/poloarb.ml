open Core.Std
open Async.Std
open Log.Global

open Dtc.Dtc
open Bs_devkit.Core
open Bs_api.PLNX
open Graph

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

let make_find_negative_cycle buf =
  fun g symbol bid ask ->
    let currs = ["BTC"] in
    match String.split symbol ~on:'_' with
    | [quote; base] ->
      G.remove_edge g quote base;
      G.remove_edge g base quote;
      G.(add_edge_e g @@ E.create quote ((1. /. ask) *. 0.9975) base);
      G.(add_edge_e g @@ E.create base (bid *. 0.9975) quote);
      List.iter currs ~f:begin fun c -> try
        let cycle = BF.find_negative_cycle_from g c in
        Buffer.clear buf;
        let gain = List.fold_left cycle ~init:1. ~f:begin fun a (f, v, _) ->
            Buffer.(add_string buf f; add_string buf " -> ");
            a *. v
          end
        in
        Buffer.add_string buf @@ Printf.sprintf "%.3f" gain;
        info "ARB %s" @@ Buffer.contents buf
      with Not_found -> ()
      end;
    | _ -> invalid_arg "quote_base_of_symbol"

let find_negative_cycle = make_find_negative_cycle @@ Buffer.create 64

type entry = { qty: int; seq: int }
type book = entry Int.Map.t
type books = { bids: book; asks: book }

let books : books String.Table.t = String.Table.create ()

let top_changed ~side ~oldb ~newb = match side with
| Buy -> Int.Map.max_elt oldb <> Int.Map.max_elt newb
| Sell -> Int.Map.min_elt oldb <> Int.Map.min_elt newb

let get_entry ~symbol ~side ~price =
  let { bids; asks } = String.Table.find_exn books symbol in
  Int.Map.find (match side with Buy -> bids | Sell -> asks) price

let best_entry ~symbol ~side =
  let { bids; asks } = String.Table.find_exn books symbol in
  match side with
  | Buy -> Int.Map.max_elt bids
  | Sell -> Int.Map.min_elt asks

let find_negative_cycle ~symbol =
  debug "find negative cycle %s" symbol;
  let best_bid = best_entry ~symbol ~side:Buy in
  let best_ask = best_entry ~symbol ~side:Sell in
  match best_bid, best_ask with
  | Some (bb,_), Some (ba,_) ->
    find_negative_cycle g symbol (bb // 100_000_000) (ba // 100_000_000)
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

let ws buf ws_init =
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
    let seq = match List.Assoc.find_exn kwArgs "seq" with Msgpck.Int i -> i | _ -> failwith "seq" in
    let iter_f msg =
      let open Ws.Msgpck in
      match of_msgpck msg with
      | Error msg -> failwith msg
      | Ok { typ="newTrade"; data } ->
        let trade = trade_of_msgpck data in
        debug "%d T %s" seq @@ Fn.compose Sexp.to_string DB.sexp_of_trade trade;
      | Ok { typ="orderBookModify"; data } ->
        let ({ side; price; qty } as update): DB.book_entry = book_of_msgpck data in
        debug "%d M %s" seq @@ Fn.compose Sexp.to_string DB.sexp_of_book_entry update;
        let changed = set_entry ~symbol ~side ~seq ~price ~qty in
        if changed then find_negative_cycle ~symbol
      | Ok { typ="orderBookRemove"; data } ->
        let ({ side; price; qty } as update): DB.book_entry = book_of_msgpck data in
        debug "%d D %s" seq @@ Fn.compose Sexp.to_string DB.sexp_of_book_entry update;
        let changed = del_entry side symbol ~seq ~price in
        if changed then find_negative_cycle ~symbol
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
  Rest.books ~buf ?depth () >>| fun bs ->
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
  end

let update_books ?depth buf =
  Rest.books ~buf ?depth () >>| fun bs ->
  List.iter bs ~f:begin fun (symbol, { asks; bids; isFrozen; seq }) ->
    List.iter bids ~f:begin fun { side; price; qty } ->
      let (_:bool) = set_entry ~symbol ~side ~price ~qty ~seq in ()
    end;
    List.iter asks ~f:begin fun { side; price; qty } ->
      let (_:bool) = set_entry ~symbol ~side ~price ~qty ~seq in ()
    end
  end

let main daemon pidfile logfile loglevel () =
  if daemon then Daemon.daemonize ~cd:"." ();
  don't_wait_for begin
    Lock_file.create_exn pidfile >>= fun () ->
    set_output Log.Output.[stderr (); file `Text ~filename:logfile];
    set_level (match loglevel with 2 -> `Info | 3 -> `Debug | _ -> `Error);
    let buf = Bi_outbuf.create 4096 in
    let ws_init = Ivar.create () in
    info "poloarb starting";
    Rest.currencies ~buf () >>= fun currs ->
    List.iter currs ~f:(fun (c, _) -> G.add_vertex g c);
    load_books ~depth:0 buf >>= fun () ->
    let ws_t = ws buf ws_init in
    Ivar.read ws_init >>= fun () ->
    update_books buf >>= fun () ->
    ws_t
  end;
  never_returns @@ Scheduler.go ()

let command =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-daemon" no_arg ~doc:" Daemonize"
    +> flag "-pidfile" (optional_with_default "run/poloarb.pid" string) ~doc:"filename Path of the pid file (run/poloarb.pid)"
    +> flag "-logfile" (optional_with_default "log/poloarb.log" string) ~doc:"filename Path of the log file (log/poloarb.log)"
    +> flag "-loglevel" (optional_with_default 1 int) ~doc:"1-3 loglevel"
  in
  Command.basic ~summary:"Poloniex arbitrageur" spec main

let () = Command.run command
