open Core.Std
open Async.Std
open Log.Global

open Bs_devkit.Core
open Bs_api.PLNX

let polo_of_symbol = function
| "ETHXBT" -> "BTC_ETH"
| _ -> invalid_arg "polo_of_symbol"

let ws ~db symbol =
  let sym_polo = polo_of_symbol symbol in
  let store = DB.make_store () in
  let on_ws_msg = function
  | Wamp.Subscribed { reqid; id } ->
    let rec loop () =
      Monitor.try_with_or_error (fun () -> Rest.orderbook sym_polo) >>= function
      | Ok { asks; bids; seq } ->
        let evts = List.map (bids @ asks) ~f:(fun evt -> DB.BModify evt) in
        store db seq evts;
        info "stored %d %s partial" (List.length evts) symbol;
        Deferred.unit
      | Error err ->
        error "%s" (Error.to_string_hum err);
        Clock_ns.after @@ Time_ns.Span.of_int_sec 10 >>=
        loop
    in
    don't_wait_for @@ loop ();
    debug "subscribed %s" symbol
  | Event { Wamp.pubid; subid; details; args; kwArgs } ->
    let seq = match List.Assoc.find_exn kwArgs "seq" with Msgpck.Int i -> i | _ -> failwith "seq" in
    let map_f msg =
      let open Ws.Msgpck in
      match of_msgpck msg with
      | Error msg -> failwith msg
      | Ok { typ="newTrade"; data } ->
        let trade = trade_of_msgpck data in
        debug "%d T %s" seq @@ Fn.compose Sexp.to_string  DB.sexp_of_trade trade;
        DB.Trade trade
      | Ok { typ="orderBookModify"; data } ->
        let update = book_of_msgpck data in
        debug "%d M %s" seq @@ Fn.compose Sexp.to_string DB.sexp_of_book_entry update;
        DB.BModify update
      | Ok { typ="orderBookRemove"; data } ->
        let update = book_of_msgpck data in
        debug "%d D %s" seq @@ Fn.compose Sexp.to_string DB.sexp_of_book_entry update;
        DB.BRemove update
      | Ok { typ } -> failwithf "unexpected message type %s" typ ()
    in
    store db seq @@ List.map args ~f:map_f
  | msg -> error "unknown message: %s" (Wamp.sexp_of_msg Msgpck.sexp_of_t msg |> Sexplib.Sexp.to_string)
  in
  let ws = Ws.open_connection ~topics:[Uri.of_string sym_polo] () in
  Monitor.handle_errors
    (fun () -> Pipe.iter_without_pushback ~continue_on_error:true ws ~f:on_ws_msg)
    (fun exn -> error "%s" @@ Exn.to_string exn)

let record daemon datadir pidfile logfile loglevel symbol () =
  let db = LevelDB.open_db @@ Filename.concat datadir symbol in
  if daemon then Daemon.daemonize ~cd:"." ();
  Signal.handle Signal.terminating ~f:begin fun _ ->
    info "Data server stopping";
    LevelDB.close db;
    info "Saved db";
    don't_wait_for @@ Shutdown.exit 0
  end;
  don't_wait_for begin
    Lock_file.create_exn pidfile >>= fun () ->
    set_output Log.Output.[stderr (); file `Text ~filename:logfile];
    set_level (match loglevel with 2 -> `Info | 3 -> `Debug | _ -> `Error);
    ws db symbol
  end;
  never_returns @@ Scheduler.go ()

let record =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-daemon" no_arg ~doc:" Daemonize"
    +> flag "-datadir" (optional_with_default "data/poloniex.com" string) ~doc:"path Where to store DBs (data/poloniex.com)"
    +> flag "-pidfile" (optional_with_default "run/poloniex.pid" string) ~doc:"filename Path of the pid file (run/poloniex.pid)"
    +> flag "-logfile" (optional_with_default "log/poloniex.log" string) ~doc:"filename Path of the log file (log/poloniex.log)"
    +> flag "-loglevel" (optional_with_default 1 int) ~doc:"1-3 loglevel"
    +> anon ("symbol" %: string)
  in
  Command.basic ~summary:"Poloniex data aggregator" spec record

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
  let db = LevelDB.open_db @@ Filename.concat datadir symbol in
  Exn.protectx db ~finally:LevelDB.close ~f:iter_f

let show =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-datadir" (optional_with_default "data/poloniex.com" string) ~doc:"path Where to store DBs (data/poloniex.com)"
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
