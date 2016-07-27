open Core.Std
open Async.Std
open Log.Global

open Bs_devkit.Core
open Bs_api.PLNX

let (//) = Filename.concat

let polo_of_symbol = function
| "ETHXBT" -> "BTC_ETH"
| _ -> invalid_arg "polo_of_symbol"

let ws ~db symbol =
  let sym_polo = polo_of_symbol symbol in
  let store = DB.make_store db in
  let on_ws_msg = function
  | Wamp.Subscribed { reqid; id } ->
    let rec loop () =
      Monitor.try_with_or_error (fun () -> Rest.orderbook sym_polo) >>= function
      | Ok { asks; bids; seq } ->
        let evts = List.map (bids @ asks) ~f:(fun evt -> DB.BModify evt) in
        let buf = Bigstring.create @@ DB.bin_size_t evts in
        store ~buf seq evts;
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
    let seq = match List.Assoc.find_exn kwArgs "seq" with
    | `Int i -> i
    | #Yojson.Safe.json -> failwith "seq"
    in
    let map_f msg = match Ws.of_yojson msg with
    | Error msg -> failwith msg
    | Ok { typ="newTrade"; data } ->
      let trade = trade_raw_of_yojson data |> Result.ok_or_failwith |> trade_of_trade_raw in
      debug "%d T %s" seq @@ Fn.compose Sexp.to_string  DB.sexp_of_trade trade;
      DB.Trade trade
    | Ok { typ="orderBookModify"; data } ->
      let update = Ws.book_raw_of_yojson data |> Result.ok_or_failwith |> Ws.book_of_book_raw in
      debug "%d M %s" seq @@ Fn.compose Sexp.to_string DB.sexp_of_book_entry update;
      DB.BModify update
    | Ok { typ="orderBookRemove"; data } ->
      let update = Ws.book_raw_of_yojson data |> Result.ok_or_failwith |> Ws.book_of_book_raw in
      debug "%d D %s" seq @@ Fn.compose Sexp.to_string DB.sexp_of_book_entry update;
      DB.BRemove update
    | Ok { typ } -> failwithf "unexpected message type %s" typ ()
    in
    store seq @@ List.map args ~f:map_f
  | msg -> failwith (Fn.compose Yojson.Safe.to_string Wamp.msg_to_yojson msg)
  in
  let ws = Ws.open_connection ~topics:[Uri.of_string sym_polo] () in
  Monitor.handle_errors
    (fun () -> Pipe.iter_without_pushback ~continue_on_error:true ws ~f:on_ws_msg)
    (fun exn -> error "%s" @@ Exn.to_string exn)

let record daemon datadir pidfile logfile loglevel symbol () =
  let db = LevelDB.open_db @@ datadir // symbol in
  if daemon then Daemon.daemonize ~cd:"." ();
  Signal.(handle terminating ~f:(fun _ ->
      info "Data server stopping";
      LevelDB.close db;
      info "Saved db";
      don't_wait_for @@ Shutdown.exit 0)
    );
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
  let iter_f = if rev_iter then LevelDB.rev_iter else LevelDB.iter in
  let db = LevelDB.open_db @@ datadir // symbol in
  Exn.protectx
    ~finally:LevelDB.close
    ~f:(iter_f (fun seq data ->
        let seq = Binary_packing.unpack_signed_64_int_big_endian ~buf:seq ~pos:0 in
        let update = DB.bin_read_t ~pos_ref:(ref 0) @@ Bigstring.of_string data in
        begin if List.length update > 100 then
            Format.printf "%d (Partial)@." seq
          else
          Format.printf "%d %a@." seq Sexp.pp @@ DB.sexp_of_t update
        end;
        incr nb_read;
        Option.value_map max_ticks
          ~default:true ~f:(fun max_ticks -> not (!nb_read = max_ticks))
      ))
    db

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
  Command.group ~summary:"Manage order books logging"
    [
      "record", record;
      "show", show;
    ]

let () = Command.run command
