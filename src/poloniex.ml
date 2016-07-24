open Core.Std
open Async.Std
open Log.Global

open Bs_devkit.Core
open Bs_api.PLNX

let (//) = Filename.concat

let polo_of_symbol = function
  | "ETHXBT" -> "BTC_ETH"
  | _ -> invalid_arg "polo_of_symbol"

module DB = struct
  type event =
    | BModify of book_entry
    | BRemove of book_entry
    | Trade of trade [@@deriving sexp, bin_io]

  type t = event list [@@deriving sexp, bin_io]

  let make_store ?sync ?buf db =
    let key = String.create 8 in
    let value = Option.value buf ~default:(Bigstring.create 4096) in
    fun ?(buf=value) seq evts ->
      Binary_packing.pack_signed_64_int_big_endian ~buf:key ~pos:0 seq;
      let endp = bin_write_t buf ~pos:0 evts in
      let value = Bigstring.To_string.sub buf 0 endp in
      LevelDB.put ?sync db key value
end

let ws ~db symbol =
  let store = DB.make_store db in
  let on_ws_msg = function
  | Wamp.Subscribed { reqid; id } ->
    let rec loop () =
      Monitor.try_with_or_error (fun () -> Rest.orderbook (polo_of_symbol symbol)) >>= function
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
    debug "%d %s" seq @@ Yojson.Safe.to_string (`List args);
    let map_f msg = match Ws.of_yojson msg with
    | Error msg -> failwith msg
    | Ok { typ="newTrade"; data } -> begin
        match trade_raw_of_yojson data with
        | Error msg -> invalid_arg msg
        | Ok trade_raw ->
          let trade = trade_of_trade_raw trade_raw in
          debug "%s" @@ Fn.compose Sexp.to_string  sexp_of_trade trade;
          DB.Trade trade
      end
    | Ok { typ="orderBookModify"; data } ->
      let update = Ws.book_raw_of_yojson data |> Result.ok_or_failwith |> Ws.book_of_book_raw in
      debug "%s" @@ Fn.compose Sexp.to_string sexp_of_book_entry update;
      DB.BModify update
    | Ok { typ="orderBookRemove"; data } ->
      let update = Ws.book_raw_of_yojson data |> Result.ok_or_failwith |> Ws.book_of_book_raw in
      debug "%s" @@ Fn.compose Sexp.to_string sexp_of_book_entry update;
      DB.BRemove update
    | Ok { typ } -> failwithf "unexpected message type %s" typ ()
    in
    store seq @@ List.map args ~f:map_f
  | msg -> failwith (Fn.compose Yojson.Safe.to_string Wamp.msg_to_yojson msg)
  in
  let ws = Ws.open_connection ~log:Lazy.(force log) ~topics:[Uri.of_string symbol] () in
  Monitor.handle_errors
    (fun () -> Pipe.iter_without_pushback ~continue_on_error:true ws ~f:on_ws_msg)
    (fun exn -> error "%s" @@ Exn.to_string exn)

let main daemon datadir pidfile logfile loglevel symbol () =
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

let command =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-daemon" no_arg ~doc:" Daemonize"
    +> flag "-datadir" (optional_with_default "data/poloniex.com" string) ~doc:"path Where to store DBs (data/poloniex.com)"
    +> flag "-pidfile" (optional_with_default "run/poloniex_data.pid" string) ~doc:"filename Path of the pid file (run/bitmex_data.pid)"
    +> flag "-logfile" (optional_with_default "log/poloniex_data.log" string) ~doc:"filename Path of the log file (log/bitmex_data.log)"
    +> flag "-loglevel" (optional_with_default 1 int) ~doc:"1-3 loglevel"
    +> anon ("symbol" %: string)
  in
  Command.basic ~summary:"Poloniex data aggregator" spec main

let () = Command.run command
