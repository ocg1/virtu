open Core.Std
open Async.Std
open Log.Global

open Bs_devkit.Core

let update_of_l2 { Bs_api.BMEX.OrderBook.L2.id; side; size; price } =
  let side = match side with "Buy" -> Side.Bid | "Sell" -> Ask | _ -> invalid_arg "side_of_string" in
  let price = Option.map price ~f:satoshis_int_of_float_exn in
  OB.create_update ~id ~side ?price ?size ()

let bmex_dbs = String.Table.create ()

let make_on_orderbook () =
  let key = String.create 8 in
  let value = Bigstring.create 1024 in
  let open Bs_api.BMEX in
  fun action data ->
    (* debug "<- %s" (Yojson.Safe.to_string (`List data)); *)
    let now = Time_ns.now () in
    let data = List.map data ~f:(Fn.compose Result.ok_or_failwith OrderBook.L2.of_yojson) in
    let data = List.group data ~break:(fun u u' -> u.OrderBook.L2.symbol <> u'.symbol) in
    let iter_f start_ts i u =
      Binary_packing.pack_signed_64_int_big_endian ~buf:key ~pos:0 @@ start_ts + i;
      let update = OB.create ~action ~data:(update_of_l2 u) () in
      debug "%s" (OB.sexp_of_t update |> Sexp.to_string);
      let endp = OB.bin_write_t value ~pos:0 update in
      let data = Bigstring.To_string.sub value 0 endp in
      let db = String.Table.find_exn bmex_dbs u.symbol in
      LevelDB.put db key data
    in
    List.iter data ~f:(fun g ->
        let now_sec = Time_ns.to_int_ns_since_epoch now in
        List.iteri g (iter_f now_sec)
      )

let log_bmex datadir symbols =
  let open Bs_api.BMEX in
  let topics = List.map symbols ~f:(fun s -> "orderBookL2:" ^ s) in
  let on_orderbook = make_on_orderbook () in
  let ws = Ws.open_connection ~testnet:false ~topics () in
  let buf = Bi_outbuf.create 4096 in
  let on_ws_msg msg_str =
    let msg_json = Yojson.Safe.from_string ~buf msg_str in
    match Ws.update_of_yojson msg_json with
    | Error _ -> begin
        match Ws.response_of_yojson msg_json with
        | Error _ ->
          error "%s" msg_str
        | Ok response ->
          info "%s" @@ Ws.show_response response
      end
    | Ok { table; action; data } ->
      let action = update_action_of_string action in
      match table with
      | "orderBookL2" -> on_orderbook action data
      | _ -> error "Invalid table %s" table
  in
  Monitor.handle_errors
    (fun () -> Pipe.iter_without_pushback ~continue_on_error:true ws ~f:on_ws_msg)
    (fun exn -> error "%s" @@ Exn.to_string exn)

let log_bfx datadir symbols =
  Deferred.never ()

let logobs daemon rundir logdir datadir loglevel instruments () =
  let instruments = List.fold_left instruments ~init:String.Map.empty ~f:(fun a (exchange, symbol) ->
      String.Map.add_multi a exchange symbol
    )
  in
  let executable_name = Sys.executable_name |> Filename.basename |> String.split ~on:'.' |> List.hd_exn in
  let pidfile = Filename.concat rundir @@ executable_name ^ ".pid" in
  let logfile = Filename.concat logdir @@ executable_name ^ ".log" in
  if daemon then Daemon.daemonize ~cd:"." ();
  Signal.(handle terminating ~f:(fun _ ->
      info "OB logger stopping.";
      String.Table.iter bmex_dbs ~f:LevelDB.close;
      info "Saved %d dbs." @@ String.Table.length bmex_dbs;
      don't_wait_for @@ Shutdown.exit 0)
    );
  don't_wait_for begin
    Lock_file.create_exn pidfile >>= fun () ->
    let log_outputs filename = Log.Output.[stderr (); file `Text ~filename] in
    set_output @@ log_outputs logfile;
    set_level @@ loglevel_of_int loglevel;
    info "logobs starting";
    Option.iter (String.Map.find instruments "BMEX") ~f:(fun syms ->
        List.iter syms ~f:(fun s ->
            let (//) = Filename.concat in
            let path = datadir // "BMEX" // s in
            String.Table.set bmex_dbs s (LevelDB.open_db path)
          );
        don't_wait_for @@ log_bmex datadir syms
      );
    Option.iter (String.Map.find instruments "BFX") ~f:(fun syms ->
        don't_wait_for @@ log_bfx datadir syms
      );
    Deferred.never ()
  end;
  never_returns @@ Scheduler.go ()

let logobs =
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
  Command.basic ~summary:"Log exchanges order books" spec logobs

let show tail max_ticks dbpath () =
  let open Core.Std in
  let nb_read = ref 0 in
  let iter_f = if tail then LevelDB.rev_iter else LevelDB.iter in
  let db = LevelDB.open_db dbpath in
  Exn.protectx
    ~finally:LevelDB.close
    ~f:(iter_f (fun ts data ->
        let ts = Time_ns.of_int_ns_since_epoch @@
          Binary_packing.unpack_signed_64_int_big_endian ~buf:ts ~pos:0
        in
        let update = OB.bin_read_t ~pos_ref:(ref 0) @@ Bigstring.of_string data in
        Format.printf "%d %s %a@." !nb_read (Time_ns.to_string ts) Sexp.pp @@ OB.sexp_of_t update;
        incr nb_read;
        Option.value_map max_ticks
          ~default:true ~f:(fun max_ticks -> not (!nb_read = max_ticks))
      ))
    db

let show =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-tail" no_arg ~doc:" Show latest records"
    +> flag "-n" (optional int) ~doc:"n Number of ticks to display (default: all)"
    +> anon ("db" %: string)
  in
  Command.basic ~summary:"Show LevelDB order book databases" spec show

let command =
  Command.group ~summary:"Manage order books logging"
    ["show", show;
     "log", logobs
    ]

let () = Command.run command

