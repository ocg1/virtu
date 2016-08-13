open Core.Std
open Async.Std
open Log.Global

open Dtc
open Bs_devkit.Core

module BMEX = Bs_api.BMEX

let update_of_l2 { BMEX.OrderBook.L2.id; side; size; price } =
  let side = BMEX.side_of_bmex side |> Result.ok_or_failwith in
  let price = Option.value_map price ~default:0 ~f:satoshis_int_of_float_exn in
  let qty = Option.value ~default:0 size in
  DB.create_book_entry ~side ~price ~qty ()

let trade_of_bmex { BMEX.Trade.symbol; timestamp; side; price; size } =
  let side = BMEX.side_of_bmex side |> Result.ok_or_failwith in
  let price = satoshis_int_of_float_exn price in
  let qty = Int64.to_int_exn size in
  let ts = Time_ns.of_string timestamp in
  DB.create_trade ~ts ~side ~price ~qty ()

let evt_of_update_action up = function
| OB.Partial | Insert | Update -> DB.BModify up
| Delete -> BRemove up

let bmex_dbs = String.Table.create ()

let make_on_evt () =
  let store = DB.make_store () in
  let open Bs_api.BMEX in
  fun kind action data ->
    let now = Time_ns.(now () |> Time_ns.to_int_ns_since_epoch) in
    match kind with
    | `L2 ->
      (* debug "<- %s" (Yojson.Safe.to_string (`List data)); *)
      let data = List.map data ~f:(Fn.compose Result.ok_or_failwith OrderBook.L2.of_yojson) in
      let groups = List.group data ~break:(fun u u' -> u.symbol <> u'.symbol) in
      let iter_f = function
      | [] -> ()
      | { OrderBook.L2.symbol } :: t as ups ->
        let db = String.Table.find_exn bmex_dbs symbol in
        let data = List.map ups ~f:(fun u -> update_of_l2 u |> fun u -> evt_of_update_action u action) in
        debug "%s" (DB.sexp_of_t_list data |> Sexp.to_string);
        store db now data
      in
      List.iter groups ~f:iter_f
    | `Trade ->
      let data = List.map data ~f:(Fn.compose Result.ok_or_failwith Trade.of_yojson) in
      let iter_f t =
        let db = String.Table.find_exn bmex_dbs t.Trade.symbol in
        let data = List.map data ~f:(fun t -> DB.Trade (trade_of_bmex t)) in
        store db now data
      in
      List.iter data ~f:iter_f

let log_bmex datadir symbols =
  let open Bs_api.BMEX in
  let topics = List.(map symbols ~f:(fun s -> ["orderBookL2:" ^ s; "trade:" ^ s]) |> concat) in
  let on_evt = make_on_evt () in
  let ws = Ws.open_connection ~md:false ~testnet:false ~topics () in
  let on_ws_msg msg_json =
    match Ws.update_of_yojson msg_json with
    | Error _ -> begin
        match Ws.response_of_yojson msg_json with
        | Error _ -> error "%s" (Yojson.Safe.to_string msg_json)
        | Ok response -> info "%s" @@ Ws.show_response response
      end
    | Ok { table; action; data } ->
      let action = update_action_of_string action in
      match table with
      | "orderBookL2" -> on_evt `L2 action data
      | "trade" -> on_evt `Trade action data
      | _ -> error "Invalid table %s" table
  in
  Monitor.handle_errors
    (fun () -> Pipe.iter_without_pushback ~continue_on_error:true ws ~f:on_ws_msg)
    (fun exn -> error "%s" @@ Exn.to_string exn)

let record daemon rundir logdir datadir loglevel instruments () =
  let instruments = List.fold_left instruments ~init:String.Map.empty ~f:begin fun a (exchange, symbol) ->
      String.Map.add_multi a exchange symbol
    end
  in
  let executable_name = Sys.executable_name |> Filename.basename |> String.split ~on:'.' |> List.hd_exn in
  let pidfile = Filename.concat rundir @@ executable_name ^ ".pid" in
  let logfile = Filename.concat logdir @@ executable_name ^ ".log" in
  if daemon then Daemon.daemonize ~cd:"." ();
  Signal.handle Signal.terminating ~f:begin fun _ ->
    info "OB logger stopping.";
    String.Table.iter bmex_dbs ~f:LevelDB.close;
    info "Saved %d dbs." @@ String.Table.length bmex_dbs;
    don't_wait_for @@ Shutdown.exit 0
    end;
  don't_wait_for begin
    Lock_file.create_exn pidfile >>= fun () ->
    let log_outputs filename = Log.Output.[stderr (); file `Text ~filename] in
    set_output @@ log_outputs logfile;
    set_level @@ loglevel_of_int loglevel;
    info "logobs starting";
    Option.iter (String.Map.find instruments "BMEX") ~f:begin fun syms ->
      List.iter syms ~f:begin fun s ->
        let (//) = Filename.concat in
        let path = datadir // "bitmex.com" // s in
        String.Table.set bmex_dbs s (LevelDB.open_db path)
      end;
      don't_wait_for @@ log_bmex datadir syms
    end;
    Deferred.never ()
  end;
  never_returns @@ Scheduler.go ()

let record =
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
  Command.basic ~summary:"Log exchanges order books" spec record

let show datadir rev_iter max_ticks symbol () =
  let open Core.Std in
  let nb_read = ref 0 in
  let buf = Bigbuffer.create 4096 in
  let iter_f ts data =
    Bigbuffer.clear buf;
    Bigbuffer.add_string buf data;
    let ts = Time_ns.of_int_ns_since_epoch @@
      Binary_packing.unpack_signed_64_int_big_endian ~buf:ts ~pos:0
    in
    let update = DB.bin_read_t_list ~pos_ref:(ref 0) @@ Bigbuffer.big_contents buf in
    Format.printf "%d %s %a@." !nb_read (Time_ns.to_string ts) Sexp.pp @@ DB.sexp_of_t_list update;
    incr nb_read;
    Option.value_map max_ticks
      ~default:true ~f:(fun max_ticks -> not (!nb_read = max_ticks))
  in
  let iter_f = (if rev_iter then LevelDB.rev_iter else LevelDB.iter) iter_f in
  let dbpath = Filename.concat datadir symbol in
  (match Sys.is_directory dbpath with
  | `No | `Unknown -> invalid_argf "No DB for %s" symbol ()
  | `Yes -> ());
  let db = LevelDB.open_db dbpath in
  Exn.protectx db ~finally:LevelDB.close ~f:iter_f

let show =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-datadir" (optional_with_default "data/bitmex.com" string) ~doc:"path Where to store DBs (data/bitmex.com)"
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

