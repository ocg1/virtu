open Core
open Async
open Log.Global

open Bs_devkit
open Virtu_util

module Yojson_encoding = Json_encoding.Make(Json_repr.Yojson)

let log_sim = Log.(create ~level:`Error ~on_error:`Raise ~output:[Output.stderr ()])
let testnet = ref false
let cfg = ref @@ Order.create_cfg ()

let instruments : RespObj.t String.Table.t = String.Table.create ()
let instruments_initialized = Ivar.create ()
let ticksizes : ticksize String.Table.t = String.Table.create ()

let quoted_instruments : Int.t String.Table.t = String.Table.create ()

type quote = {
  timestamp: Time_ns.t;
  bid: (int * int) option;
  ask: (int * int) option;
}

let quotes : quote String.Table.t = String.Table.create ()

type trade = {
  symbol: string ;
  timestamp: Time_ns.t ;
  price: int ;
  qty: int ;
  resulting_pos: int ;
} [@@deriving sexp]

let positions : Int.t String.Table.t = String.Table.create ()
let margins : Int.t String.Table.t = String.Table.create ()

let on_instrument action data =
  let on_partial_insert i =
    let i = RespObj.of_json i in
    let sym = RespObj.string_exn i "symbol" in
    String.Table.set instruments sym i;
    String.Table.set ticksizes ~key:sym ~data:begin
      RespObj.float_exn i "tickSize" |> fun multiplier ->
      let mult_exponent, divisor = exponent_divisor_of_tickSize multiplier in
      { multiplier ; mult_exponent ; divisor }
    end
  in
  let on_update i =
    let i = RespObj.of_json i in
    let sym = RespObj.string_exn i "symbol" in
    let oldInstr = String.Table.find_exn instruments sym in
    String.Table.set instruments ~key:sym ~data:(RespObj.merge oldInstr i)
  in
  begin match action with
  | Bmex_ws.Response.Update.Partial
  | Insert ->
    List.iter data ~f:on_partial_insert;
    Ivar.fill_if_empty instruments_initialized ()
  | Update -> if Ivar.is_full instruments_initialized then List.iter data ~f:on_update
  | Delete -> error "instrument: got action Delete"
  end

let on_quote data =
  let iter_f q_json =
    let q = Yojson_encoding.destruct Bmex.Quote.encoding q_json in
    String.Table.set quotes q.symbol {
      timestamp = q.timestamp ;
      bid = (Option.map2 q.bidPrice q.bidSize ~f:(fun p s -> satoshis_int_of_float_exn p, s)) ;
      ask = (Option.map2 q.askPrice q.askSize ~f:(fun p s -> satoshis_int_of_float_exn p, s)) }
  in
  List.iter data ~f:iter_f

let on_trade datafile action data =
  let iter_f oc t_json =
    let t = Yojson_encoding.destruct Bmex.Trade.encoding t_json in
    let { divisor = tickSize } = String.Table.find_exn ticksizes t.symbol in
    let max_pos_size = String.Table.find_exn quoted_instruments t.symbol in
    let cur_pos = String.Table.find_exn positions t.symbol in
    let { bid; ask } = String.Table.find_exn quotes t.symbol in
    match t.side, bid, ask with
    | Some `Buy, _, Some (best_p, best_q) ->
      let qty = Int.min best_q @@ cur_pos + max_pos_size in
      let price = best_p - tickSize in
      let resulting_pos = cur_pos - qty in
      { symbol = t.symbol ;
        timestamp = t.timestamp ;
        price ;
        qty = (Int.neg qty) ;
        resulting_pos } |>
      sexp_of_trade |>
      Sexp.output_mach oc;
      String.Table.set positions t.symbol resulting_pos
    | Some `Sell, Some (best_p, best_q), _ ->
      let qty = Int.min best_q @@ max_pos_size - cur_pos in
      let price = best_p + tickSize in
      let resulting_pos = cur_pos + qty in
      { symbol = t.symbol ;
        timestamp = t.timestamp ;
        price ; qty ; resulting_pos } |>
      sexp_of_trade |>
      Sexp.output_mach oc;
      String.Table.set positions t.symbol resulting_pos
    | _ -> ()
  in
  match action with
  | Bmex_ws.Response.Update.Insert ->
    if Ivar.is_full instruments_initialized then
      Out_channel.with_file datafile ~binary:false ~append:true
        ~f:(fun oc -> List.iter data ~f:(iter_f oc))
  | _ -> ()

let simulate datafile buf instruments =
  let on_ws_msg msg_json =
    match Yojson_encoding.destruct Bmex_ws.Response.encoding msg_json with
    | Update { table; action; data } -> begin
      match table with
      | "instrument" -> on_instrument action data; Deferred.unit
      | "quote" -> on_quote data; Deferred.unit
      | "trade" -> on_trade datafile action data; Deferred.unit
      | _ -> error "Invalid table %s" table; Deferred.unit
    end
    | _ -> Deferred.unit
  in
  let topics = List.(map instruments ~f:begin fun i ->
      ["instrument:" ^ i; "quote:" ^ i; "trade:" ^ i]
    end |> concat) in
  let ws = Bmex_ws.open_connection ~md:false ~testnet:!testnet ~topics () in
  Monitor.handle_errors
    (fun () -> Pipe.iter ~continue_on_error:true ws ~f:on_ws_msg)
    (fun exn -> error "%s" @@ Exn.to_string exn)

let main cfg daemon rundir logdir loglevel test instruments () =
  let executable_name = Sys.executable_name |> Filename.basename |> String.split ~on:'.' |> List.hd_exn in
  let pidfile = Filename.concat rundir @@ executable_name ^ ".pid" in
  let logfile = Filename.concat logdir @@ executable_name ^ ".log" in
  let datafile = Filename.concat logdir @@ executable_name ^ ".data" in
  if daemon then Daemon.daemonize ~cd:"." ();
  stage begin fun `Scheduler_started ->
    Lock_file.create_exn pidfile >>= fun () ->
    testnet := test;
    let cfg = begin match Sexplib.Sexp.load_sexp_conv cfg Cfg.t_of_sexp with
    | `Error (exn, _) -> raise exn
    | `Result cfg -> cfg
    end in
    let { Cfg.quote } =
      List.Assoc.find_exn ~equal:String.(=) cfg (if test then "BMEXT" else "BMEX") in
    let instruments = if instruments = [] then quote else instruments in
    let buf = Bi_outbuf.create 4096 in
    let log_outputs filename = Log.Output.[stderr (); file `Text ~filename] in
    set_output @@ log_outputs logfile;
    set_level @@ loglevel_of_int loglevel;
    Log.set_output log_sim @@ log_outputs datafile;
    List.iter instruments ~f:begin fun (symbol, max_pos_size) ->
      String.Table.set quoted_instruments symbol max_pos_size;
      String.Table.set positions symbol 0;
    end ;
    info "Sim starting";
    Monitor.try_with_or_error begin fun () ->
      let data_pipe = Log.Reader.pipe `Sexp datafile in
      Pipe.iter_without_pushback data_pipe ~f:Log.Message.(fun msg ->
          let msg = message msg in
          let s = Sexp.of_string msg in
          let { symbol; resulting_pos } = trade_of_sexp s in
          String.Table.set positions symbol resulting_pos
        )
    end >>= function
    | Ok _ ->
      info "Done reading %s" datafile;
      simulate datafile buf @@ String.Table.keys quoted_instruments
    | Error _ ->
      simulate datafile buf @@ String.Table.keys quoted_instruments;
  end

let command =
  let default_cfg = Filename.concat (Option.value_exn (Sys.getenv "HOME")) ".virtu" in
  let spec =
    let open Command.Spec in
    empty
    +> flag "-cfg" (optional_with_default default_cfg string) ~doc:"path Filepath of config file (default: ~/.virtu)"
    +> flag "-daemon" no_arg ~doc:" Run as a daemon"
    +> flag "-rundir" (optional_with_default "run" string) ~doc:"filename Path of the run directory (default: run)"
    +> flag "-logdir" (optional_with_default "log" string) ~doc:"filename Path of the log directory (default: log)"
    +> flag "-loglevel" (optional_with_default 1 int) ~doc:"1-3 loglevel"
    +> flag "-test" no_arg ~doc:" Use testnet"
    +> anon (sequence (t2 ("instrument" %: string) ("quote" %: int)))
  in
  Command.Staged.async ~summary:"Market maker simulator" spec main

let () = Command.run command
