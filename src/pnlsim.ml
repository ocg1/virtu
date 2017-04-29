open Core
open Async
open Log.Global

open Bs_devkit

type trade = { price: int; qty: int; side: [`Buy | `Sell] } [@@deriving sexp]

type evt =
  | Trade of trade
  | OB of OB.t
[@@deriving sexp]

type msg = {
  ts: Time_ns.t;
  data: evt;
}

module LDB = struct
  let trades ?(max_count=Int.max_value) ?(low=Time_ns.min_value) ?(high=Time_ns.max_value) db =
    let open LevelDB in
    let i = Iterator.make db in
    Iterator.seek_to_first i;
    let count = ref 0 in
    let rec create_f w =
      if not @@ Iterator.valid i || !count >= max_count then raise Exit;
      let ts = Iterator.get_key i in
      let ts = Time_ns.of_int_ns_since_epoch @@ Binary_packing.unpack_signed_64_int_big_endian ~buf:ts ~pos:0 in
      if not @@ Time_ns.between ts ~low ~high then raise Exit;
      let data = Iterator.get_value i in
      let { Tick.p; v; side } = Tick.Bytes.read' ~ts ~data () in
      let price = Int63.to_int_exn p in
      let qty = Int63.to_int_exn v in
      let side = Option.value_exn side in
      incr count;
      Iterator.next i;
      Pipe.write w @@ { ts ; data = (Trade { price; qty; side }) } >>= fun () ->
      create_f w
    in
    Pipe.create_reader ~close_on_exception:true create_f

  let book ?(max_count=Int.max_value) ?(low=Time_ns.min_value) ?(high=Time_ns.max_value) db =
    let open LevelDB in
    let i = Iterator.make db in
    Iterator.seek_to_first i;
    let count = ref 0 in
    let rec create_f w =
      if not @@ Iterator.valid i || !count >= max_count then raise Exit;
      let ts = Iterator.get_key i in
      let ts = Time_ns.of_int_ns_since_epoch @@ Binary_packing.unpack_signed_64_int_big_endian ~buf:ts ~pos:0 in
      if not @@ Time_ns.between ts ~low ~high then raise Exit;
      let data = Iterator.get_value i in
      let update = OB.bin_read_t ~pos_ref:(ref 0) @@ Bigstring.of_string data in
      incr count;
      Iterator.next i;
      Pipe.write w @@ { ts ; data = (OB update) } >>= fun () ->
      create_f w
    in
    Pipe.create_reader ~close_on_exception:true create_f

  let interleave ?max_count ?low ?high tdb bdb =
    let tradesp = trades ?max_count ?low ?high tdb in
    let bookp = book ?max_count ?low ?high bdb in
    let cmp { ts=ts1; _ } { ts=ts2; _ } = Time_ns.compare ts1 ts2 in
    Pipe.merge [tradesp; bookp] ~cmp
end

let main gnuplot max_count low high (db_ob, db_trades, ticksize, size) () =
  let low = Option.map low ~f:Time_ns.of_string in
  let high = Option.map high ~f:Time_ns.of_string in
  let count = ref 0 in
  let position = ref 0 in
  let balance = ref 0 in
  let pnl = ref 0 in
  let ts_int_at_start = ref 0 in
  let obs = Int.Table.create () in
  let bids = ref Int.Map.empty in
  let asks = ref Int.Map.empty in
  let last_partial = ref false in
  let iter_f { ts; data } = match data with
  | OB ({ action=Partial; data={ id; side; price; size }} as ob)
  | OB ({ action=Insert; data={ id; side; price; size }} as ob) ->
    if not gnuplot then Format.printf "%a %a@." Time_ns.pp ts Sexp.pp (OB.sexp_of_t ob);
    let table = match side with `Buy -> bids | `Sell -> asks in
    if ob.action = Partial && !last_partial = false then begin
      Int.Table.clear obs;
      table := Int.Map.empty;
      last_partial := true;
    end;
    Int.Table.set obs id (price, size);
    table := Int.Map.add_multi !table price (id, size);
    if ob.action = Insert then last_partial := false
  | OB ({ action=Delete; data={ id; side; }} as ob) ->
    if not gnuplot then Format.printf "%a %a@." Time_ns.pp ts Sexp.pp (OB.sexp_of_t ob);
    (* FIXME: Should never be deleted 2 times! *)
    (* Option.iter (Int.Table.find obs id) ~f:begin fun (oldp, _) -> *)
    let oldp, _ = Int.Table.find_exn obs id in
      Int.Table.remove obs id;
      let table = match side with `Buy -> bids | `Sell -> asks in
      table := Int.Map.update !table oldp ~f:(function
        | None -> failwith "Inconsistent orderbook"
        | Some bs -> List.Assoc.remove ~equal:Int.(=) bs id
        );
    (* end; *)
    last_partial := false;
    | OB ({ action=Update; data={ id; side; price; size }} as ob) ->
    if not gnuplot then Format.printf "%a %a@." Time_ns.pp ts Sexp.pp (OB.sexp_of_t ob);
    let oldp, _ = Int.Table.find_exn obs id in
    Int.Table.set obs id (price, size);
    let table = match side with `Buy -> bids | `Sell -> asks in
    let intermediate = Int.Map.update !table oldp ~f:(function
      | None -> failwith "Inconsistent orderbook"
      | Some bs -> List.Assoc.remove ~equal:Int.(=) bs id
      )
    in
    table := Int.Map.add_multi intermediate price (id, size);
    last_partial := false
  | Trade { price; qty; side } ->
    if !count = 0 then ts_int_at_start := Time_ns.to_int_ns_since_epoch ts;
    let old_position = !position in
    let old_balance = !balance in
    let qty = match side with `Buy -> Int.neg qty | `Sell -> qty in
    let max_buy = size - old_position in
    let max_sell = - size - old_position in
    let trade_qty = match Int.sign qty with
    | Zero -> 0
    | Pos -> Int.min max_buy qty
    | Neg -> Int.max max_sell qty
    in

    position := old_position + trade_qty;
    balance := old_balance - trade_qty * price;

    let price = price / ticksize in
    let balance = !balance / ticksize in
    let position_value = !position * price in
    let cur_pnl = balance + position_value in
    let pnl_diff = 100. *. ((Float.of_int cur_pnl /. Float.of_int !pnl) -. 1.) in
    pnl := cur_pnl;
    if gnuplot then
      Format.printf "%d %d@." Time_ns.((to_int_ns_since_epoch ts - !ts_int_at_start) / 1_000_000) cur_pnl
    else
    Format.printf "%a P %d Q %d Pos %d B %d PV %d P/L %d %.0f%%\n"
      Time_ns.pp ts price trade_qty !position balance position_value cur_pnl pnl_diff;
    incr count
  in
  Monitor.try_with ~extract_exn:true
    (fun () ->
       let db_trades = LevelDB.open_db db_trades in
       let db_ob = LevelDB.open_db db_ob in
       let evts = LDB.interleave ?max_count ?low ?high db_trades db_ob in
       Pipe.iter_without_pushback evts ~f:iter_f
    ) >>= function
  | Ok _ -> assert false
  | Error Exit -> Shutdown.exit 0
  | Error exn ->
    error "%s" (Exn.to_string exn);
    Shutdown.exit 0

let command =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-gnuplot" no_arg ~doc:" output for gnuplot"
    +> flag "-max-count" (optional int) ~doc:"int max record size"
    +> flag "-low" (optional string) ~doc:"timestamp Low timestamp"
    +> flag "-high" (optional string) ~doc:"timestamp High timestamp"
    +> anon (t4 ("ob" %: string) ("trades" %: string) ("tick_size" %: int) ("max_pos_size" %: int))
  in
  Command.async ~summary:"P/L simulator" spec main

let () = Command.run command
