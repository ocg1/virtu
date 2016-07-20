open Core.Std
open Dtc

let main gnuplot max_count low high (dbpath, ticksize, size) () =
  let low = Option.value_map low ~default:Time_ns.epoch ~f:Time_ns.of_string in
  let high = Option.value_map high ~default:Time_ns.max_value ~f:Time_ns.of_string in
  let db = LevelDB.open_db dbpath in
  let count = ref 0 in
  let position = ref 0 in
  let balance = ref 0 in
  let pnl = ref 0 in
  let ts_int_at_start = ref 0 in
  let iter_f ts data =
    let ts = Time_ns.of_int_ns_since_epoch @@
      Binary_packing.unpack_signed_64_int_big_endian ~buf:ts ~pos:0
    in
    if !count = 0 then ts_int_at_start := Time_ns.to_int_ns_since_epoch ts;
    let { Tick.p; v; side } = Tick.Bytes.read' ~ts ~data () in
    let old_position = !position in
    let old_balance = !balance in
    let price = Int63.to_int_exn p in
    let abs_qty = Int63.to_int_exn v in
    let qty = match side with `Buy -> Int.neg abs_qty | `Sell -> abs_qty | `Unset -> invalid_arg "qty_of_side" in
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
    incr count;
    Time_ns.between ts ~low ~high &&
    Option.value_map max_count ~default:true ~f:(fun max_count -> !count < max_count)
  in
  Exn.protectx ~finally:LevelDB.close ~f:(LevelDB.iter iter_f) db

let command =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-gnuplot" no_arg ~doc:" output for gnuplot"
    +> flag "-max-count" (optional int) ~doc:"int max record size"
    +> flag "-low" (optional string) ~doc:"timestamp Low timestamp"
    +> flag "-high" (optional string) ~doc:"timestamp High timestamp"
    +> anon (t3 ("db" %: string) ("tick_size" %: int) ("max_pos_size" %: int))
  in
  Command.basic ~summary:"P/L simulator" spec main

let () = Command.run command
