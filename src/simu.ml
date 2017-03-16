open Core

open Dtc
open Bs_devkit

let ticksizes =
  ["ETHXBT", 1]

let make_fill_prob a k = fun d -> a *. exp (-. k *. d)

module LevelDB_ext = struct
  open LevelDB

  let update_books bids asks evts =
    let update_f (bids, asks) = function
    | DB.Trade _ -> (bids, asks)
    | BModify { side; price; qty } -> begin
      match side with
      | `Buy -> Int.Map.add bids price qty, asks
      | `Sell -> bids, Int.Map.add asks price qty
      end
    | BRemove { side; price } -> begin
      match side with
      | `Buy -> Int.Map.remove bids price, asks
      | `Sell -> bids, Int.Map.remove asks price
      end
    in
    List.fold evts ~init:(bids, asks) ~f:update_f

  let fold ?(low=Int.min_value) ?(high=Int.max_value) ?(size=Int.max_value) ~init db ~f =
    let buf = Bigstring.create 4096 in
    let bids = ref Int.Map.empty in
    let asks = ref Int.Map.empty in
    let state = ref init in
    let initialized = ref false in
    let nb_processed = ref 0 in
    let iter_f key value =
      let key = Binary_packing.unpack_signed_64_int_big_endian ~buf:key ~pos:0 in
      let len = String.length value in
      Bigstring.From_string.blit ~src:value ~src_pos:0 ~dst:buf ~dst_pos:0 ~len;
      let evts_stats evts =
        let nb_mods = List.count evts ~f:(function DB.BModify _ -> true | _ -> false) in
        let nb_dels = List.count evts ~f:(function DB.BRemove _ -> true | _ -> false) in
        let nb_trades = List.count evts ~f:(function DB.Trade _ -> true | _ -> false) in
        nb_mods, nb_dels, nb_trades
      in
      let evts = DB.bin_read_t_list ~pos_ref:(ref 0) buf in
      let evts_len = List.length evts in
      let ups, dels, trades = evts_stats evts in
      let partial = evts_len > 50 && trades = 0 in
      let bids', asks' =
        if partial then begin
          initialized := true;
          update_books Int.Map.empty Int.Map.empty evts
        end else
        update_books !bids !asks evts
      in
      bids := bids';
      asks := asks';
      (* Format.printf "%d B %d A %d — U %d D %d T %d@." key (Int.Map.length bids') (Int.Map.length asks') ups dels trades; *)
      let state' =
        if not !initialized then !state
        else if Int.Map.is_empty bids' || Int.Map.is_empty asks' then failwith "empty orderbook"
        else f !state bids' asks' key (if partial then [] else evts)
      in
      state := state';
      incr nb_processed;
      Int.between key ~low ~high && !nb_processed <= size
    in
    LevelDB.iter iter_f db

  let with_db ~f = Exn.protectx ~finally:LevelDB.close ~f
end

type state = {
  bids: int Int.Map.t [@default Int.Map.empty];
  asks: int Int.Map.t [@default Int.Map.empty];
  mid_price: int [@default 0];
  balance: int [@default 0];
  nb_contracts: int [@default 0];
} [@@deriving sexp]

let create_state
    ?(bids = Int.Map.empty)
    ?(asks = Int.Map.empty)
    ?(mid_price = 0)
    ?(balance = 0)
    ?(nb_contracts = 0) () =
  { bids ; asks ; mid_price ; balance ; nb_contracts }

let best_bid = Int.Map.max_elt_exn
let best_ask = Int.Map.min_elt_exn

let pnl { balance; nb_contracts; mid_price; } =
  let contracts_value = mid_price * nb_contracts / 100_000_000 in
  balance + contracts_value

let main gnuplot datadir low high size (symbol, max_pos_size) () =
  let max_pos_size = max_pos_size * 100_000_000 in
  let ticksize = List.Assoc.find_exn ~equal:String.(=) ticksizes symbol in
  let fill_prob = make_fill_prob 0.9 (log 0.9 /. log 0.5) in
  let init_seq = ref 0 in
  let blanket old_state bids asks seq evts =
    if !init_seq = 0 then init_seq := seq;
    let bb_price, bb_qty = best_bid bids in
    let ba_price, ba_qty = best_ask asks in
    let mid_price = (ba_price + bb_price) / 2 in
    let old_pnl = pnl old_state in
    let b_distance = mid_price - bb_price in
    let a_distance = ba_price - mid_price in
    let mybid_qty = Int.abs @@ max_pos_size - old_state.nb_contracts in
    let myask_qty = Int.abs @@ old_state.nb_contracts + max_pos_size in
    let mybid_price, myask_price =
      if bb_price + ticksize = ba_price then
        bb_price, ba_price
      else
      bb_price + ticksize, ba_price - ticksize
    in
    let my_b_distance = mid_price - mybid_price in
    let my_a_distance = myask_price - mid_price in
    let mybids = Int.Map.(add empty mybid_price mybid_qty) in
    let myasks = Int.Map.(add empty myask_price myask_qty) in
    let state = { old_state with bids=mybids; asks=myasks; mid_price } in
    let fold_evts state = function
    | DB.BModify _ | BRemove _ -> state
    | Trade ({ ts; side = `Buy; price; qty } as trade) ->
      if Random.float 1. > fill_prob (my_a_distance // a_distance)
      || state.nb_contracts = - max_pos_size
      then begin
        if not gnuplot then Format.printf "%d MISS %a %a@."
            seq Sexp.pp (DB.sexp_of_trade trade) Sexp.pp (sexp_of_state state);
        state
      end
      else
      let myask_p, myask_qty = best_ask state.asks in
      let sell_qty = Int.min qty myask_qty in
      let new_balance = state.balance + myask_p * sell_qty / 100_000_000 in
      let new_nb_contracts = state.nb_contracts - sell_qty in
      let mybid_qty = mybid_qty + sell_qty in
      let myask_qty = myask_qty - sell_qty in
      let mybids = Int.Map.(add empty mybid_price mybid_qty) in
      let myasks = Int.Map.(add empty myask_price myask_qty) in
      let new_state = { state with bids=mybids; asks=myasks; balance=new_balance; nb_contracts=new_nb_contracts } in
      let new_pnl = pnl new_state in
      let pnlchg = new_pnl // old_pnl in
      let pnldiff = new_pnl - old_pnl in
      if pnlchg > 10. || pnlchg < 0.1 then
        Format.eprintf "%a@.%a@.%d %d %dP %f%%@."
          Sexp.pp (sexp_of_state state)
          Sexp.pp (sexp_of_state new_state)
          old_pnl new_pnl pnldiff (pnlchg *. 100.);
      if gnuplot then Format.printf "%d %d %d %d %f@." (seq - !init_seq) mid_price (new_pnl / Int.pow 10 5) pnldiff pnlchg
      else Format.printf "%d %a %a@." seq Sexp.pp (DB.sexp_of_trade trade) Sexp.pp (sexp_of_state new_state);
      new_state

    | Trade ({ ts; side = `Sell; price; qty } as trade) ->
      if Random.float 1. > fill_prob (my_b_distance // b_distance)
      || state.nb_contracts = max_pos_size
      then begin
        if not gnuplot then Format.printf "%d MISS %a %a@." seq Sexp.pp (DB.sexp_of_trade trade) Sexp.pp (sexp_of_state state);
        state
      end
      else
      let mybid_p, mybid_qty = best_bid state.bids in
      let buy_qty = Int.min qty mybid_qty in
      let new_balance = state.balance - mybid_p * buy_qty / 100_000_000 in
      let new_nb_contracts = state.nb_contracts + buy_qty in
      let mybid_qty = mybid_qty - buy_qty in
      let myask_qty = myask_qty + buy_qty in
      let mybids = Int.Map.(add empty mybid_price mybid_qty) in
      let myasks = Int.Map.(add empty myask_price myask_qty) in
      let new_state = { state with bids=mybids; asks=myasks; balance=new_balance; nb_contracts=new_nb_contracts } in
      let new_pnl = pnl new_state in
      let pnlchg = new_pnl // old_pnl in
      let pnldiff = new_pnl - old_pnl in
      if pnlchg > 10. || pnlchg < 0.1 then
        Format.eprintf "%a@.%a@.%d %d %dP %f%%@."
          Sexp.pp (sexp_of_state state)
          Sexp.pp (sexp_of_state new_state)
          old_pnl new_pnl pnldiff (pnlchg *. 100.);
      if gnuplot then Format.printf "%d %d %d %d %f@." (seq - !init_seq) mid_price (new_pnl / Int.pow 10 5) pnldiff pnlchg
      else Format.printf "%d %a %a@." seq Sexp.pp (DB.sexp_of_trade trade) Sexp.pp (sexp_of_state new_state);
      new_state
    in
    List.fold_left evts ~init:state ~f:fold_evts
  in
  let db_f db =
    ignore @@ LevelDB_ext.fold ?size ?low ?high ~init:(create_state ()) ~f:blanket db
  in
  let dbdir = Filename.concat datadir symbol in
  if Sys.is_directory_exn dbdir then
    LevelDB_ext.with_db ~f:db_f @@ LevelDB.open_db dbdir

let command =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-gnuplot" no_arg ~doc:" gnuplot output"
    +> flag "-datadir" (optional_with_default "data/poloniex.com" string) ~doc:"path Path of datadir"
    +> flag "-low" (optional int) ~doc:"int Low DB key"
    +> flag "-high" (optional int) ~doc:"int High DB key"
    +> flag "-size" (optional int) ~doc:"int Do not process more than #size DB records"
    +> anon (t2 ("symbol" %: string) ("max_pos_size" %: int))
  in
  Command.basic ~summary:"P/L simulator" spec main

let () = Command.run command
