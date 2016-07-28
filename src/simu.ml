open Core.Std

open Dtc
open Bs_devkit.Core

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
      | Bid -> Int.Map.add bids price qty, asks
      | Ask -> bids, Int.Map.add asks price qty
      end
    | BRemove { side; price } -> begin
      match side with
      | Bid -> Int.Map.remove bids price, asks
      | Ask -> bids, Int.Map.remove asks price
      end
    in
    List.fold evts ~init:(bids, asks) ~f:update_f

  let fold ?(low=Int.min_value) ?(high=Int.max_value) ~init db ~f =
    let buf = Bigstring.create 4096 in
    let bids = ref Int.Map.empty in
    let asks = ref Int.Map.empty in
    let state = ref init in
    let iter_f key value =
      let initialized = ref false in
      let key = Binary_packing.unpack_signed_64_int_big_endian ~buf:key ~pos:0 in
      let len = String.length value in
      Bigstring.From_string.blit ~src:value ~src_pos:0 ~dst:buf ~dst_pos:0 ~len;
      let evts = DB.bin_read_t ~pos_ref:(ref 0) buf in
      if not !initialized && List.length evts > 50 then initialized := true;
      let bids', asks' = update_books !bids !asks evts in
      bids := bids';
      asks := asks';
      let state' = if !initialized then
          f !state bids' asks' key evts
        else !state
      in
      state := state';
      Int.between key ~low ~high
    in
    LevelDB.iter iter_f db

  let with_db ~f = Exn.protectx ~finally:LevelDB.close ~f
end

type state = {
  bids: int Int.Map.t [@default Int.Map.empty];
  asks: int Int.Map.t [@default Int.Map.empty];
  balance: int [@default 0];
  nb_contracts: int [@default 0];
} [@@deriving create, sexp]

let best_bid = Int.Map.max_elt_exn
let best_ask = Int.Map.min_elt_exn

let main datadir low high (symbol, max_pos_size) () =
  let max_pos_size = max_pos_size * 100_000_000 in
  let ticksize = List.Assoc.find_exn ticksizes symbol in
  let fill_prob = make_fill_prob 0.9 (log 0.9 /. log 0.5) in
  let blanket state bids asks seq evts =
    List.fold_left evts ~init:state ~f:(fun state evt ->
        let bb_price, bb_qty = best_bid bids in
        let ba_price, ba_qty = best_ask asks in
        let mid_price = (ba_price + bb_price) / 2 in
        let b_distance = mid_price - bb_price in
        let a_distance = ba_price - mid_price in
        let mybid_qty = Int.abs @@ max_pos_size - state.nb_contracts in
        let myask_qty = Int.abs @@ state.nb_contracts + max_pos_size in
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
        match evt with
        | DB.Trade { ts; side=Buy; price; qty } ->
          if Random.float 1. > fill_prob (my_a_distance // a_distance) then
            { state with bids=mybids; asks=myasks }
          else
            let myask_p, myask_qty = best_ask state.asks in
            let sell_qty = Int.min qty myask_qty in
            let new_balance = state.balance + myask_p * sell_qty in
            let new_nb_contracts = state.nb_contracts - sell_qty in
            let mybid_qty = mybid_qty + sell_qty in
            let myask_qty = myask_qty - sell_qty in
            let mybids = Int.Map.(add empty mybid_price mybid_qty) in
            let myasks = Int.Map.(add empty myask_price myask_qty) in
            let new_state = { bids=mybids; asks=myasks; balance=new_balance; nb_contracts=new_nb_contracts } in
            Format.printf "%a@." Sexp.pp (sexp_of_state new_state);
            new_state
        | DB.Trade { ts; side=Sell; price; qty } ->
          if Random.float 1. > fill_prob (my_b_distance // b_distance) then
            { state with bids=mybids; asks=myasks }
          else
            let mybid_p, mybid_qty = best_bid state.bids in
            let sell_qty = Int.min qty myask_qty in
            let new_balance = state.balance - mybid_p * sell_qty in
            let new_nb_contracts = state.nb_contracts + sell_qty in
            let mybid_qty = mybid_qty - sell_qty in
            let myask_qty = myask_qty + sell_qty in
            let mybids = Int.Map.(add empty mybid_price mybid_qty) in
            let myasks = Int.Map.(add empty myask_price myask_qty) in
            let new_state = { bids=mybids; asks=myasks; balance=new_balance; nb_contracts=new_nb_contracts } in
            Format.printf "%a@." Sexp.pp (sexp_of_state new_state);
            new_state
        | _ ->
          { state with bids=mybids; asks=myasks }
      )
  in
  let db_f db =
    ignore @@ LevelDB_ext.fold ?low ?high ~init:(create_state ()) ~f:blanket db
  in
  let dbdir = Filename.concat datadir symbol in
  if Sys.is_directory_exn dbdir then
    LevelDB_ext.with_db ~f:db_f @@ LevelDB.open_db dbdir

let command =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-datadir" (optional_with_default "data/poloniex.com" string) ~doc:"path Path of datadir"
    +> flag "-low" (optional int) ~doc:"timestamp Low timestamp"
    +> flag "-high" (optional int) ~doc:"timestamp High timestamp"
    +> anon (t2 ("symbol" %: string) ("max_pos_size" %: int))
  in
  Command.basic ~summary:"P/L simulator" spec main

let () = Command.run command
