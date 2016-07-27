open Core.Std
open Async.Std
open Log.Global

open Dtc
open Bs_devkit.Core

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
      let key = Binary_packing.unpack_signed_64_int_big_endian ~buf:key ~pos:0 in
      let len = String.length value in
      Bigstring.From_string.blit ~src:value ~src_pos:0 ~dst:buf ~dst_pos:0 ~len;
      let evts = DB.bin_read_t ~pos_ref:(ref 0) buf in
      let bids', asks' = update_books !bids !asks evts in
      bids := bids';
      asks := asks';
      let state' = f !state bids' asks' key evts in
      state := state';
      Int.between key ~low ~high
    in
    LevelDB.iter iter_f db

  let with_db ~f = Exn.protectx ~finally:LevelDB.close ~f
end

type state = {
  bids: DB.book_entry list [@default []];
  asks: DB.book_entry list [@default []];
  balance: int [@default 0];
  contracts: int [@default 0];
} [@@deriving create]

let main low high (db, ticksize, max_pos_size) () =
  let db_f db =
    let fold_f state bids asks seq evts = state in
    let _state' = LevelDB_ext.fold ?low ?high ~init:(create_state ()) ~f:fold_f in
    ()
  in
  let db = LevelDB.open_db db in
  LevelDB_ext.with_db db ~f:db_f

let command =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-low" (optional int) ~doc:"timestamp Low timestamp"
    +> flag "-high" (optional int) ~doc:"timestamp High timestamp"
    +> anon (t3 ("db" %: string) ("tick_size" %: int) ("max_pos_size" %: int))
  in
  Command.basic ~summary:"P/L simulator" spec main

let () = Command.run command
