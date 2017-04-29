open Core
open Async

open Bs_devkit
open Bs_api.BMEX
open Virtu_util

module type Cfg = sig
  val log : Log.t
  val order_cfg : Order.cfg Lazy.t

  val current_bids : RespObj.t String.Table.t
  val current_asks : RespObj.t String.Table.t
  val quoted_instruments : instrument_info String.Table.t

  val ticksizes : ticksize String.Table.t
  val local_best_price :
    ?remove_order:(int * int) ->
    Side.t -> string -> (int * int) option
end

module Common (C : Cfg) = struct
  open C

  let current_working_order symbol side =
    let open Option.Monad_infix in
    let current_orders = match side with
    | `Buy -> current_bids
    | `Sell -> current_asks in
    String.Table.find current_orders symbol >>= fun o ->
    RespObj.int64 o "leavesQty" >>= fun leavesQty ->
    RespObj.bool o "workingIndicator" >>= fun working ->
    if leavesQty > 0L && working then Some o else None

  let update_orders_price symbol side dprice tickSize =
    let mk_order o =
      let orig, oid = Order.oid_of_respobj o in
      let old_price = satoshis_int_of_float_exn @@ RespObj.float_exn o "price" in
      let new_price = match dprice with `Abs p -> p | `Diff dp -> old_price + dp in
      if old_price = new_price then None else begin
        Log.info log "update order price %s %s %s %d -> %d"
          symbol (Side.to_bitmex_string side)
          (String.sub oid 0 8)
          (old_price / tickSize) (new_price / tickSize);
        let ticksize = String.Table.find_exn ticksizes symbol in
        Option.some @@ mk_amended_limit_order ~symbol ~side ~ticksize ~price:new_price orig oid
      end
    in
    Option.Monad_infix.(current_working_order symbol side >>= mk_order)
end

module Blanket (C : Cfg) = struct
  open C
  module Common = Common(C)
  open Common

  let update_price ~remBid ~remAsk ~locBid ~locAsk symbol strategy =
    let { divisor=tickSize } = String.Table.find_exn ticksizes symbol in
    let newBid, newAsk = match strategy with
    | `Fixed i -> locBid + i * tickSize, locAsk - i * tickSize
    | `FixedRemote i ->
      let newFixedBid = locBid + i * tickSize in
      let newFixedAsk = locAsk - i * tickSize in
      (if remBid > newFixedBid then Int.min remBid newFixedAsk else newFixedBid),
      (if remAsk < newFixedAsk then Int.max remAsk newFixedAsk else newFixedAsk)
    in
    let newBid = Int.(min newBid @@ locAsk - tickSize) in
    let newAsk = Int.(max newAsk @@ locBid + tickSize) in
    let newAsk = Int.(max newAsk @@ newBid + tickSize) in
    List.filter_opt [
      update_orders_price symbol `Buy (`Abs newBid) tickSize;
      update_orders_price symbol `Sell (`Abs newAsk) tickSize
    ]

  let update_orders strategy =
    let iter_f (symbol, { ticker }) =
      let { divisor=tickSize } = String.Table.find_exn ticksizes symbol in
      let update_f =
        let oldLocBid = local_best_price `Buy symbol |> Option.map ~f:fst |> Ref.create in
        let oldLocAsk = local_best_price `Sell symbol |> Option.map ~f:fst |> Ref.create in
        let oldRemBid = ref 0 in
        let oldRemAsk = ref 0 in
        let latest_update = ref Time_ns.epoch in
        fun (newRemBid, newRemAsk) ->
          let newRemBid = newRemBid / tickSize * tickSize in
          let newRemAsk = newRemAsk / tickSize * tickSize in
          let now = Time_ns.now () in
          let time_elapsed = Time_ns.diff now !latest_update in
          if Time_ns.Span.(time_elapsed < of_int_ms 1500) then Deferred.unit
          else begin
            latest_update := now;
            let currentBidOrder = current_working_order symbol `Buy in
            let currentAskOrder = current_working_order symbol `Sell in
            let currentBidPQty = Option.bind currentBidOrder price_qty_of_order in
            let currentAskPQty = Option.bind currentAskOrder price_qty_of_order in
            let newLocBid = local_best_price ?remove_order:currentBidPQty `Buy symbol |> Option.map ~f:fst in
            let newLocAsk = local_best_price ?remove_order:currentAskPQty `Sell symbol |> Option.map ~f:fst in
            let orders =
              match !oldLocBid, !oldLocAsk, newLocBid, newLocAsk with
              | Some oldlb, Some oldla, Some newlb, Some newla when oldlb <> oldla || newlb <> newla || newRemBid <> !oldRemBid || newRemAsk <> !oldRemAsk ->
                update_price ~remBid:newRemBid ~remAsk:newRemAsk ~locBid:newlb ~locAsk:newla symbol strategy
              | _ -> []
            in
            oldLocBid := newLocBid;
            oldLocAsk := newLocAsk;
            oldRemBid := newRemBid;
            oldRemAsk := newRemAsk;
            begin match orders with
            | [] -> Deferred.unit
            | _ -> Order.update Lazy.(force order_cfg) orders >>| function
              | Ok _ -> ()
              | Error err -> Log.error log "strategy: %s: %s" (Yojson.Safe.to_string (`List orders)) (Error.to_string_hum err)
            end
          end
      in
      Monitor.handle_errors ~name:"update_orders"
        (fun () -> Pipe.iter ~continue_on_error:true ticker ~f:update_f)
        (fun exn -> Log.error log "%s" @@ Exn.to_string exn)
    in
    String.Table.to_alist quoted_instruments |>
    Deferred.List.iter ~how:`Parallel ~f:iter_f

end
