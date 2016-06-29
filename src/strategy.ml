open Core.Std
open Async.Std

open Bs_devkit.Core
open Bs_api.BMEX
open Util

module type Cfg = sig
  val log : Log.t
  val order_cfg : Order.cfg Lazy.t

  val orders : RespObj.t Uuid.Table.t
  val current_orders : Side.t -> Uuid.t String.Table.t
  val quoted_instruments : instrument_info String.Table.t

  val ticksizes : ticksize String.Table.t
  val remote_ticker : string -> Side.t -> ticker_kind -> Int.t option
  val local_mid_price :
    ?remove_bid:(int * int) ->
    ?remove_ask:(int * int) ->
    string -> ticker_kind -> (int * int) option
  val remote_mid_price : string -> ticker_kind -> (int * int) option
end

module Common (C : Cfg) = struct
  open C

  let current_working_order symbol side =
    let open Option.Monad_infix in
    String.Table.find (current_orders side) symbol >>= fun oid ->
    Uuid.Table.find orders oid >>= fun o ->
    RespObj.int64 o "leavesQty" >>= fun leavesQty ->
    RespObj.bool o "workingIndicator" >>= fun working ->
    if leavesQty > 0L && working then Some o else None

  let price_qty_of_order o =
    let open Option.Monad_infix in
    RespObj.float o "price" >>= fun price ->
    RespObj.int64 o "leavesQty" >>| fun leavesQty ->
    satoshis_int_of_float_exn price, Int64.to_int_exn leavesQty

  let update_orders_price symbol side dprice =
    let mk_order o =
      let oid = RespObj.string_exn o "orderID" in
      let old_price = satoshis_int_of_float_exn @@ RespObj.float_exn o "price" in
      let new_price = match dprice with `Abs p -> p | `Diff dp -> old_price + dp in
      if old_price = new_price then None else begin
        Log.info log "update order price %s %s %s %d -> %d"
          symbol (Side.show side) oid old_price new_price;
        let ticksize = String.Table.find_exn ticksizes symbol in
        Option.some @@ mk_amended_limit_order ~symbol ~ticksize ~price:new_price oid
      end
    in
    Option.Monad_infix.(current_working_order symbol side >>= mk_order)
end

module Blanket (C : Cfg) = struct
  open C
  module Common = Common(C)
  open Common

  let update_orders param =
    let iter_f ~key:symbol ~data:{ update_period } =
      let update_f =
        let oldMidPrice = ref @@ local_mid_price symbol Best in
        let oldRemoteMidPrice = ref @@ remote_mid_price symbol Best in
        fun () ->
          let oldMidPrice' = !oldMidPrice in
          let oldRemoteMidPrice' = !oldRemoteMidPrice in
          let remoteMidPrice = remote_mid_price symbol Best in
          let currentBidOrder = current_working_order symbol Bid in
          let currentAskOrder = current_working_order symbol Ask in
          let currentBidPQty = Option.bind currentBidOrder price_qty_of_order in
          let currentAskPQty = Option.bind currentAskOrder price_qty_of_order in
          let midPrice = local_mid_price ?remove_bid:currentBidPQty ?remove_ask:currentAskPQty symbol Best in
          oldMidPrice := midPrice;
          oldRemoteMidPrice := remoteMidPrice;
          begin match oldMidPrice', midPrice, oldRemoteMidPrice', remoteMidPrice with
          | Some (oldm, olds), Some (newm, news), Some (oldremm, oldrems), Some (remm, rems) when oldm <> newm || oldremm <> remm ->
            let { divisor } = String.Table.find_exn ticksizes symbol in
            let center, spread = match param with
            | `Blanket -> newm, news + divisor
            | `Fixed i -> newm, i * divisor
            | `FixedRemote i ->
              let center =
                let low_boundary = newm - news + i * divisor in
                let high_boundary = newm + news - i * divisor in
                if remm < low_boundary then low_boundary
                else if remm > high_boundary then high_boundary
                else remm
              in
              center, i * divisor
            in
            let orders = List.filter_opt [
                update_orders_price symbol Bid (`Abs (center - spread));
                update_orders_price symbol Ask (`Abs (center + spread))
              ]
            in
            begin match orders with
            | [] -> Deferred.unit
            | orders -> Order.update Lazy.(force order_cfg) orders >>| function
              | Ok _ -> ()
              | Error err -> Log.error log "%s" @@ Error.to_string_hum err
            end
          | _ -> Deferred.unit
          end;
      in
      Clock_ns.run_at_intervals' (Time_ns.Span.of_int_ms update_period) update_f
    in
    String.Table.iteri quoted_instruments ~f:iter_f
end

module FollowBFX (C : Cfg) = struct
  open C
  module Common = Common(C)
  open Common

  let update_orders () =
    let iter_f ~key:symbol ~data:{ update_period } =
      let update_f =
        let old_best_bid = ref @@ remote_ticker symbol Bid Vwap in
        let old_best_ask = ref @@ remote_ticker symbol Ask Vwap in
        fun () ->
          let new_best_bid = remote_ticker symbol Bid Vwap in
          let new_best_ask = remote_ticker symbol Ask Vwap in
          let amended_bid =
            Option.(map2 !old_best_bid new_best_bid (fun old_bb bb ->
                update_orders_price symbol Bid (`Diff (bb - old_bb))) |> join)
          in
          let amended_ask =
            Option.(map2 !old_best_ask new_best_ask (fun old_ba ba ->
                update_orders_price symbol Ask (`Diff (ba - old_ba))) |> join)
          in
          old_best_bid := new_best_bid;
          old_best_ask := new_best_ask;
          let orders = List.filter_opt [amended_bid; amended_ask] in
          match orders with
          | [] -> Deferred.unit
          | orders -> Order.update Lazy.(force order_cfg) orders >>| function
            | Ok _ -> ()
            | Error err -> Log.error log "%s" @@ Error.to_string_hum err
      in
      Clock_ns.run_at_intervals' (Time_ns.Span.of_int_ms update_period) update_f
    in
    String.Table.iteri quoted_instruments ~f:iter_f
end
