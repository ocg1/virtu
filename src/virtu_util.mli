open Core
open Async
open Bs_devkit

val set_sign :
  Sign.t -> int -> int

val book_add_qty :
  int Int.Map.t -> Int.Map.Key.t -> int -> int Int.Map.t

val book_modify_qty :
  ?allow_empty:bool -> int Int.Map.t -> Int.Map.Key.t ->
  int -> int Int.Map.t

val of_remote_sym : string -> string
val to_remote_sym : string -> string

val price_qty_of_order :
  Yojson.Safe.json String.Map.t -> (int * int) option

type best_price_kind = Best | Vwap

type instrument_info = {
  bids_initialized: unit Ivar.t ;
  asks_initialized: unit Ivar.t ;
  max_pos_size: int ;
  ticker: (int * int) Pipe.Reader.t ;
}

val create_instrument_info :
  ?bids_initialized:unit Ivar.t ->
  ?asks_initialized:unit Ivar.t ->
  max_pos_size:int ->
  ticker:(int * int) Pipe.Reader.t -> unit -> instrument_info

type ticksize = {
  multiplier: Float.t; (* i.e. 0.01 *)
  mult_exponent: Int.t; (* 0.01 -> -2 *)
  divisor: Int.t; (* i.e. 1_000_000 *)
}

val exponent_divisor_of_tickSize :
  float -> int * int

val best_of_side :
  [< `Buy | `Sell ] -> 'a Int.Map.t -> (Int.Map.Key.t * 'a) option

module Order : sig
  type cfg = {
    dry_run: bool ;
    current_bids: RespObj.t String.Table.t ;
    current_asks: RespObj.t String.Table.t ;
    testnet: bool ;
    key: string ;
    secret: string ;
    log: Log.t option ;
  }

  val create_cfg :
    ?dry_run:bool ->
    ?current_bids:RespObj.t String.Table.t ->
    ?current_asks:RespObj.t String.Table.t ->
    ?testnet:bool ->
    ?key:string -> ?secret:string -> ?log:Log.t -> unit -> cfg

  module Oid : sig
    type t =
      | Server of Uuid.t
      | Client of string

    val pp : Format.formatter -> t -> unit
    val show : t -> string
  end

  val oid_of_respobj :
    Yojson.Safe.json String.Map.t -> Oid.t
  val position :
    cfg -> (Cohttp.Response.t * Yojson.Safe.json list) Deferred.Or_error.t

  val submit :
    ?buf:Bi_outbuf.t ->
    cfg ->
    Bmex_rest.Order.t list ->
    (Cohttp.Response.t * Yojson.Safe.json) Deferred.Or_error.t

  val update :
    ?buf:Bi_outbuf.t ->
    cfg ->
    Bmex_rest.Order.amend list ->
    (Cohttp.Response.t * Yojson.Safe.json) Deferred.Or_error.t

  val cancel_all :
    ?symbol:string ->
    ?filter:Yojson.Safe.json -> cfg ->
    Cohttp.Response.t Deferred.Or_error.t

  val cancel_all_after :
    cfg -> Time_ns.Span.t ->
    Cohttp.Response.t Deferred.Or_error.t
end

val mk_amended_limit_order :
  ?price:int ->
  ?leavesQty:int ->
  symbol:string ->
  ticksize:ticksize ->
  Order.Oid.t ->
  Bmex_rest.Order.amend

val mk_new_market_order :
  symbol:string ->
  orderQty:int ->
  Bmex_rest.Order.t

val mk_new_limit_order :
  clOrdID:string ->
  symbol:string ->
  orderQty:int ->
  ticksize:ticksize ->
  price:int ->
  Bmex_rest.Order.t


