open Core.Std
open Async.Std

open Bs_devkit.Core
open Bs_api.BMEX

module Cfg = struct
  type cfg = {
    key: string;
    secret: string;
    quote: (string * int) list [@default []];
  } [@@deriving yojson]

  type t = (string * cfg) list [@@deriving yojson]
end

module Order = struct
  type cfg = {
    dry_run: bool [@default false];
    orders_t: RespObj.t Uuid.Table.t [@default Uuid.Table.create ()];
    current_bids: RespObj.t String.Table.t [@default String.Table.create ()];
    current_asks: RespObj.t String.Table.t [@default String.Table.create ()];
    testnet: bool [@default true];
    key: string [@default ""];
    secret: Cstruct.t [@default Cstruct.of_string ""];
    log: Log.t option;
  } [@@deriving create]

  let oid_of_respobj o = match RespObj.(string o "clOrdID", string o "orderID") with
  | Some clOrdID, _ when clOrdID <> "" -> `C, clOrdID
  | _, Some orderID when orderID <> "" -> `S, orderID
  | _ -> invalid_arg "oid_of_respobj"

  let submit cfg orders =
    if orders = [] then invalid_arg "submit: empty orders";
    if cfg.dry_run then
      Deferred.Or_error.return @@
      Printf.sprintf "[Sim] submitted %s" Yojson.Safe.(to_string @@ `List orders)
    else
    Monitor.try_with_or_error ~name:"Order.submit" (fun () ->
        Bs_api.BMEX.Rest.Order.submit ?log:cfg.log
          ~testnet:cfg.testnet ~key:cfg.key ~secret:cfg.secret orders
      ) >>| function
    | Ok res ->
      List.iter orders ~f:(fun o ->
          let o = RespObj.of_json o in
          let sym = RespObj.string_exn o "symbol" in
          let side = RespObj.string_exn o "side" |> buy_sell_of_bmex in
          let current_table = match side with `Buy -> cfg.current_bids | `Sell -> cfg.current_asks in
          String.Table.set current_table sym o
        );
      Ok res
    | Error err ->
      List.iter orders ~f:(fun o ->
          let o = RespObj.of_json o in
          let sym = RespObj.string_exn o "symbol" in
          let side = RespObj.string_exn o "side" |> buy_sell_of_bmex in
          let current_table = match side with `Buy -> cfg.current_bids | `Sell -> cfg.current_asks in
          String.Table.remove current_table sym;
        );
      Error err

  let update cfg orders =
    if orders = [] then invalid_arg "update: empty orders";
    if cfg.dry_run then
      Deferred.Or_error.return @@
      Printf.sprintf "[Sim] updated %s" Yojson.Safe.(to_string @@ `List orders)
    else
    Monitor.try_with_or_error ~name:"Order.update" (fun () ->
        Bs_api.BMEX.Rest.Order.update ?log:cfg.log ~testnet:cfg.testnet ~key:cfg.key ~secret:cfg.secret orders
      ) >>| function
    | Error err -> Error err
    | Ok res -> List.iter orders ~f:(fun o ->
        let o = RespObj.of_json o in
        let orig, oid_str = oid_of_respobj o in
        match Uuid.Table.find cfg.orders_t Uuid.(of_string oid_str) with
        | None -> ()
        | Some o ->
          let sym = RespObj.string_exn o "symbol" in
          let side = RespObj.string_exn o "side" |> buy_sell_of_bmex in
          let current_table = match side with `Buy -> cfg.current_bids | `Sell -> cfg.current_asks in
          String.Table.update current_table sym ~f:(function
            | Some old_o -> RespObj.merge old_o o
            | None -> o)
      );
      Ok res

  let cancel_all ?symbol ?filter cfg =
    if cfg.dry_run then begin
      String.Table.clear cfg.current_bids;
      String.Table.clear cfg.current_asks;
      Deferred.Or_error.return "[Sim] canceled all orders"
    end
    else
    Monitor.try_with_or_error ~name:"Order.cancel_all" (fun () ->
        Bs_api.BMEX.Rest.Order.cancel_all ?log:cfg.log ~testnet:cfg.testnet ~key:cfg.key ~secret:cfg.secret ?symbol ?filter ()
      )

  let cancel_all_after cfg timeout =
    if cfg.dry_run then
      Deferred.Or_error.return @@ Printf.sprintf "[Sim] cancel all after %d" timeout
    else
    Monitor.try_with_or_error ~name:"Order.cancel_all_after" (fun () ->
        Bs_api.BMEX.Rest.Order.cancel_all_after ?log:cfg.log ~testnet:cfg.testnet ~key:cfg.key ~secret:cfg.secret timeout
      )
end

type best_price_kind = Best | Vwap

type instrument_info = {
  bids_initialized: unit Ivar.t [@default Ivar.create ()];
  asks_initialized: unit Ivar.t [@default Ivar.create ()];
  max_pos_size: int;
  ticker: (int * int) Pipe.Reader.t;
} [@@deriving create]

type ticksize = {
  multiplier: Float.t; (* i.e. 0.01 *)
  mult_exponent: Int.t; (* 0.01 -> -2 *)
  divisor: Int.t; (* i.e. 1_000_000 *)
} [@@deriving create]

let set_sign sign i = match sign, Int.sign i with
| Sign.Zero, _ -> invalid_arg "set_sign"
| _, Zero -> 0
| Pos, Pos -> i
| Neg, Neg -> i
| Pos, Neg -> Int.neg i
| Neg, Pos -> Int.neg i

let book_add_qty book price qty =
  Int.Map.update book price ~f:(function
    | None -> qty
    | Some oldq -> oldq + qty)

let book_modify_qty ?(allow_empty=false) book price qty =
  Int.Map.change book price ~f:(function
    | None -> if allow_empty then Some qty else invalid_arg "book_modify_qty"
    | Some oldq ->
      let newq = oldq + qty in
      if newq > 0 then Some newq else None
    )

let best_of_side = function
| Side.Bid -> Int.Map.max_elt
| Ask -> Int.Map.min_elt

let exponent_divisor_of_tickSize tickSize =
  let ts_str = Printf.sprintf "%.0e" tickSize in
  let e = String.index_exn ts_str 'e' in
  let exponent = String.(sub ts_str (e+2) (length ts_str - e - 2)) |> Int.of_string in
  let exponent = match String.get ts_str (e+1) with
  | '+' -> exponent
  | '-' -> Int.neg exponent
  | _ -> invalid_arg "digits_of_tickSize"
  in
  exponent, Int.(pow 10 (8 + exponent))

let float_of_satoshis symbol { multiplier; mult_exponent; divisor } price =
  let res = Printf.sprintf "%.*f" (Int.neg mult_exponent) (Float.of_int (price / divisor) *. multiplier) in
  Float.of_string res

let mk_new_market_order ~symbol ~qty : Yojson.Safe.json =
  `Assoc [
    "symbol", `String symbol;
    "orderQty", `Int qty;
    "ordType", `String "Market";
  ]

let mk_new_limit_order ~symbol ~ticksize ~side ~price ~qty uuid_str =
  `Assoc [
    "clOrdID", `String uuid_str;
    "symbol", `String symbol;
    "side", `String (match side with Side.Bid -> "Buy" | Ask -> "Sell");
    "price", `Float (float_of_satoshis symbol ticksize price);
    "orderQty", `Int qty;
    "execInst", `String "ParticipateDoNotInitiate"
  ]

let mk_amended_limit_order ?price ?qty ~symbol ~ticksize orderID =
  `Assoc (List.filter_opt [
    Some ("orderID", `String orderID);
    Option.map qty ~f:(fun qty -> "leavesQty", `Int qty);
    Option.map price ~f:(fun price -> "price", `Float (float_of_satoshis symbol ticksize price));
    ])

let to_remote_sym = function
  | "XBTUSD" -> "BTCUSD"
  | "LTCUSD" -> "LTCUSD"
  | "ETHXBT" -> "BTC_ETH"
  | "LSKXBT" -> "BTC_LSK"
  | "DAOETH" -> "ETH_DAO"
  | "FCTXBT" -> "BTC_FCT"
  | _  -> invalid_arg "to_remote_sym"

let of_remote_sym = function
  | "BTCUSD" -> "XBTUSD"
  | "LTCUSD" -> "LTCUSD"
  | "BTC_ETH" -> "ETHXBT"
  | "BTC_LSK" -> "LSKXBT"
  | "ETH_DAO" -> "DAOETH"
  | "BTC_FCT" -> "FTCXBT"
  | _  -> invalid_arg "to_remote_sym"
