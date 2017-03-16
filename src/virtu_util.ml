open Core
open Async

(* open Dtc *)
open Bs_devkit
open Bs_api.BMEX

module Side = struct
  type t = [`Buy | `Sell]

  let to_bitmex_string = function
  | `Buy -> "Buy"
  | `Sell -> "Sell"

  let pp_bitmex ppf side =
    Format.fprintf ppf "%s" (to_bitmex_string side)
end

module Order = struct
  type cfg = {
    dry_run: bool ;
    current_bids: RespObj.t String.Table.t ;
    current_asks: RespObj.t String.Table.t ;
    testnet: bool ;
    key: string ;
    secret: Cstruct.t ;
    log: Log.t option ;
  }

  let create_cfg
      ?(dry_run=false)
      ?(current_bids=String.Table.create ())
      ?(current_asks=String.Table.create ())
      ?(testnet=true)
      ?(key="")
      ?(secret=Cstruct.of_string "")
      ?log () =
    { dry_run ; current_bids ; current_asks ; testnet ; key ; secret ; log }

  let oid_of_respobj o = match RespObj.(string o "clOrdID", string o "orderID") with
  | Some clOrdID, _ when clOrdID <> "" -> `C, clOrdID
  | _, Some orderID when orderID <> "" -> `S, orderID
  | _ -> invalid_arg "oid_of_respobj"

  let position cfg =
    Rest.Position.position ?log:cfg.log ~testnet:cfg.testnet ~key:cfg.key ~secret:cfg.secret ()

  let submit ?buf cfg orders =
    if orders = [] then Deferred.Or_error.error_string "submit: empty orders"
    else if cfg.dry_run then Deferred.Or_error.return (`List orders)
    else
    Rest.Order.submit ?buf ?log:cfg.log ~testnet:cfg.testnet ~key:cfg.key ~secret:cfg.secret orders >>| function
    | Ok res ->
      List.iter orders ~f:begin fun o ->
        let o = RespObj.of_json o in
        let sym = RespObj.string_exn o "symbol" in
        let side = Option.value_exn (RespObj.string_exn o "side" |> side_of_bmex) in
        let current_table = match side with `Buy -> cfg.current_bids | `Sell -> cfg.current_asks in
        String.Table.set current_table sym o
      end;
      Ok res
    | Error err ->
      List.iter orders ~f:begin fun o ->
        let o = RespObj.of_json o in
        let sym = RespObj.string_exn o "symbol" in
        let side = Option.value_exn (RespObj.string_exn o "side" |> side_of_bmex) in
        let current_table = match side with `Buy -> cfg.current_bids | `Sell -> cfg.current_asks in
        String.Table.remove current_table sym;
      end;
      Error err

  let update cfg orders =
    if orders = [] then Deferred.Or_error.error_string "update: empty orders"
    else if cfg.dry_run then
      Deferred.Or_error.return (`List orders)
    else
    Bs_api.BMEX.Rest.Order.update ?log:cfg.log ~testnet:cfg.testnet ~key:cfg.key ~secret:cfg.secret orders  >>| function
    | Error err -> Error err
    | Ok res -> List.iter orders ~f:begin fun o ->
        let o = RespObj.of_json o in
        let sym = RespObj.string_exn o "symbol" in
        let side = Option.value_exn (RespObj.string_exn o "side" |> side_of_bmex) in
        let current_table = match side with `Buy -> cfg.current_bids | `Sell -> cfg.current_asks in
        String.Table.update current_table sym ~f:begin function
          | Some old_o -> RespObj.merge old_o o
          | None -> o
        end
      end;
      Ok res

  let cancel_all ?symbol ?filter cfg =
    if cfg.dry_run then begin
      String.Table.clear cfg.current_bids;
      String.Table.clear cfg.current_asks;
      Deferred.Or_error.return `Null
    end
    else
    Rest.Order.cancel_all ?log:cfg.log ~testnet:cfg.testnet ~key:cfg.key ~secret:cfg.secret ?symbol ?filter ()

  let cancel_all_after cfg timeout =
    if cfg.dry_run then
      Deferred.Or_error.return `Null
    else
    Rest.Order.cancel_all_after ?log:cfg.log ~testnet:cfg.testnet ~key:cfg.key ~secret:cfg.secret timeout
end

type best_price_kind = Best | Vwap

type instrument_info = {
  bids_initialized: unit Ivar.t ;
  asks_initialized: unit Ivar.t ;
  max_pos_size: int ;
  ticker: (int * int) Pipe.Reader.t ;
}

let create_instrument_info
    ?(bids_initialized=Ivar.create ())
    ?(asks_initialized=Ivar.create ())
    ~max_pos_size ~ticker () =
  { bids_initialized ; asks_initialized ; max_pos_size ; ticker }

type ticksize = {
  multiplier: Float.t; (* i.e. 0.01 *)
  mult_exponent: Int.t; (* 0.01 -> -2 *)
  divisor: Int.t; (* i.e. 1_000_000 *)
}

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
| `Buy -> Int.Map.max_elt
| `Sell -> Int.Map.min_elt

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
  Printf.sprintf "%.*f" (Int.neg mult_exponent) (Float.of_int (price / divisor) *. multiplier) |> Float.of_string

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
    "side", `String (Side.to_bitmex_string side);
    "price", `Float (float_of_satoshis symbol ticksize price);
    "orderQty", `Int qty;
    "execInst", `String "ParticipateDoNotInitiate"
  ]

let mk_amended_limit_order ?price ?qty ~symbol ~side ~ticksize orig oid =
  `Assoc (List.filter_opt [
      Some ("symbol", `String symbol);
      Some ("side", `String (Side.to_bitmex_string side));
      Some ((match orig with `S -> "orderID" | `C -> "clOrdID"), `String oid);
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

let price_qty_of_order o =
  let open Option.Monad_infix in
  RespObj.float o "price" >>= fun price ->
  RespObj.int64 o "leavesQty" >>| fun leavesQty ->
  satoshis_int_of_float_exn price, Int64.to_int_exn leavesQty
