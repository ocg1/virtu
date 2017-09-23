open Core
open Async

open Bs_devkit

module Order = struct
  open Bmex
  module R = Bmex_rest

  type cfg = {
    dry_run: bool ;
    current_bids: R.Order.t String.Table.t ;
    current_asks: R.Order.t String.Table.t ;
    testnet: bool ;
    key: string ;
    secret: string ;
    log: Log.t option ;
  }

  let create_cfg
      ?(dry_run=false)
      ?(current_bids=String.Table.create ())
      ?(current_asks=String.Table.create ())
      ?(testnet=true)
      ?(key="")
      ?(secret="")
      ?log () =
    { dry_run ; current_bids ; current_asks ; testnet ; key ; secret ; log }

  let oid_of_respobj o = match RespObj.(string o "clOrdID", string o "orderID") with
  | Some clOrdID, _ when clOrdID <> "" -> `C, clOrdID
  | _, Some orderID when orderID <> "" -> `S, orderID
  | _ -> invalid_arg "oid_of_respobj"

  let position cfg =
    R.Position.get ?log:cfg.log ~testnet:cfg.testnet ~key:cfg.key ~secret:cfg.secret ()

  let submit ?buf cfg orders =
    if orders = [] then Deferred.Or_error.error_string "submit: empty orders"
    else if cfg.dry_run then
      Deferred.Or_error.return (Cohttp.Response.make (), `Assoc [])
    else
    R.Order.submit_bulk
      ?buf
      ?log:cfg.log
      ~testnet:cfg.testnet
      ~key:cfg.key
      ~secret:cfg.secret orders >>| function
    | Ok res ->
      List.iter orders ~f:begin fun o ->
        match Int.sign o.orderQty with
        | Zero -> ()
        | Pos -> String.Table.set cfg.current_bids o.symbol o
        | Neg -> String.Table.set cfg.current_asks o.symbol o
      end ;
      Ok res
    | Error err ->
      List.iter orders ~f:begin fun o ->
        match Int.sign o.orderQty with
        | Zero -> ()
        | Pos -> String.Table.remove cfg.current_bids o.symbol
        | Neg -> String.Table.remove cfg.current_asks o.symbol
      end ;
      Error err

  let update_table table sym o =
    String.Table.update table sym ~f:begin function
    | Some old_o ->
      (* RespObj.merge old_o o *)
      old_o (* FIXME!!! *)
    | None -> o
    end

  (* let update cfg orders = *)
  (*   if orders = [] then Deferred.Or_error.error_string "update: empty orders" *)
  (*   else if cfg.dry_run then *)
  (*     Deferred.Or_error.return (Cohttp.Response.make (), `Assoc []) *)
  (*   else *)
  (*   R.Order.amend_bulk *)
  (*     ?log:cfg.log *)
  (*     ~testnet:cfg.testnet *)
  (*     ~key:cfg.key *)
  (*     ~secret:cfg.secret orders  >>| function *)
  (*   | Error err -> Error err *)
  (*   | Ok res -> List.iter orders ~f:begin fun o -> *)
  (*       match Option.value_map ~default:Sign.Zero ~f:Int.sign o.leavesQty with *)
  (*       | Zero -> () *)
  (*       | Pos -> update_table cfg.current_bids o.symbol o *)
  (*       | Neg -> update_table cfg.current_asks o.symbol o *)
  (*     end ; *)
  (*     Ok res *)

  let cancel_all ?symbol ?filter cfg =
    if cfg.dry_run then begin
      String.Table.clear cfg.current_bids;
      String.Table.clear cfg.current_asks;
      Deferred.Or_error.return (Cohttp.Response.make ())
    end
    else
    R.Order.cancel_all
      ?log:cfg.log
      ~testnet:cfg.testnet
      ~key:cfg.key
      ~secret:cfg.secret ?symbol ?filter ()

  let cancel_all_after cfg timeout =
    if cfg.dry_run then
      Deferred.Or_error.return (Cohttp.Response.make ())
    else
    R.Order.cancel_all_after
      ?log:cfg.log
      ~testnet:cfg.testnet
      ~key:cfg.key
      ~secret:cfg.secret
      timeout
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
  Int.Map.change book price ~f:begin function
  | None -> if allow_empty then Some qty else invalid_arg "book_modify_qty"
  | Some oldq ->
    let newq = oldq + qty in
    if newq > 0 then Some newq else None
  end

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

let mk_new_market_order ~symbol ~orderQty =
  Bmex_rest.Order.create ~symbol ~orderQty ~ordType:`order_type_market ()

let mk_new_limit_order ~clOrdID ~symbol ~orderQty ~ticksize ~price =
  Bmex_rest.Order.create ~symbol ~clOrdID ~orderQty
    ~price:(float_of_satoshis symbol ticksize price)
    ~execInst:[ParticipateDoNotInitiate] ()

type order_id =
  | Server of Uuid.t
  | Client of string

let mk_amended_limit_order ?price ?leavesQty ~symbol ~ticksize oid =
  let orderID, clOrdID =
    match oid with
    | Server oid -> Some oid, None
    | Client oid -> None, Some oid in
  let price = Option.map price
      ~f:(fun price -> (float_of_satoshis symbol ticksize price)) in
  Bmex_rest.Order.create_amend ?orderID ?clOrdID ?leavesQty ?price ()

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
