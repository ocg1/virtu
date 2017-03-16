open Core
open Async

open Bs_devkit

module OrderBook = struct
  type client =
    | Subscribe of (string * int) list (* symbol, max_pos_size *)
    | Unsubscribe of string list
    [@@deriving bin_io]

  type ticker = {
    symbol: string;
    side: [`Buy | `Sell];
    best: int;
    vwap: int;
  } [@@deriving bin_io]

  type server =
    | Subscribed of string * int
    | Ticker of ticker
    [@@deriving bin_io]

  let t = Rpc.Pipe_rpc.create
      ~name:"orderbook" ~version:1
      ~bin_query:bin_client
      ~bin_response:bin_server
      ~bin_error:Error.bin_t
      ()
end

module Position = struct
  type query = string * int [@@deriving bin_io]
  type response = unit [@@deriving bin_io]

  let t = Rpc.Rpc.create
      ~name:"position" ~version:1
      ~bin_query ~bin_response
end
