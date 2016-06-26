open Core.Std
open Async.Std

module Cfg = struct
  type cfg = {
    key: string;
    secret: string;
    quote: (string * int * int) list [@default []];
  } [@@deriving yojson]

  type t = (string * cfg) list [@@deriving yojson]
end

let set_sign sign i = match sign, Int.sign i with
| Sign.Zero, _ -> invalid_arg "set_sign"
| _, Zero -> 0
| Pos, Pos -> i
| Neg, Neg -> i
| Pos, Neg -> Int.neg i
| Neg, Pos -> Int.neg i


