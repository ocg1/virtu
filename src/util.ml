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
