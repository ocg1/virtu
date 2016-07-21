open Core.Std

open Bs_devkit.Core

module OB = struct
  type update = {
    id: int;
    side: Side.t;
    price: int [@default 0]; (* in satoshis *)
    size: int [@default 0] (* in contracts or in tick size *);
  } [@@deriving create, sexp, bin_io]

  type t = {
    action: Bs_api.BMEX.update_action;
    data: update;
  } [@@deriving create, sexp, bin_io]
end
