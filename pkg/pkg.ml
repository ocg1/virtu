#!/usr/bin/env ocaml
#use "topfind"
#require "topkg"
open Topkg

let () =
  Pkg.describe "virtu" @@ fun c ->
  Ok [
    Pkg.bin "src/hedger";
    Pkg.bin "src/virtu";
    Pkg.bin "src/livesim";
    Pkg.bin "src/pnlsim";
    Pkg.bin "src/logobs";
    Pkg.bin "src/poloniex";
    Pkg.bin "src/simu";
  ]
