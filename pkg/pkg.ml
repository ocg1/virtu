#!/usr/bin/env ocaml
#use "topfind"
#require "topkg"
open Topkg

let () =
  Pkg.describe "virtu" @@ fun c ->
  Ok [
    Pkg.bin "src/hedger";
    Pkg.bin "src/virtu";
    Pkg.bin "src/sim";
    Pkg.bin "src/logobs";
  ]
