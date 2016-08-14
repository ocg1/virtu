#!/bin/bash

set -x

SRC=_build/src
DST=admin@bitsouk.com:dtc_of_bitmex

./build clean
./build logobs.native

rsync -avz ${SRC}/logobs.native ${DST}/logobs.native
