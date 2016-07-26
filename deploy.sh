#!/bin/bash

set -x

SRC=_build/src
DST=admin@bitsouk.com:dtc_of_bitmex

rsync -avz ${SRC}/poloniex.native ${DST}/poloniex.native
