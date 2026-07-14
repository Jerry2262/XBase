#!/usr/bin/env bash

set -euo pipefail

rm -rf build/ third_party/

./x.py build -j"$(nproc)" \
  -D FETCHCONTENT_SOURCE_DIR_ROCKSDB=../../rocksdb
