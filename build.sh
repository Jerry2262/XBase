#!/usr/bin/env bash

set -euo pipefail

if [[ -n "${BUILD_JOBS:-}" ]]; then
  build_jobs="${BUILD_JOBS}"
else
  build_jobs="$(nproc)"
  ((build_jobs > 32)) && build_jobs=32
fi

if [[ "${CLEAN_BUILD:-0}" == "1" ]]; then
  rm -rf build/
fi
if [[ "${CLEAN_THIRD_PARTY:-0}" == "1" ]]; then
  rm -rf third_party/
fi

./x.py build -j"${build_jobs}" \
  -D BRPC_BUILD_JOBS="${build_jobs}" \
  -D CMAKE_WARN_DEPRECATED=OFF \
  -D FETCHCONTENT_SOURCE_DIR_ROCKSDB=../../rocksdb \
  -D BRPC_SOURCE_DIR_OVERRIDE=../../brpc
