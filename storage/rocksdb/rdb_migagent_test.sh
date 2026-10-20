#!/usr/bin/env bash
# [migagent 2026-10-20] Standalone smoke of the MyRocks migration-agent glue, no mysqld build needed:
#   1. rdb_migagent.cc compiles alone against the fork's headers (no MySQL headers);
#   2. a 20-line stub libmigagent.so (rdb_migagent_stub.cc) + rdb_migagent_test.cc exercise start / decision /
#      stop / restart through dlopen exactly as ha_rocksdb.cc and Rdb_compact_filter use the glue.
# Usage: bash rdb_migagent_test.sh <CaaS-LSM checkout>/include   (only rocksdb/slice.h is used)
set -euo pipefail
INC=${1:?usage: $0 <rocksdb include dir>}
DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OUT=${OUT:-$(mktemp -d "${TMPDIR:-/tmp}/rdb_migagent_test.XXXXXX")}
CXX=${CXX:-g++}
echo "[1] standalone compile of rdb_migagent.cc"
$CXX -std=c++17 -Wall -Wextra -I"$INC" -c "$DIR/rdb_migagent.cc" -o "$OUT/rdb_migagent.o"
echo "[2] stub libmigagent.so"
$CXX -std=c++17 -Wall -fPIC -shared "$DIR/rdb_migagent_stub.cc" -o "$OUT/libmigagent_stub.so"
echo "[3] test"
$CXX -std=c++17 -Wall -I"$INC" "$DIR/rdb_migagent_test.cc" "$OUT/rdb_migagent.o" -ldl -o "$OUT/rdb_migagent_test"
"$OUT/rdb_migagent_test" "$OUT/libmigagent_stub.so"
