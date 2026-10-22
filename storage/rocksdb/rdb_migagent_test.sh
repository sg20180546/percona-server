#!/usr/bin/env bash
# [migagent 2026-10-20] Standalone smoke of the MyRocks migration-agent glue, no mysqld build needed:
#   1. rdb_migagent.cc compiles alone against the fork's headers (no MySQL headers);
#   2. a stub libmigagent.so (rdb_migagent_stub.cc) + rdb_migagent_test.cc exercise start / decision /
#      stop / restart through dlopen exactly as ha_rocksdb.cc and Rdb_compact_filter use the glue, and
#      [fence 2026-10-21] the commit gate through the hooks (needs the fork's librocksdb for WriteBatch).
# Usage: bash rdb_migagent_test.sh <CaaS-LSM checkout>/include <dir holding the fork's librocksdb.so>
#   lab 53: bash rdb_migagent_test.sh /data/sungjin/CaaS-LSM/include /data/sungjin/myrocks/fork_build
#   (a WITH_HDFS build needs libhdfs/libjvm at load: the script adds $HADOOP_HOME/lib/native and $JAVA_HOME/lib/server)
set -euo pipefail
INC=${1:?usage: $0 <rocksdb include dir> <librocksdb dir>}
LIBDIR=${2:?usage: $0 <rocksdb include dir> <librocksdb dir>}
DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OUT=${OUT:-$(mktemp -d "${TMPDIR:-/tmp}/rdb_migagent_test.XXXXXX")}
CXX=${CXX:-g++}
echo "[1] standalone compile of rdb_migagent.cc"
$CXX -std=c++17 -Wall -Wextra -fPIC -I"$INC" -c "$DIR/rdb_migagent.cc" -o "$OUT/rdb_migagent.o"
echo "[2] stub libmigagent.so"
$CXX -std=c++17 -Wall -fPIC -shared "$DIR/rdb_migagent_stub.cc" -o "$OUT/libmigagent_stub.so"
echo "[3] test"
$CXX -std=c++17 -Wall -pthread -I"$INC" "$DIR/rdb_migagent_test.cc" "$OUT/rdb_migagent.o" -L"$LIBDIR" \
  -Wl,-rpath,"$LIBDIR" -lrocksdb -ldl -o "$OUT/rdb_migagent_test"
HADOOP_HOME=${HADOOP_HOME:-/data/sungjin/hadoop-3.3.1}
JAVA_HOME=${JAVA_HOME:-/data/sungjin/jdk-11.0.24+8}
LD_LIBRARY_PATH="$HADOOP_HOME/lib/native:$JAVA_HOME/lib/server:$LIBDIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
  "$OUT/rdb_migagent_test" "$OUT/libmigagent_stub.so"
