/*
   [migagent 2026-10-20] In-process migration agent glue for MyRocks.

   mysqld never links the agent at build time: when rocksdb_migagent_port > 0
   the glue dlopen()s libmigagent.so (the KV server's migration code,
   migration_mechansim_exp serverclient/server_main.cc built with
   -DMIGAGENT_LIB) after the TransactionDB is open, injects the DB and the
   "default" column family, and starts its listener; the data CF's compaction
   filter asks it per key whether a key still belongs to this node. Everything
   is off unless the port is set.

   This header depends ONLY on <string> and rocksdb/slice.h (no MySQL headers)
   so rdb_migagent.cc compiles standalone:
     g++ -std=c++17 -I<fork>/include -c rdb_migagent.cc

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */
#pragma once

#include <string>

/* RocksDB includes */
#include "rocksdb/slice.h"

namespace myrocks {

struct Rdb_migagent_params {
  void *db;             // rocksdb::TransactionDB* (passed as rocksdb::DB*)
  void *data_cf;        // rocksdb::ColumnFamilyHandle* of "default"
  const char *datadir;  // rocksdb_datadir
  unsigned port;        // rocksdb_migagent_port; 0 = agent disabled
  const char *lib;      // rocksdb_migagent_lib: path of libmigagent.so
  const char *addr;     // rocksdb_migagent_addr: listen address
  unsigned shard_id;    // rocksdb_migagent_shard_id
  const char *log_path;  // rocksdb_migagent_log: "" = host logger (mysqld error log)
  void (*log)(int level, const char *msg);  // host logger supplied by ha_rocksdb.cc
                                            // (wraps LogPluginErrMsg); 0 info 1 warn 2 error
};

// Loads the library and starts the agent. Returns true when the agent is
// running OR when p.port == 0 (disabled, nothing loaded); false = failed to
// start (only possible when port > 0): the caller fails plugin init.
bool rdb_migagent_start(const Rdb_migagent_params &p);

// Stops the agent (idempotent). The library stays loaded (no dlclose): the
// agent's threads and the compaction filter may still be unwinding.
void rdb_migagent_stop();

// For Rdb_compact_filter on the data CF (cf id 0): true = the agent says drop
// (the key's range moved away and is not retained for an incoming migration).
// Always false when the agent is not running. Very cheap: one acquire load
// plus the agent's immutable-snapshot lookup.
bool rdb_migagent_should_drop(const rocksdb::Slice &key);

// The loaded library's migagent_version() string, "" when not running.
const char *rdb_migagent_version();

}  // namespace myrocks
