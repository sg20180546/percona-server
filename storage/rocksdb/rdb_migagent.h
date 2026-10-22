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

namespace rocksdb {
class WriteBatch;
}

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

// [migagent fence 2026-10-21] Commit-time write fence (CAB with Q23=commit; MIGAGENT.md section 6, "must implement before
// any write workload"). The agent does not see SQL, so the fence lives here and the agent only reports its state through
// the ABI hooks that were unused until now:
//   on_pause(lo, hi)        the SOURCE starts exporting [lo, hi) -- called at the start of OP_MIG_EXPORT, before the
//                           memtable flush / iteration / file listing (OP_PAUSE itself carries no range and is ignored)
//   on_resume()             the coordinator resumed: an exporting range waits up to 30 s (env MIGAGENT_FENCE_GRACE_MS)
//                           for its drop; no drop = the move was aborted, the fence lapses
//   on_ownership(lo,hi,0)   the source dropped [lo, hi): fenced for good (until owned again or the process restarts)
//   on_ownership(lo,hi,1)   [lo, hi) is owned here (again): any fence on it is lifted
// A commit whose write batch touches a fenced range of the data CF (column family id 0) is refused: the caller rolls the
// transaction back and reports ER_LOCK_DEADLOCK (1213, "try restarting transaction"), so the client retries it at the new
// owner. Atomicity: a commit holds the gate's SHARED side across the engine commit; raising a fence takes the EXCLUSIVE
// side, so every commit that passed the check is in the memtable before the export flushes or reads it, and no later
// commit can reach the range. Ranges compare like the agent's: the key's first 8 bytes, zero padded, bytewise.
// The gate lock prefers writers (glibc), so a raise waits only for the commits already inside. Fences live in the
// process: (re)starting the agent clears them, and with the agent off the gate takes no lock and checks nothing
// (bit-identical to stock MyRocks).
class Rdb_migagent_commit_gate {
 public:
  Rdb_migagent_commit_gate();
  ~Rdb_migagent_commit_gate();
  Rdb_migagent_commit_gate(const Rdb_migagent_commit_gate &) = delete;
  Rdb_migagent_commit_gate &operator=(const Rdb_migagent_commit_gate &) = delete;
  // true = the batch writes a fenced range: the caller must roll back and report ER_LOCK_DEADLOCK
  bool refused(const rocksdb::WriteBatch &wb);

 private:
  bool m_locked = false;
};

}  // namespace myrocks
