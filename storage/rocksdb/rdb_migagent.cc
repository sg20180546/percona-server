/*
   [migagent 2026-10-20] In-process migration agent glue for MyRocks -- see
   rdb_migagent.h. Depends only on <dlfcn.h>, <atomic>, <string>,
   migagent_api.h and rocksdb/slice.h so it compiles standalone (no MySQL
   headers); the unit test rdb_migagent_test.cc dlopens a stub library.

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

#include "./rdb_migagent.h"

#include <dlfcn.h>

#include <atomic>
#include <cstdio>
#include <cstring>
#include <string>

#include "./migagent_api.h"

namespace myrocks {

namespace {

struct Rdb_migagent_state {
  void *lib = nullptr;
  std::atomic<migagent_handle *> handle{nullptr};
  int (*start)(const migagent_options *, migagent_handle **) = nullptr;
  void (*stop)(migagent_handle *) = nullptr;
  int (*decide)(migagent_handle *, const char *, size_t) = nullptr;
  const char *(*version)(void) = nullptr;
  // Copies of the option strings: migagent_start reads them during the call
  // only, but keeping them alive costs nothing and protects a library that
  // stores the pointers.
  std::string version_str, addr, datadir, log_path;
  void (*host_log)(int level, const char *msg) = nullptr;
};

Rdb_migagent_state g_state;
// Read by every compaction-filter key (acquire); written by start/stop only.
std::atomic<bool> g_active{false};

void host_log(int level, const std::string &msg) {
  const std::string line = "[migagent] " + msg;   // the library's own lines already carry their tag
  if (g_state.host_log != nullptr) {
    g_state.host_log(level, line.c_str());
  } else {
    fprintf(stderr, "%s\n", line.c_str());
  }
}

// migagent_options::log trampoline: the agent's (void* user, level, msg) to
// the host's (level, msg).
void log_trampoline(void *user, int level, const char *msg) {
  auto *st = static_cast<Rdb_migagent_state *>(user);
  if (st != nullptr && st->host_log != nullptr && msg != nullptr) {
    st->host_log(level, msg);
  }
}

template <typename F>
bool load_sym(void *lib, const char *name, F *out) {
  dlerror();  // clear
  void *p = dlsym(lib, name);
  const char *err = dlerror();
  if (p == nullptr || err != nullptr) {
    host_log(2, std::string("libmigagent: dlsym(") + name + ") failed: " +
                    (err != nullptr ? err : "null symbol"));
    return false;
  }
  *out = reinterpret_cast<F>(p);
  return true;
}

}  // namespace

bool rdb_migagent_start(const Rdb_migagent_params &p) {
  if (p.port == 0) return true;  // disabled: nothing loaded, nothing to undo
  g_state.host_log = p.log;
  if (g_active.load(std::memory_order_acquire)) {
    host_log(1, "migration agent already running; start ignored");
    return true;
  }
  if (p.lib == nullptr || *p.lib == '\0') {
    host_log(2, "rocksdb_migagent_port is set but rocksdb_migagent_lib is empty");
    return false;
  }
  if (p.db == nullptr || p.data_cf == nullptr) {
    host_log(2, "migration agent: DB or data column family handle is null");
    return false;
  }
  if (g_state.lib == nullptr) {
    // RTLD_LOCAL: the agent's symbols never leak into mysqld's namespace.
    // RTLD_NOW: an unresolved reference (e.g. a librocksdb.so.7 ABI mismatch)
    // fails here, at plugin init, not in a migration. The library's own
    // DT_NEEDED on librocksdb.so.7 binds to the copy mysqld already loaded.
    g_state.lib = dlopen(p.lib, RTLD_NOW | RTLD_LOCAL);
    if (g_state.lib == nullptr) {
      const char *err = dlerror();
      host_log(2, std::string("dlopen(") + p.lib + ") failed: " +
                      (err != nullptr ? err : "unknown error"));
      return false;
    }
    if (!load_sym(g_state.lib, "migagent_start", &g_state.start) ||
        !load_sym(g_state.lib, "migagent_stop", &g_state.stop) ||
        !load_sym(g_state.lib, "migagent_key_decision", &g_state.decide) ||
        !load_sym(g_state.lib, "migagent_version", &g_state.version)) {
      return false;
    }
  }
  const char *ver = g_state.version();
  g_state.version_str = (ver != nullptr) ? ver : "";
  if (g_state.version_str.rfind("migagent ", 0) != 0) {
    host_log(2, "libmigagent: unexpected migagent_version() = '" +
                    g_state.version_str + "'");
    return false;
  }
  g_state.addr = (p.addr != nullptr && *p.addr != '\0') ? p.addr : "0.0.0.0";
  g_state.datadir = (p.datadir != nullptr) ? p.datadir : "";
  g_state.log_path = (p.log_path != nullptr) ? p.log_path : "";

  migagent_options o;
  memset(&o, 0, sizeof(o));
  o.abi_version = MIGAGENT_ABI_VERSION;  // checked by the library against its own
  o.db = p.db;
  o.data_cf = p.data_cf;
  o.db_dir = g_state.datadir.c_str();
  o.listen_addr = g_state.addr.c_str();
  o.listen_port = static_cast<int>(p.port);
  o.shard_id = static_cast<int>(p.shard_id);
  // Empty log path = route the agent's lines through the host logger (the
  // mysqld error log); a path = the agent writes its own file. The library
  // gives the callback precedence over log_path, so pass the callback only
  // when no file is wanted (the glue's own messages always use the host log).
  o.log_path = g_state.log_path.empty() ? nullptr : g_state.log_path.c_str();
  o.log = (p.log != nullptr && g_state.log_path.empty()) ? &log_trampoline : nullptr;
  o.on_pause = nullptr;
  o.on_resume = nullptr;
  o.on_ownership = nullptr;
  o.user = &g_state;

  migagent_handle *h = nullptr;
  const int rc = g_state.start(&o, &h);
  if (rc != 0 || h == nullptr) {
    host_log(2, "migagent_start(" + g_state.addr + ":" + std::to_string(p.port) +
                    ") failed: rc=" + std::to_string(rc));
    return false;
  }
  g_state.handle.store(h, std::memory_order_release);
  g_active.store(true, std::memory_order_release);
  return true;
}

void rdb_migagent_stop() {
  // Clear the fast-path flag FIRST so new compaction-filter calls stop asking,
  // then detach the handle (a filter call that already passed the flag sees a
  // null handle and answers "keep"), then let migagent_stop join the agent's
  // threads and free the handle. migagent_key_decision only null-checks its
  // handle and reads process-wide ownership state that outlives stop.
  if (!g_active.exchange(false, std::memory_order_acq_rel)) return;
  migagent_handle *h = g_state.handle.exchange(nullptr, std::memory_order_acq_rel);
  if (g_state.stop != nullptr && h != nullptr) {
    g_state.stop(h);
  }
  // no dlclose: keep the library mapped (threads unwinding, stable strings)
}

bool rdb_migagent_should_drop(const rocksdb::Slice &key) {
  if (!g_active.load(std::memory_order_acquire)) return false;
  migagent_handle *h = g_state.handle.load(std::memory_order_acquire);
  if (h == nullptr) return false;
  return g_state.decide(h, key.data(), key.size()) != 0;
}

const char *rdb_migagent_version() {
  return g_active.load(std::memory_order_acquire) ? g_state.version_str.c_str()
                                                  : "";
}

}  // namespace myrocks
