/*
   [migagent 2026-10-20] In-process migration agent glue for MyRocks -- see
   rdb_migagent.h. Depends only on <dlfcn.h>, <pthread.h>, the C++ library,
   migagent_api.h and the fork's rocksdb/slice.h + rocksdb/write_batch.h, so it
   compiles standalone (no MySQL headers); the unit test rdb_migagent_test.cc
   dlopens a stub library and links the fork's librocksdb (WriteBatch walk).

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
#include <pthread.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <vector>

#include "./migagent_api.h"
#include "rocksdb/write_batch.h"

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

// ---- [migagent fence 2026-10-21] the write fence's state (see Rdb_migagent_commit_gate in the header) ----
enum Fence_state { FENCE_EXPORTING = 1, FENCE_RESUMED = 2, FENCE_MOVED = 3 };
struct Fence {
  std::string lo, hi;        // prefix-8 bounds as the agent encodes them (8-byte big endian)
  int state = FENCE_EXPORTING;
  int64_t resumed_ms = 0;    // FENCE_RESUMED: when on_resume came
  uint64_t refused = 0;      // commits refused on this range
};
// An exporting range whose drop never comes (aborted move) lapses this long after the resume. Env
// MIGAGENT_FENCE_GRACE_MS overrides it (read at rdb_migagent_start; the unit test uses a short one).
int64_t g_grace_ms = 30000;
// The gate: a commit holds the shared side across the engine commit, a fence change takes the exclusive side. A
// pthread rwlock that prefers writers on glibc: with the default (reader-preferring) lock a steady stream of
// overlapping commits could keep a fence raise waiting indefinitely. Never taken recursively (one per commit).
class Gate_lock {
 public:
  Gate_lock() {
    pthread_rwlockattr_t a;
    pthread_rwlockattr_init(&a);
#if defined(__GLIBC__)
    pthread_rwlockattr_setkind_np(&a, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
#endif
    pthread_rwlock_init(&m_l, &a);
    pthread_rwlockattr_destroy(&a);
  }
  ~Gate_lock() { pthread_rwlock_destroy(&m_l); }
  Gate_lock(const Gate_lock &) = delete;
  Gate_lock &operator=(const Gate_lock &) = delete;
  void lock() { pthread_rwlock_wrlock(&m_l); }
  void unlock() { pthread_rwlock_unlock(&m_l); }
  void lock_shared() { pthread_rwlock_rdlock(&m_l); }
  void unlock_shared() { pthread_rwlock_unlock(&m_l); }

 private:
  pthread_rwlock_t m_l;
};
Gate_lock g_gate;
std::vector<Fence> g_fences;                  // guarded by g_gate
std::atomic<int> g_fence_n{0};                // fences present (read under the shared lock)

int64_t mono_ms() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}
std::string key8(const char *k, size_t n) {   // the key's first 8 bytes, zero padded (the agent's prefix-8 rule)
  std::string b(8, '\0');
  memcpy(&b[0], k, n < 8 ? n : 8);
  return b;
}
bool fence_live(const Fence &f, int64_t now) {
  return f.state != FENCE_RESUMED || now - f.resumed_ms < g_grace_ms;
}
std::string hex8(const std::string &b) {
  static const char *d = "0123456789abcdef";
  std::string o;
  for (unsigned char c : b) { o += d[c >> 4]; o += d[c & 15]; }
  return o;
}
std::string fence_name(const Fence &f) { return "[" + hex8(f.lo) + "," + hex8(f.hi) + ")"; }
// under the exclusive lock: drop lapsed RESUMED fences, publish the count
void fence_gc_locked(int64_t now) {
  std::vector<Fence> keep;
  for (const Fence &f : g_fences) {
    if (fence_live(f, now)) keep.push_back(f);
    else host_log(1, "write fence " + fence_name(f) + " lapsed: resumed " + std::to_string(g_grace_ms) +
                         " ms ago without a drop (move aborted) -- writes allowed again (refused " +
                         std::to_string(f.refused) + ")");
  }
  g_fences.swap(keep);
  g_fence_n.store(static_cast<int>(g_fences.size()), std::memory_order_release);
}
Fence *fence_find_locked(const std::string &lo, const std::string &hi) {
  for (Fence &f : g_fences)
    if (f.lo == lo && f.hi == hi) return &f;
  return nullptr;
}

void on_pause_hook(void *, const char *lo, size_t lo_len, const char *hi, size_t hi_len) {
  if (lo == nullptr || hi == nullptr) return;   // OP_PAUSE carries no range; SQL is not paused by it
  const int64_t t0 = mono_ms();
  std::unique_lock<Gate_lock> lk(g_gate);   // waits for every commit that is inside the gate
  const int64_t waited = mono_ms() - t0;
  fence_gc_locked(mono_ms());
  const std::string l(lo, lo_len), h(hi, hi_len);
  Fence *f = fence_find_locked(l, h);
  if (f == nullptr) { g_fences.push_back(Fence{l, h}); f = &g_fences.back(); }
  f->state = FENCE_EXPORTING;
  g_fence_n.store(static_cast<int>(g_fences.size()), std::memory_order_release);
  host_log(0, "write fence " + fence_name(*f) + " raised (export start; in-flight commits drained in " +
                  std::to_string(waited) + " ms)");
}

void on_resume_hook(void *) {
  std::unique_lock<Gate_lock> lk(g_gate);
  const int64_t now = mono_ms();
  for (Fence &f : g_fences) {
    if (f.state == FENCE_EXPORTING) {
      f.state = FENCE_RESUMED;
      f.resumed_ms = now;
      host_log(0, "write fence " + fence_name(f) + " kept after resume, waiting for the drop (refused " +
                      std::to_string(f.refused) + " so far)");
    }
  }
}

void on_ownership_hook(void *, const char *lo, size_t lo_len, const char *hi, size_t hi_len, int owned) {
  if (lo == nullptr || hi == nullptr) return;   // hash groups: not used with MyRocks
  std::unique_lock<Gate_lock> lk(g_gate);
  fence_gc_locked(mono_ms());
  const std::string l(lo, lo_len), h(hi, hi_len);
  if (!owned) {
    Fence *f = fence_find_locked(l, h);
    if (f == nullptr) { g_fences.push_back(Fence{l, h}); f = &g_fences.back(); }
    f->state = FENCE_MOVED;
    host_log(0, "write fence " + fence_name(*f) + " permanent: range dropped (moved away; refused " +
                    std::to_string(f->refused) + " so far)");
  } else {
    std::vector<Fence> keep;
    for (const Fence &f : g_fences) {
      if (f.hi <= l || h <= f.lo) keep.push_back(f);   // no overlap with the range owned again
      else host_log(0, "write fence " + fence_name(f) + " lifted: range owned here");
    }
    g_fences.swap(keep);
  }
  g_fence_n.store(static_cast<int>(g_fences.size()), std::memory_order_release);
}

// Walks a write batch: does any data-CF key fall in a live fence? (called under the shared lock)
class Fence_check : public rocksdb::WriteBatch::Handler {
 public:
  explicit Fence_check(int64_t now) : m_now(now) {}
  Fence *hit = nullptr;
  rocksdb::Status PutCF(uint32_t cf, const rocksdb::Slice &k, const rocksdb::Slice &) override { return chk(cf, k); }
  rocksdb::Status DeleteCF(uint32_t cf, const rocksdb::Slice &k) override { return chk(cf, k); }
  rocksdb::Status SingleDeleteCF(uint32_t cf, const rocksdb::Slice &k) override { return chk(cf, k); }
  rocksdb::Status MergeCF(uint32_t cf, const rocksdb::Slice &k, const rocksdb::Slice &) override { return chk(cf, k); }
  rocksdb::Status PutEntityCF(uint32_t cf, const rocksdb::Slice &k, const rocksdb::Slice &) override {
    return chk(cf, k);
  }
  rocksdb::Status PutBlobIndexCF(uint32_t cf, const rocksdb::Slice &k, const rocksdb::Slice &) override {
    return chk(cf, k);
  }
  rocksdb::Status DeleteRangeCF(uint32_t cf, const rocksdb::Slice &b, const rocksdb::Slice &e) override {
    if (cf != 0 || hit != nullptr) return rocksdb::Status::OK();
    // The smallest prefix in [b, e) is key8(b). The largest is key8(e) when e is longer than 8 bytes (keys between
    // key8(e) and e share that prefix), otherwise the prefixes stay below key8(e).
    const std::string kb = key8(b.data(), b.size()), ke = key8(e.data(), e.size());
    const bool e_long = e.size() > 8;
    for (Fence &f : g_fences)
      if (fence_live(f, m_now) && kb < f.hi && (e_long ? f.lo <= ke : f.lo < ke)) { hit = &f; break; }
    return rocksdb::Status::OK();
  }
  rocksdb::Status MarkBeginPrepare(bool) override { return rocksdb::Status::OK(); }
  rocksdb::Status MarkEndPrepare(const rocksdb::Slice &) override { return rocksdb::Status::OK(); }
  rocksdb::Status MarkNoop(bool) override { return rocksdb::Status::OK(); }
  rocksdb::Status MarkRollback(const rocksdb::Slice &) override { return rocksdb::Status::OK(); }
  rocksdb::Status MarkCommit(const rocksdb::Slice &) override { return rocksdb::Status::OK(); }
  rocksdb::Status MarkCommitWithTimestamp(const rocksdb::Slice &, const rocksdb::Slice &) override {
    return rocksdb::Status::OK();
  }
  bool Continue() override { return hit == nullptr; }   // first hit decides

 private:
  rocksdb::Status chk(uint32_t cf, const rocksdb::Slice &k) {
    if (cf != 0 || hit != nullptr) return rocksdb::Status::OK();   // the dictionary CF is never fenced
    const std::string k8 = key8(k.data(), k.size());
    for (Fence &f : g_fences)
      if (fence_live(f, m_now) && f.lo <= k8 && k8 < f.hi) { hit = &f; break; }
    return rocksdb::Status::OK();
  }
  int64_t m_now;
};

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

  {   // [migagent fence] a (re)started agent owns every range again: no fence survives a restart of the agent
    std::unique_lock<Gate_lock> lk(g_gate);
    g_fences.clear();
    g_fence_n.store(0, std::memory_order_release);
    const char *g = getenv("MIGAGENT_FENCE_GRACE_MS");
    const long v = (g != nullptr) ? strtol(g, nullptr, 10) : 0;
    g_grace_ms = (v > 0) ? v : 30000;
  }

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
  o.on_pause = &on_pause_hook;           // [migagent fence 2026-10-21] the write fence's inputs
  o.on_resume = &on_resume_hook;
  o.on_ownership = &on_ownership_hook;
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

Rdb_migagent_commit_gate::Rdb_migagent_commit_gate() {
  if (!g_active.load(std::memory_order_acquire)) return;   // agent off: no lock, no check
  g_gate.lock_shared();
  m_locked = true;
}

Rdb_migagent_commit_gate::~Rdb_migagent_commit_gate() {
  if (m_locked) g_gate.unlock_shared();
}

bool Rdb_migagent_commit_gate::refused(const rocksdb::WriteBatch &wb) {
  if (!m_locked || g_fence_n.load(std::memory_order_acquire) == 0) return false;
  Fence_check chk(mono_ms());
  const rocksdb::Status s = wb.Iterate(&chk);
  if (!s.ok()) {   // an unknown record type: let the commit through (the engine validates it) and say so once
    static std::atomic<bool> warned{false};
    if (!warned.exchange(true)) host_log(1, "write fence: batch walk failed (" + s.ToString() + "); commit not checked");
    return false;
  }
  if (chk.hit == nullptr) return false;
  // under the shared lock: a relaxed per-fence counter is enough (only logged)
  const uint64_t n = __atomic_add_fetch(&chk.hit->refused, 1, __ATOMIC_RELAXED);
  if (n <= 3 || (n & (n - 1)) == 0)
    host_log(0, "write fence " + fence_name(*chk.hit) + " refused a commit (" + std::to_string(n) +
                    " so far): ER_LOCK_DEADLOCK, the client retries at the new owner");
  return true;
}

const char *rdb_migagent_version() {
  return g_active.load(std::memory_order_acquire) ? g_state.version_str.c_str()
                                                  : "";
}

}  // namespace myrocks
