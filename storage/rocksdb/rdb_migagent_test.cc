// [migagent 2026-10-20] Standalone test of the MyRocks migration-agent glue (rdb_migagent.cc) against the
// stub library rdb_migagent_stub.cc. No MySQL: the fork's headers plus, since the write fence (2026-10-21), the
// fork's librocksdb for rocksdb::WriteBatch.
// Build + run: bash rdb_migagent_test.sh <fork include dir> <dir of the fork's librocksdb.so>
#include <dlfcn.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>

#include "./rdb_migagent.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/write_batch.h"

static std::string g_log;
static std::mutex g_log_mu;   // hooks log from the threads of 6f/6g
static void test_log(int level, const char* msg) {
  std::lock_guard<std::mutex> lk(g_log_mu);
  g_log += std::to_string(level) + ":" + msg + "\n";
}

static std::atomic<bool> g_thread_fail{false};
#define CHECK_T(c)                                                                     \
  do {                                                                                 \
    if (!(c)) { fprintf(stderr, "CHECK_T FAILED %s:%d: %s\n", __FILE__, __LINE__, #c); g_thread_fail = true; } \
  } while (0)

#define CHECK(c)                                                                    \
  do {                                                                              \
    if (!(c)) {                                                                     \
      fprintf(stderr, "CHECK FAILED %s:%d: %s\nlog so far:\n%s", __FILE__, __LINE__, \
              #c, g_log.c_str());                                                   \
      return 1;                                                                     \
    }                                                                               \
  } while (0)

// ---- [fence 2026-10-21] helpers ----
static std::string be64(uint64_t v) {   // 8-byte big endian, as the agent encodes a range bound (EncKey)
  std::string b(8, '\0');
  for (int i = 7; i >= 0; --i) { b[i] = static_cast<char>(v & 0xff); v >>= 8; }
  return b;
}
static std::string ik(uint32_t index_id, const std::string& rest) {   // a MyRocks key: 4-byte index id + key fields
  std::string b(4, '\0');
  for (int i = 3; i >= 0; --i) { b[i] = static_cast<char>(index_id & 0xff); index_id >>= 8; }
  return b + rest;
}
class FakeCF : public rocksdb::ColumnFamilyHandle {   // only the id and the comparator are read by WriteBatch::Put
 public:
  explicit FakeCF(uint32_t id) : m_id(id), m_name("cf" + std::to_string(id)) {}
  const std::string& GetName() const override { return m_name; }
  uint32_t GetID() const override { return m_id; }
  rocksdb::Status GetDescriptor(rocksdb::ColumnFamilyDescriptor*) override { return rocksdb::Status::NotSupported(); }
  const rocksdb::Comparator* GetComparator() const override { return rocksdb::BytewiseComparator(); }

 private:
  uint32_t m_id;
  std::string m_name;
};
static bool refused(const rocksdb::WriteBatch& wb) {   // what commit_no_binlog does
  myrocks::Rdb_migagent_commit_gate gate;
  return gate.refused(wb);
}
static int64_t now_ms() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
      .count();
}
static void sleep_ms(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

int main(int argc, char** argv) {
  if (argc < 2) { fprintf(stderr, "usage: %s <stub libmigagent.so>\n", argv[0]); return 2; }
  int fake_db = 1, fake_cf = 2;
  using myrocks::Rdb_migagent_params;

  // 1. port 0 = disabled: start is a no-op success, nothing loaded, the filter never drops.
  Rdb_migagent_params off = {&fake_db, &fake_cf, "/tmp/sql", 0, argv[1], "127.0.0.1", 0, "", &test_log};
  CHECK(myrocks::rdb_migagent_start(off));
  CHECK(!myrocks::rdb_migagent_should_drop(rocksdb::Slice("drop-me")));
  CHECK(std::string(myrocks::rdb_migagent_version()).empty());
  myrocks::rdb_migagent_stop();  // idempotent when nothing runs

  // 2. port set but no library path -> failure (plugin init would fail loudly).
  Rdb_migagent_params nolib = {&fake_db, &fake_cf, "/tmp/sql", 7900, "", "127.0.0.1", 0, "", &test_log};
  CHECK(!myrocks::rdb_migagent_start(nolib));
  CHECK(g_log.find("rocksdb_migagent_lib is empty") != std::string::npos);

  // 3. a library that does not exist -> failure with the dlopen error logged.
  Rdb_migagent_params badlib = {&fake_db, &fake_cf, "/tmp/sql", 7900, "/nonexistent/libmigagent.so",
                                "127.0.0.1", 0, "", &test_log};
  CHECK(!myrocks::rdb_migagent_start(badlib));
  CHECK(g_log.find("dlopen(") != std::string::npos);

  // 4. the stub: start -> version, decision (stub rule: keys starting with 'd' drop), stop -> never drops.
  Rdb_migagent_params good = {&fake_db, &fake_cf, "/tmp/sql", 7900, argv[1], "127.0.0.1", 1, "", &test_log};
  CHECK(myrocks::rdb_migagent_start(good));
  CHECK(std::string(myrocks::rdb_migagent_version()) == "migagent stub 1.0");
  CHECK(g_log.find("stub agent on 127.0.0.1:7900 shard=1 db_dir=/tmp/sql log_path=(host)") != std::string::npos);
  CHECK(myrocks::rdb_migagent_should_drop(rocksdb::Slice("d-moved-away")));
  CHECK(!myrocks::rdb_migagent_should_drop(rocksdb::Slice("k-still-mine")));
  CHECK(!myrocks::rdb_migagent_should_drop(rocksdb::Slice("")));
  CHECK(myrocks::rdb_migagent_start(good));  // second start while running: ignored, still true
  myrocks::rdb_migagent_stop();
  CHECK(!myrocks::rdb_migagent_should_drop(rocksdb::Slice("d-moved-away")));
  CHECK(std::string(myrocks::rdb_migagent_version()).empty());
  myrocks::rdb_migagent_stop();  // idempotent

  // 5. restart after stop reuses the mapped library (no dlclose) and works again; a log path is passed through.
  Rdb_migagent_params again = {&fake_db, &fake_cf, "/tmp/sql", 7901, argv[1], "0.0.0.0", 0, "/tmp/agent.log",
                               &test_log};
  CHECK(myrocks::rdb_migagent_start(again));
  {   // a390ca4: with a log file the glue passes no callback (the library would prefer it over the file)
    void* stub0 = dlopen(argv[1], RTLD_NOW | RTLD_NOLOAD);
    CHECK(stub0 != nullptr);
    auto log_path = reinterpret_cast<const char* (*)()>(dlsym(stub0, "stub_last_log_path"));
    auto has_cb = reinterpret_cast<int (*)()>(dlsym(stub0, "stub_last_has_log_callback"));
    CHECK(log_path != nullptr && has_cb != nullptr);
    CHECK(std::string(log_path()) == "/tmp/agent.log");
    CHECK(has_cb() == 0);
    CHECK(g_log.find("stub agent on 0.0.0.0:7901") == std::string::npos);   // nothing reached the host logger
  }
  CHECK(myrocks::rdb_migagent_should_drop(rocksdb::Slice("dd")));
  myrocks::rdb_migagent_stop();

  // 6. [fence 2026-10-21] the commit gate, driven through the hooks the agent fires.
  setenv("MIGAGENT_FENCE_GRACE_MS", "300", 1);   // read at start
  CHECK(myrocks::rdb_migagent_start(good));
  void* stub = dlopen(argv[1], RTLD_NOW | RTLD_NOLOAD);   // the object the glue mapped
  CHECK(stub != nullptr);
  auto fire_pause = reinterpret_cast<int (*)(const char*, size_t, const char*, size_t)>(dlsym(stub, "stub_fire_pause"));
  auto fire_resume = reinterpret_cast<int (*)()>(dlsym(stub, "stub_fire_resume"));
  auto fire_own =
      reinterpret_cast<int (*)(const char*, size_t, const char*, size_t, int)>(dlsym(stub, "stub_fire_ownership"));
  CHECK(fire_pause != nullptr && fire_resume != nullptr && fire_own != nullptr);
  const std::string lo = be64(5ull << 32), hi = be64(7ull << 32);   // index ids 5 and 6, as smoke_mig.sh numbers them
  FakeCF cf1(1);                                                   // MyRocks' dictionary CF is never fenced
  rocksdb::WriteBatch in5, in6, del5, mixed, dr_over, dr_long, out4, out7, in_cf1, short_key, dr_below, dr_above;
  CHECK(in5.Put(ik(5, "row"), "v").ok());
  CHECK(in6.Put(ik(6, ""), "v").ok());
  CHECK(del5.Delete(ik(5, "x")).ok());
  CHECK(mixed.Put(ik(4, "a"), "v").ok() && mixed.Put(&cf1, ik(6, "a"), "v").ok() && mixed.Delete(ik(6, "b")).ok());
  CHECK(dr_over.DeleteRange(ik(4, ""), ik(5, "x")).ok());                        // [4.., 5x) reaches index 5
  CHECK(dr_long.DeleteRange(ik(4, ""), be64(5ull << 32) + std::string(1, '\0')).ok());   // includes key 5.00000000
  CHECK(out4.Put(ik(4, "\xff\xff\xff\xff\xff"), "v").ok());
  CHECK(out7.Put(ik(7, ""), "v").ok());
  CHECK(in_cf1.Put(&cf1, ik(5, "row"), "v").ok());
  CHECK(short_key.Put(std::string(2, '\0'), "v").ok());
  CHECK(dr_below.DeleteRange(ik(4, ""), be64(5ull << 32)).ok());                 // ends exactly at the fence start
  CHECK(dr_above.DeleteRange(ik(7, ""), ik(8, "")).ok());                        // starts exactly at the fence end
  rocksdb::WriteBatch* fenced[] = {&in5, &in6, &del5, &mixed, &dr_over, &dr_long};
  rocksdb::WriteBatch* free_[] = {&out4, &out7, &in_cf1, &short_key, &dr_below, &dr_above};
  // 6a. no fence: everything commits
  for (auto* b : fenced) CHECK(!refused(*b));
  // 6b. export start raises [5, 7): exactly the batches touching it in CF 0 are refused
  CHECK(fire_pause(lo.data(), lo.size(), hi.data(), hi.size()) == 1);
  CHECK(g_log.find("write fence [0000000500000000,0000000700000000) raised") != std::string::npos);
  for (auto* b : fenced) CHECK(refused(*b));
  for (auto* b : free_) CHECK(!refused(*b));
  CHECK(fire_pause(nullptr, 0, nullptr, 0) == 1);   // OP_PAUSE (no range) changes nothing
  CHECK(refused(in5));
  // 6c. resume without a drop: kept for the grace period, then lapses (aborted move)
  CHECK(fire_resume() == 1);
  CHECK(refused(in5));
  sleep_ms(400);
  CHECK(!refused(in5));
  // 6d. a completed move: drop -> permanent, survives resume + grace
  CHECK(fire_pause(lo.data(), lo.size(), hi.data(), hi.size()) == 1);
  CHECK(fire_resume() == 1);
  CHECK(fire_own(lo.data(), lo.size(), hi.data(), hi.size(), 0) == 1);
  CHECK(g_log.find("permanent: range dropped") != std::string::npos);
  sleep_ms(400);
  CHECK(refused(in5) && refused(dr_long) && !refused(out7));
  CHECK(fire_resume() == 1);
  CHECK(refused(in5));
  // 6e. a drop without an export (the source learns it lost the range) also fences; owning a range lifts only it
  const std::string lo2 = be64(9ull << 32), hi2 = be64(10ull << 32);
  rocksdb::WriteBatch in9;
  CHECK(in9.Put(ik(9, "k"), "v").ok());
  CHECK(fire_own(lo2.data(), lo2.size(), hi2.data(), hi2.size(), 0) == 1);
  CHECK(refused(in9) && refused(in5));
  CHECK(fire_own(lo.data(), lo.size(), hi.data(), hi.size(), 1) == 1);
  CHECK(g_log.find("lifted: range owned here") != std::string::npos);
  CHECK(!refused(in5) && refused(in9));
  CHECK(fire_own(lo2.data(), lo2.size(), hi2.data(), hi2.size(), 1) == 1);
  CHECK(!refused(in9));
  // 6f. raising a fence waits for the commits already inside the gate (they land before the export reads)
  {
    std::atomic<bool> inside{false};
    std::thread t([&] {
      myrocks::Rdb_migagent_commit_gate gate;   // a commit in progress
      CHECK_T(!gate.refused(in5));
      inside = true;
      sleep_ms(300);
    });
    while (!inside) sleep_ms(1);
    const int64_t t0 = now_ms();
    CHECK(fire_pause(lo.data(), lo.size(), hi.data(), hi.size()) == 1);
    const int64_t waited = now_ms() - t0;
    t.join();
    CHECK(waited >= 250);
    CHECK(refused(in5));
    CHECK(fire_own(lo.data(), lo.size(), hi.data(), hi.size(), 1) == 1);
  }
#if defined(__GLIBC__)
  // 6g. writer preference: a commit that arrives while a raise waits goes after the raise and sees the fence
  {
    std::atomic<bool> inside{false};
    std::thread holder([&] {
      myrocks::Rdb_migagent_commit_gate gate;
      inside = true;
      sleep_ms(400);
    });
    while (!inside) sleep_ms(1);
    std::thread raiser([&] { fire_pause(lo2.data(), lo2.size(), hi2.data(), hi2.size()); });
    sleep_ms(100);   // the raiser is now queued behind the holder
    const bool late_refused = refused(in9);   // blocks until the raise is done (glibc: writers first)
    holder.join();
    raiser.join();
    CHECK(late_refused);
    CHECK(fire_own(lo2.data(), lo2.size(), hi2.data(), hi2.size(), 1) == 1);
  }
#endif
  // 6h. agent stopped: the gate is inert; a restart starts with no fence
  CHECK(fire_own(lo.data(), lo.size(), hi.data(), hi.size(), 0) == 1);
  CHECK(refused(in5));
  myrocks::rdb_migagent_stop();
  CHECK(!refused(in5));
  CHECK(myrocks::rdb_migagent_start(good));
  CHECK(!refused(in5));
  myrocks::rdb_migagent_stop();
  if (g_thread_fail) { fprintf(stderr, "a CHECK_T failed in a thread\nlog so far:\n%s", g_log.c_str()); return 1; }

  printf("rdb_migagent_test: PASS\n");
  return 0;
}
