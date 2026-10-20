// [migagent 2026-10-20] Standalone test of the MyRocks migration-agent glue (rdb_migagent.cc) against the
// stub library rdb_migagent_stub.cc. No MySQL, no RocksDB library: only the fork's headers (rocksdb/slice.h).
// Build + run: bash rdb_migagent_test.sh <path to the fork's include dir>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "./rdb_migagent.h"

static std::string g_log;
static void test_log(int level, const char* msg) { g_log += std::to_string(level) + ":" + msg + "\n"; }

#define CHECK(c)                                                                    \
  do {                                                                              \
    if (!(c)) {                                                                     \
      fprintf(stderr, "CHECK FAILED %s:%d: %s\nlog so far:\n%s", __FILE__, __LINE__, \
              #c, g_log.c_str());                                                   \
      return 1;                                                                     \
    }                                                                               \
  } while (0)

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
  CHECK(g_log.find("stub agent on 0.0.0.0:7901 shard=0 db_dir=/tmp/sql log_path=/tmp/agent.log") !=
        std::string::npos);
  CHECK(myrocks::rdb_migagent_should_drop(rocksdb::Slice("dd")));
  myrocks::rdb_migagent_stop();

  printf("rdb_migagent_test: PASS\n");
  return 0;
}
