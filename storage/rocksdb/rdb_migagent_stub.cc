// [migagent 2026-10-20] 20-line FAKE libmigagent.so implementing migagent_api.h, for
// rdb_migagent_test.cc only (never built into mysqld). Decision rule: keys starting with 'd' are dropped.
//   g++ -std=c++17 -fPIC -shared rdb_migagent_stub.cc -o libmigagent_stub.so
#include <cstdio>
#include <cstring>

#include "./migagent_api.h"

struct migagent_handle { int started; int starts; };
static migagent_handle g_stub_handle = {0, 0};
extern "C" int migagent_start(const migagent_options* o, migagent_handle** out) {
  if (o == nullptr || out == nullptr || o->abi_version != MIGAGENT_ABI_VERSION || o->db == nullptr ||
      o->data_cf == nullptr || o->listen_port <= 0 || o->listen_addr == nullptr) return 22;  // EINVAL
  if (o->log != nullptr) {
    char line[256];
    snprintf(line, sizeof(line), "stub agent on %s:%d shard=%d db_dir=%s log_path=%s", o->listen_addr,
             o->listen_port, o->shard_id, o->db_dir ? o->db_dir : "(null)", o->log_path ? o->log_path : "(host)");
    o->log(o->user, 0, line);
  }
  g_stub_handle.started = 1; g_stub_handle.starts++; *out = &g_stub_handle; return 0;
}
extern "C" void migagent_stop(migagent_handle* h) { if (h) h->started = 0; }
extern "C" int migagent_key_decision(migagent_handle* h, const char* key, size_t len) {
  return (h != nullptr && h->started && len > 0 && key[0] == 'd') ? 1 : 0;
}
extern "C" const char* migagent_version(void) { return "migagent stub 1.0"; }
