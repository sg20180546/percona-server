/* migagent/migagent.h -- [migagent 2026-10-20] C ABI of libmigagent.so: the KV server's migration code
 * (serverclient/server_main.cc compiled with -DMIGAGENT_LIB) running inside a host process such as mysqld
 * (MyRocks). The host opens the DB, injects it here, and serves the coordinator's / peer agent's wire ops
 * on listen_addr:listen_port exactly as serverclient does. Keep this file C: the MyRocks glue carries a
 * verbatim copy (storage/rocksdb/migagent_api.h); MIGAGENT_ABI_VERSION guards drift.
 */
#ifndef MIGAGENT_H
#define MIGAGENT_H
#include <stddef.h>
#ifdef __cplusplus
extern "C" {
#endif
#define MIGAGENT_ABI_VERSION 1
typedef struct migagent_handle migagent_handle;
typedef struct migagent_options {
  int   abi_version;            /* must be MIGAGENT_ABI_VERSION */
  void* db;                     /* rocksdb::DB* (a TransactionDB = StackableDB); the agent calls GetRootDB() where needed */
  void* data_cf;                /* rocksdb::ColumnFamilyHandle* of "default"; "__system__" is never touched */
  const char* db_dir;           /* e.g. "/kg/sql53" (HDFS under the fork's default Env on the lab; POSIX in tests) */
  const char* listen_addr;      /* e.g. "192.168.88.53"; "0.0.0.0" allowed */
  int   listen_port;            /* the port the coordinator and the peer agent dial */
  int   shard_id;               /* logical shard id as the coordinator numbers it */
  const char* log_path;         /* agent log file; NULL = stderr */
  void (*log)(void* user, int level, const char* msg);   /* optional host logger (0 info, 1 warn, 2 error); NULL = own log */
  void (*on_pause)(void* user, const char* lo, size_t lo_len, const char* hi, size_t hi_len);  /* optional */
  void (*on_resume)(void* user);                                                                 /* optional */
  void (*on_ownership)(void* user, const char* lo, size_t lo_len, const char* hi, size_t hi_len, int owned); /* optional */
  void* user;
} migagent_options;
/* 0 = ok (listener threads running), else a positive errno-like code; *out is valid only on 0 */
int  migagent_start(const migagent_options* o, migagent_handle** out);
/* stop listening, finish or abort in-flight migrations, join threads, free state; does NOT close the DB */
void migagent_stop(migagent_handle* h);
/* for the host's compaction filter on the data CF: 0 = keep, 1 = drop (key not owned by this node and not retained
 * for an incoming PREPAREd range) -- identical semantics to the KV server's OwnershipFilter; must be very cheap */
int  migagent_key_decision(migagent_handle* h, const char* key, size_t len);
const char* migagent_version(void);   /* e.g. "migagent 1.0 (<git sha>)" */
#ifdef __cplusplus
}
#endif
#endif
