# DB-Eval Project — Claude Context

## Project Overview

Database benchmarking and profiling framework comparing storage engines (Percona InnoDB vs Percona MyRocks) under OLTP and OLAP workloads. Research goal: quantify MyRocks' MVCC/versioning overhead in OLAP (full table scan) workloads via CPU and I/O profiling.

## Research Context

- **Hypothesis**: In OLAP workloads, MyRocks spends a significant fraction of CPU time in version-traversal / MVCC visibility functions (e.g., `rocksdb::DBIter::FindNextUserEntry`, `rocksdb::GetContext::SaveValue`) compared to OLTP workloads. This is due to LSM-tree design requiring per-row version checks during full table scans.
- **Current phase**: Setting up CPU and I/O profiling of MyRocks under TPC-C (OLTP) and TPC-H (OLAP). Results to be presented to professor.
- **Engine choice**: Percona Server 8.4 with MyRocks (`ha_rocksdb.so`) — this IS MyRocks; no need to switch to a different distribution.

## Environment

- **Platform**: Ubuntu (Linux), x86_64, NVMe SSD at `/mnt/nvme`
- **Engines**: Percona InnoDB, Percona MyRocks (same Percona Server binary, different storage engine plugin)
- **Memory**: 16GB cgroup limit for both engines
  - InnoDB: 12GB buffer pool + O_DIRECT
  - MyRocks: 512MB RocksDB block cache + OS page cache
- **MySQL version**: 8.4.7 (Percona Server)
- **Sockets**: `/tmp/mysql_percona_myrocks.sock`, `/tmp/mysql_percona_innodb.sock`

## Directory Structure

```
db-eval/
  common/config/
    env.sh                  # All benchmark parameters (source this first)
    my-percona-myrocks.cnf  # MyRocks MySQL config (rocksdb_perf_context_level=1)
    my-percona-innodb.cnf   # InnoDB MySQL config
  scripts/
    mysql-control.sh        # Start/stop MySQL instances
    monitor.sh              # pidstat/iostat/mpstat/vmstat monitoring functions
    run-benchmark.sh        # Top-level benchmark runner
  tpcc/
    run.sh                  # TPC-C runner (args: <engine> <threads> <result_dir>)
    prepare.sh
  tpch-olap/
    run.sh                  # TPC-H runner (args: <engine> <result_dir>)
    prepare.sh
    queries/mysql/          # 1.sql through 22.sql
    schema/create_tables.sql
  results/
    <benchmark>/<engine>/<timestamp>/   # All result output goes here
```

## Key Config Values (env.sh)

- `TPCC_WAREHOUSES=2000`, `TPCC_DURATION=600`
- `TPCH_SCALE_FACTOR=100` (SF100, ~100GB data)
- `MYROCKS_BLOOM_FILTER="off"` (disabled after TPC-C analysis)
- `CGROUP_MEMORY_LIMIT="16G"`

## Result File Conventions

Each benchmark run produces in its result directory:
- `tpch_summary.csv` — per-query best times (cold, warm1, warm2, min_time, status)
- `tpch_query_times.csv` — raw per-run times
- `tpch_stats.txt` — human-readable summary
- `tpch_size_metrics.csv` — DB size info
- `tpch_{pidstat,iostat,mpstat,vmstat}.txt` — raw monitor output
- `tpch_resource_summary.csv` — parsed monitor summary (avg/min/max/stddev)

## Previous Benchmark Results (comparison/)

Full details in `comparison/InnoDB_vs_MyRocks_Analysis_20260205.md` and `comparison/InnoDB_vs_MyRocks_Summary.md`.

### Test Environment
- Intel i7-13700K (16 cores, 24 threads), 64GB RAM, 16GB cgroup limit, NVMe SSD
- Percona Server 8.4.7-7

### Sysbench (500M rows, ~100GB)

| Workload | 32 threads | Winner |
|----------|------------|--------|
| Read-Only | InnoDB 25,439 TPS vs MyRocks 13,999 TPS | **InnoDB (1.8x)** |
| Write-Only | InnoDB 4,229 TPS vs MyRocks 33,259 TPS | **MyRocks (7.9x)** |
| Read/Write | InnoDB 4,233 TPS vs MyRocks 9,496 TPS | **MyRocks (2.2x)** |

### TPC-C / OLTP (2000 warehouses, bloom filter ON)

InnoDB beats MyRocks at all thread counts (~1.1–1.3x). Root cause: bloom filters consume 67% of 512MB block cache, evicting hot data.

| Threads | InnoDB tpmC | MyRocks tpmC |
|---------|-------------|--------------|
| 1 | 7,344 | 5,638 |
| 16 | 63,089 | 57,187 |
| 32 | 82,744 | 65,162 |

**Bloom filter impact (TPC-C):** Disabling bloom filters improves MyRocks to 10,873 tpmC (1T) and 168,839 tpmC (32T) — far exceeding InnoDB. Bloom ON loses 48–61% throughput because 342MB of 512MB cache is consumed by filter blocks.

### TPC-H / OLAP (SF100, ~100GB, bloom filter OFF)

InnoDB dominates. MyRocks timed out on 13/22 queries vs InnoDB's 12/22.

| Query | InnoDB (s) | MyRocks (s) | Ratio |
|-------|-----------|------------|-------|
| Q1 (lineitem full scan + agg) | 512.5 | 582.9 | 1.14x InnoDB |
| Q2 (5-table join) | 5.3 | 116.1 | **22x InnoDB** |
| Q6 (lineitem scan + filter) | 116.5 | 229.3 | **2.0x InnoDB** |
| Q22 (small table + NOT EXISTS) | 34.0 | 23.2 | **0.68x MyRocks wins** |

Geometric mean over 9 shared queries: InnoDB 115.1s vs MyRocks 191.5s (**40% faster**).

**Why InnoDB wins TPC-H:**
1. B-tree sequential scan is more efficient than LSM multi-level merge reads
2. InnoDB 12GB buffer pool (user-space, zero syscall) vs MyRocks OS page cache (kernel transitions per block)
3. LSM per-lookup overhead multiplies across millions of join iterations (Q2: 22x)
4. MyRocks only wins Q22 — small table scan + existence check, where compact storage wins

**Storage:** MyRocks 179 GB vs InnoDB 240 GB (**25% smaller** despite no compression)

### ClickBench / OLAP (single wide table, 100M rows, bloom filter OFF)

MyRocks dominates. **2.55x geomean speedup** over InnoDB on 32 shared queries.

- MyRocks: 33/43 completed, total best time 4,689s
- InnoDB: 36/43 completed, total best time 12,292s
- MyRocks 32% smaller storage (59 GB vs 87 GB)

**Why MyRocks wins ClickBench:** Single-table queries with no joins — compact storage means fewer pages to read. Full-scan aggregations over sequential SST files outperform InnoDB's larger B-tree pages. InnoDB only wins on highly selective PK-prefix filtered queries.

**Key contrast (TPC-H vs ClickBench):** MyRocks struggles with TPC-H *multi-table joins* (LSM per-lookup penalty multiplies), but wins ClickBench *single-table scans* (compact storage + sequential SST reads). The bottleneck in TPC-H OLAP for MyRocks is likely the version-traversal overhead on the massive lineitem table — which is what the current profiling work aims to confirm.

---

## Known Issues / History

- First InnoDB TPC-H run (`20260205_203027`) was on an empty DB — all queries ~0.004s. Use `20260206_181826` instead.
- Bloom filter was disabled (`MYROCKS_BLOOM_FILTER="off"`) after TPC-C analysis showed it complicated fair comparison.
- `rocksdb_perf_context_level = 1` currently set in my-percona-myrocks.cnf (count only). Bump to `3` for profiling runs.

## Profiling Plan (Current Work)

### Goal
Show that version-related RocksDB functions have meaningfully higher CPU share during OLAP (TPC-H) vs OLTP (TPC-C).

### Three-layer approach

**Layer 1 — CPU Flamegraphs (`perf` + FlameGraph)**
```bash
# Setup (one-time)
sudo apt install linux-tools-$(uname -r) linux-tools-generic
git clone https://github.com/brendangregg/FlameGraph ~/FlameGraph

# During workload
MYSQLD_PID=$(cat /tmp/mysql_percona_myrocks.pid)
sudo perf record -F 99 -p $MYSQLD_PID --call-graph dwarf -o perf_olap.data -- sleep 60
sudo perf script -i perf_olap.data \
    | ~/FlameGraph/stackcollapse-perf.pl \
    | ~/FlameGraph/flamegraph.pl > flamegraph_myrocks_olap.svg
```

Key functions to look for (MVCC/version overhead):
- `rocksdb::DBIter::FindNextUserEntry` — skips invisible versions during scan
- `rocksdb::GetContext::SaveValue` — per-key version visibility check
- `rocksdb::DBImpl::GetSnapshot` — snapshot acquisition
- `ha_rocksdb::rnd_next` — full table scan entry point
- `rocksdb::Version::Get`, `rocksdb::TableCache::Get` — LSM level reads

**Layer 2 — RocksDB Perf Context**

Bump config to level 3, then per query:
```sql
SET SESSION rocksdb_perf_context_level = 3;
-- run query --
SELECT * FROM information_schema.rocksdb_perf_context;
```

Key metrics:
- `internal_key_skipped_count` — versions skipped per scan (primary MVCC metric)
- `internal_delete_skipped_count` — tombstones traversed
- `get_snapshot_time` — snapshot acquisition CPU time
- `block_read_count` / `block_read_time` — read amplification

Derived metric: `version_overhead_ratio = internal_key_skipped_count / rows_examined`

**Layer 3 — I/O (already in monitor.sh)**

Before each OLAP run, flush memtable:
```sql
SET GLOBAL rocksdb_force_flush_memtable_now = 1;
```

Also run:
```bash
sudo perf stat -p $MYSQLD_PID -e cycles,instructions,cache-misses,LLC-load-misses sleep 60
```

### TPC-H queries to isolate for profiling
- Q1, Q6 — pure full table scans (lineitem, ~600M rows at SF100)
- Q12, Q14 — join + scan
- Q19 — range scan

### Protocol per query
1. Flush memtable → drop OS page cache → enable perf context level 3
2. Run `perf record` concurrently for query duration
3. Collect `rocksdb_perf_context` after query
4. Generate flamegraph

### Notes
- `--call-graph dwarf` does not require recompiling MySQL (no frame pointer flag needed)
- `rocksdb_perf_context_level = 3` adds ~5-10% overhead — use only for profiling runs, not final benchmark numbers
- For maximizing snapshot overhead visibility, consider `SET SESSION transaction_isolation = 'REPEATABLE-READ'` during profiling (current default is READ-COMMITTED which reduces it slightly)
- Perf data with dwarf can be 1-5GB per 60s recording
