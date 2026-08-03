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
  mise.toml                     # Pins python=3.12; auto-creates/manages .venv on `mise` activation
  requirements.txt               # python-pptx, pandas, matplotlib — the only 3rd-party deps db-eval's .py files need
  .venv/                         # mise-managed venv (see "Python Environment" below)
  create_presentation.py         # InnoDB vs MyRocks benchmark deck (TPC-C/TPC-H/ClickBench/Sysbench results)
  create_cemu_presentation.py    # CEMU MVCC filter lab-meeting deck (current work, updated most recently)
  common/config/
    env.sh                  # All benchmark parameters (source this first)
    my-percona-myrocks.cnf  # MyRocks MySQL config (rocksdb_perf_context_level=1)
    my-percona-innodb.cnf   # InnoDB MySQL config
  scripts/
    mysql-control.sh        # Start/stop MySQL instances
    monitor.sh              # pidstat/iostat/mpstat/vmstat monitoring functions
    run-benchmark.sh        # Top-level benchmark runner
    plot-results.py         # matplotlib plotting for benchmark results (pandas + matplotlib)
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

## Python Environment (Presentation & Plotting Scripts)

- Managed by `mise` — `mise.toml` pins `python = "3.12"` and sets `env._.python.venv = {path = ".venv", create = true}`, so `.venv` is created/activated automatically whenever `mise` is active in this directory. No manual `python -m venv` needed.
- Dependencies are pinned in `requirements.txt` (`python-pptx`, `pandas`, `matplotlib` — verified against every `import`/`from` in db-eval's `.py` files, nothing more). Install with:
  ```bash
  mise exec -- python -m pip install -r requirements.txt
  ```
- Run scripts via `mise exec -- python <script>.py` (or activate `.venv` directly) so they use the pinned venv, not the global mise Python.
- **In a normal interactive terminal, `mise exec --` is not required** — `~/.zshrc` sources `mise activate zsh`, which hooks `chpwd`/`precmd` to auto-run `mise hook-env` whenever you `cd` into `db-eval/` or a subdirectory. Just `cd db-eval && python script.py` picks up `.venv` automatically. The `mise exec --` (or `eval "$(mise hook-env)"`) prefix is only needed for tooling that runs commands non-interactively without a real `cd`/prompt cycle (e.g. Claude Code's Bash tool) — verified empirically: `env | grep mise` there shows `__MISE_ZSH_CHPWD_RAN=0`, meaning the auto-activation hook never fired even though `pwd` was already inside `db-eval/`.
- **Gotcha**: if `mise` floats the pinned `3.12` to a new patch version and removes the old install (e.g. 3.12.12 → 3.12.13), the existing `.venv/bin/python` symlink dangles. `pip`/`python` inside `.venv` then silently fail or fall through to whatever global Python is on `PATH` — no error surfaces until an import fails. Fix: `rm -rf .venv && mise install` (recreates `.venv` per `mise.toml`), then reinstall from `requirements.txt`.

### `create_cemu_presentation.py` notes
- Both bar/line chart helpers (`add_bar_chart`, `add_line_chart`) require explicit `x_axis_title` and `y_axis_title` args — python-pptx does not label axes by default, and it's easy to add a chart with only a y-axis title. Always pass both.
- The flamegraph comparison slide uses `add_image_or_placeholder()`: it embeds a PNG if one exists at the expected path, otherwise draws a placeholder box naming the source SVG and the `rsvg-convert` command to produce the PNG. **python-pptx cannot embed SVG directly** (its image support is PIL-backed; PIL has no SVG decoder) — flamegraphs must be converted from `.svg` to `.png` before they'll render in the deck. Source SVGs for the primary comparison pair live at `results/profiling/htap/percona-myrocks-csd/{20260629_125215,20260629_165929}/flamegraph_htap_run1.svg`.

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

---

## FLAX/NVMeVirt HTAP Harness (separate sub-project, `percona-myrocks-nvmevirt` engine)

Same HTAP profiling machinery as the CEMU thread above (`profiling/profile-htap.sh`,
`scripts/mysql-control.sh`), reused for a **different, independent research thread** —
see `~/Projects/snu-csl/CLAUDE.md` and `FLAX/CLAUDE.md` for what FLAX itself is. Two
deployment environments, each with its own env-override file, both hooked into
`common/config/env.sh` via the same mechanism as `CEMU_VM_ENV`:

| Environment | Env override file | Runner script | Scale |
|---|---|---|---|
| QEMU sandbox guest | `env-flax-sandbox.sh` (set `FLAX_SANDBOX_ENV`) | `run-flax-sandbox-htap.sh` | Scaled down (10k rows/table, 8 OLTP threads, 2 LLTs) |
| Bare metal (`s1`) | `env-flax-baremetal.sh` (set `FLAX_BAREMETAL_ENV`) | `run-flax-baremetal-htap.sh` | Full AIDE-paper scale (100k rows/table, 24 threads, 4 LLTs) — `env.sh`'s own unmodified defaults |

### Gotchas specific to this harness

- **`env.sh` sets `HTAP_QUERY_TIMEOUT`/`HTAP_OLAP_RUNS`/etc. with a plain unconditional
  `export`, not `${VAR:-default}`.** A pre-export from a calling script (e.g. trying to
  temporarily scale down a run) gets silently clobbered the moment `env.sh` is sourced
  — confirmed 2026-07-29 when an overnight run's own scale-down override had no visible
  effect and proceeded at full scale unnoticed until checking the logged OLTP duration.
  Any such override **must** live inside whichever env-override file is sourced *last*
  (`env-flax-sandbox.sh`/`env-flax-baremetal.sh`, sourced at the very end of `env.sh`),
  never in the shell that calls into these scripts.
- **`sysbench-htap/prepare.sh`'s own `mysql` calls have no explicit `-u`**, so they
  default to the client's OS-username-as-MySQL-username fallback. `--initialize-insecure`
  only creates a passwordless `root@localhost`, so running `prepare.sh` as anyone other
  than root fails with `Access denied` before ever reaching the actual sysbench prepare
  step. `run-flax-baremetal-htap.sh` wraps this specific call in `sudo -E` (root
  identity, but preserving the calling shell's exported variables, which plain `sudo`
  would otherwise reset along with `$HOME`).
- **`mysql-control.sh`'s own `stop_mysql()` has the identical no-explicit-`-u` issue**
  (`mysqladmin ... shutdown`) — not yet fixed at the source, it just falls back to a
  force-kill after a ~30s timeout, which works but is noisy in logs. Known, non-blocking.
- **`percona-myrocks-nvmevirt`'s debug-build `mysqld` (8.4.10-10-debug) can crash on
  `ANALYZE TABLE`** with an internal assertion
  (`MDL_checker::is_read_locked` in `dd::cache::Dictionary_client::acquire`) when a
  table already has histogram statistics from a prior `ANALYZE ... UPDATE HISTOGRAM`
  cycle against the same long-lived datadir — confirmed to trigger on as few as two
  consecutive cycles in practice, and `ANALYZE TABLE ... DROP HISTOGRAM` hits the exact
  same assertion (not a viable workaround). Only fix found: a genuinely fresh
  `mysql-control.sh <engine> init` (which does `rm -rf $DATADIR` + fresh
  `--initialize-insecure`) before the next `ANALYZE TABLE` runs — there is no way to
  keep a long-lived FLAX datadir across many profiling sessions without eventually
  hitting this.
- **Root-caused 2026-08-03 (previously just worked around): plain `rm -rf $DATADIR`,
  and even a bare `mkfs.ext4 -F` reformat, are not reliably sufficient to avoid a
  spurious `InnoDB` redo-log "decryption" abort on the next `mysqld` startup**
  (`no keyring configured` — a red herring, nothing is actually encrypted). Root cause:
  `mkfs.ext4` defers inode-table/journal zeroing to a background `ext4lazyinit` kernel
  thread that runs *after* mount, not during `mkfs` itself — `mysqld
  --initialize-insecure` (which starts almost immediately after `mount.sh`) can race
  ahead of it and allocate a file (e.g. an InnoDB redo log) into a not-yet-zeroed
  region, which reads back as garbage InnoDB misreports as encrypted. Explains why this
  was previously seen as intermittent/"not fully root-caused" (a genuine race, not a
  deterministic bug) and why a full-device `dd if=/dev/zero` reliably masked it (every
  block already zero, independent of `ext4lazyinit` timing). **Fix**: see the
  `run-flax-baremetal-htap.sh` bullet below — forces synchronous inode/journal init
  instead. Full root-cause writeup: `FLAX/CLAUDE.md` gotcha #7.
- **`env-flax-baremetal.sh`'s `check_ssd_device`/`check_ssd_mount` are real safety
  checks** (unlike the sandbox's blind no-op stubs) — they resolve the NVMeVirt device
  via its stable `/dev/disk/by-id/nvme-CSL_Virt_MN_01_CSL_Virt_SN_01` alias and verify
  `/mnt/nvme` is actually mounted from it, given `s1` has multiple physical NVMe
  devices whose `/dev/nvmeXn1` naming has been observed reshuffling across reboots.
  `check_ssd_mount` is self-sufficient (resolves the device itself if not already set)
  because `profile-htap.sh` (shared, not FLAX-specific) calls it alone, without calling
  `check_ssd_device` first — don't assume call order across scripts.
- **Full-scale (`HTAP_QUERY_TIMEOUT=7200`, `HTAP_OLAP_RUNS=5`) HTAP runs can fill
  `/mnt/nvme` before completing**, if that filesystem is small (the emulated device's
  capacity is just a `memmap=` GRUB reservation, easy to under-size early on and forget
  to revisit — see `FLAX/CLAUDE.md`'s bare-metal section). Calibration from one clean
  single-session disk-full incident: ~0.13GB/hour/OLTP-thread. Two root causes worth
  fixing rather than permanently working around by scaling down the query loop: (1) the
  emulated device's `memmap=` reservation can simply be made bigger (more physical RAM
  is usually available); (2) only the offloaded column family (`sbtest1-4` in the
  current `join4.sql`-driven setup) actually needs to live on the emulated device at
  all — the other 8 tables could use RocksDB's per-CF `cf_paths` to live on a real,
  larger host disk instead, cutting emulated-device growth roughly to a third (OLTP
  writes spread ~evenly across all 12 tables). Neither implemented yet as of
  2026-07-29 — `env-flax-baremetal.sh` currently has a *temporary* scale-down override
  instead (see that file's own comment for the math and removal criteria). Superseded
  on `star1`: not needed there (~206GB usable), see `env-flax-baremetal.sh`'s own
  updated comment.
- **`component_reference_cache` is a mandatory component, not an optional one tied to
  the KMIP/Vault/OpenID flags disabled at `cmake` time** — confirmed 2026-08-01 by
  reading `sql/mysqld.cc:2069` (`component_urns[] = {"file://component_reference_cache"}`)
  and its load call at `sql/mysqld.cc:8139` (`if (!is_help_or_validate_option() &&
  !opt_initialize) dynamic_loader_srv->load(component_urns, ...)`) — loaded on every
  *normal* startup (not during `--initialize`/`--initialize-insecure`), unconditionally.
  It's a separate CMake target (`MYSQL_ADD_COMPONENT(reference_cache ...)` in
  `components/reference_cache/CMakeLists.txt` → target name `component_reference_cache`)
  that a `make mysqld mysql mysqladmin`-only build never produces. Missing it doesn't
  actually crash startup by itself (the load call's own comment confirms it's
  best-effort, no `unireg_abort()` follows it) — but it's worth building anyway to avoid
  a confusing red herring in startup logs. Build with
  `make -j$(nproc) component_reference_cache` alongside whatever else is being rebuilt.
- **`rocksdb` (the `ha_rocksdb.so` plugin) is a separate CMake target from `mysqld`/
  `mysql`/`mysqladmin`, not a dependency of them** — confirmed 2026-08-01 when switching
  the FLAX bare-metal build from `CMAKE_BUILD_TYPE=Debug` to `RelWithDebInfo`: rebuilding
  only `mysqld mysql mysqladmin` after the reconfigure would have left `ha_rocksdb.so`
  on the old build flags, silently defeating the entire point of the switch (virtually
  all the code this project measures — `FindNextUserEntry`, `nvmevirt_table.cc`/
  `RunMvccFilter` — lives in that plugin, not in `mysqld` itself). Any build-type/flag
  change requires rebuilding `rocksdb` explicitly alongside the server binaries:
  `make -j$(nproc) component_reference_cache rocksdb mysqld mysql mysqladmin`.
- **`/tmp/nvmevirt_debug.log` (the `RunMvccFilter` wall-clock instrumentation) is never
  truncated automatically between runs** — same class of bug as the already-documented
  CEMU-thread `cemu_debug.log` gotcha, just not previously caught on this side. Confirmed
  2026-08-01: after weeks of accumulated runs it had grown to ~4 million lines; a script
  step that dumps/tails it (`verify.sh`) looked like it was reprocessing enormous amounts
  of leftover data, when actually it was just displaying the file's entire multi-session
  history with the current run's own (tiny) output at the tail. Truncate before trusting
  any log-volume-based read on this file: `sudo truncate -s 0 /tmp/nvmevirt_debug.log`.
- **`run-flax-baremetal-htap.sh`'s Phase 1 (non-`--skip-prepare`) now does a full
  physical wipe of `/mnt/nvme` before every independent session**, added 2026-08-01, on
  top of `prepare.sh`'s own logical `DROP DATABASE`. The logical drop alone was already
  sufficient for data *correctness* between sessions (confirmed by reading
  `sysbench-htap/prepare.sh:63`) — the physical wipe is purely for cleanliness: without
  it, a reused datadir directory leaves the previous session's dropped-table SSTs on
  disk pending background compaction/GC (minor extra I/O contention early in the new
  session), separate from and not fully explaining the `nvmevirt_debug.log`
  accumulation issue above (that log lives outside `/mnt/nvme` entirely and isn't
  touched by this wipe — truncate it separately). The wipe intentionally does **not**
  run under `--skip-prepare` (defeats the purpose of that flag, which exists
  specifically to reuse already-prepared data), and stops any running `mysqld` first
  since `umount` fails on a busy device — this only stops the engine's own tracked
  instance (`mysql-control.sh ... stop`), not any manually-started `mysqld` (e.g. a
  `verify.sh` correctness-test instance) using a different datadir/socket; stop those
  separately first.
  **Updated 2026-08-03**: this step no longer shells out to FLAX's own `src/mount.sh` —
  it inlines the same `umount`/`mkfs.ext4 -F`/`mount`/`chown` sequence directly, with
  `-E lazy_itable_init=0,lazy_journal_init=0` added to the `mkfs.ext4` call (root-cause
  fix for the redo-log-decryption gotcha above). Kept in `db-eval` rather than editing
  `FLAX/src/mount.sh` itself, per `feedback_minimal_flax_footprint` — anything outside
  `db-eval` that calls FLAX's own `mount.sh` directly is still exposed to the race.
- **Query execution plan for `join4.sql` was observed to differ across runs and across
  OFF/ON sessions** (filesort/nested-loop in some runs, hash-join in others) — root
  cause is RocksDB's own approximate per-CF cardinality estimate drifting under
  sustained OLTP writes, independent of `ANALYZE TABLE`/histogram freshness (which
  itself only runs once per session, before Run 1, not per-run). This is a real
  confound: a worse plan can require examining far more row-pairs than a hash join for
  the same output, enough on its own to explain why OLAP Run 1 specifically times out
  every session while Runs 2-5 don't, independent of any offload effect. Fixed 2026-08-01
  by adding `STRAIGHT_JOIN` to `join4.sql` to pin join order deterministically across
  every run/session (see the query's own comment for the full reasoning and a note that
  this pins join *order* only, not join *algorithm* — if flamegraphs still show
  plan-type flips after this, also pin `optimizer_switch='block_nested_loop=off'`).
- **Flamegraphs from the RelWithDebInfo build show a large (90-96%) `[unknown]` root
  frame at address `0xffffffffffffffff` — this is tail-call elimination, not a symbol-
  resolution failure, and is not fixable via `perf`/`profile-htap.sh` flags.** Confirmed
  2026-08-01 by directly ruling out, in order: `--no-inline` (the box is one node, not
  many partially-resolved frames — inconsistent with what `--no-inline` actually does);
  `kptr_restrict`/`perf_event_paranoid` (both `perf record` and `perf script` already run
  as root, which bypasses these); missing kernel symbols (`/proc/kallsyms` resolves real
  kernel-text addresses fine under `sudo`); and the `--call-graph dwarf` default 8192-byte
  stack-snapshot size (a controlled test with `dwarf,65528` showed an unrelated thread
  unwinding cleanly to genuine thread-entry code with no sentinel, while the specific call
  path tested still hit the identical sentinel at wildly varying depths below it — ruling
  out a fixed size/depth limit). `0xffffffffffffffff` is the canonical "undefined return
  address register" DWARF-CFI sentinel: when GCC tail-call-optimizes a call, the caller's
  stack frame is discarded before the callee runs, so there's genuinely nothing left on
  the real runtime stack to unwind into — not a tooling gap. Far less visible on the old
  Debug build (`-O0` doesn't perform tail-call elimination). In every case examined so
  far, the loss occurs at the *outer* dispatch/connection layer (e.g.
  `mysql_execute_command`/`dispatch_command`), one level above the RocksDB/optimizer/
  mutex-contention code this project's analysis actually depends on, which resolves fully
  beneath it — treat as a disclosed methodology caveat, not a reason to distrust the
  flamegraphs' function-level attribution. Full investigation:
  `project_flax_integration_status` memory.
