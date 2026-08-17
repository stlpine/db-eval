#!/bin/bash
# HTAP Profiling: concurrent OLTP writes + LLTs + analytical join queries
#
# Simulates HTAP per AIDE VLDB'23 §6.4 + SIGMOD'20 LLT paper:
#   - 24 sysbench OLTP threads continuously writing to sbtest1..sbtest12
#   - 4 long-lived transactions (LLTs) holding RocksDB GC back
#   - Analytical 4-table equi-join profiled with perf + RocksDB perf context
#
# LLTs are necessary to accumulate versions across runs. Without them, GC
# advances between OLAP runs and cleans old versions — internal_key_skipped_count
# stays low and version-traversal overhead is too small to appear in flamegraphs.
# Our join query finishes in seconds, so explicit LLTs simulate the GC-blocking
# effect that long analytical queries (minutes) create naturally in the AIDE paper.
#
# Measures: internal_key_skipped_count growth over time (version chain buildup),
#           OLAP query latency degradation across runs, and CPU flamegraphs.
#
# Usage:
#   sudo cgexec -g memory:limited_memory_group \
#       bash ./profiling/profile-htap.sh [cutoff] [result_dir] [engine]
#
#   cutoff      k <= cutoff value (default: $HTAP_JOIN_CUTOFF from env.sh)
#   result_dir  Output dir (default: results/profiling/htap/<engine>/<timestamp>)
#   engine      percona-myrocks | percona-innodb | percona-myrocks-csd | percona-myrocks-nvmevirt (default: percona-myrocks)
#
# Prerequisites:
#   - sysbench-htap data loaded: prepare-data.sh -e <engine> -b sysbench-htap
#   - ~/FlameGraph cloned (brendangregg/FlameGraph)
#   - linux-tools-$(uname -r) installed
#   - sysbench installed

# Note: no set -e / set -o pipefail — mirrors other profiling script pattern.
# Critical failures use explicit exit 1; perf/mysql pipelines are best-effort.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"
source "${SCRIPT_DIR}/../scripts/monitor.sh"

# ── Configuration ─────────────────────────────────────────────────────────────

CUTOFF="${1:-${HTAP_JOIN_CUTOFF}}"
ENGINE="${3:-percona-myrocks}"
JOIN4_SQL="${SCRIPT_DIR}/../sysbench-htap/queries/join4.sql"

case "$ENGINE" in
    percona-myrocks)
        SOCKET="${MYSQL_SOCKET_PERCONA_MYROCKS}"
        PID_FILE="${MYSQL_PID_PERCONA_MYROCKS}"
        EXPECTED_ENGINE="ROCKSDB"
        ;;
    percona-myrocks-csd)
        SOCKET="${MYSQL_SOCKET_PERCONA_MYROCKS_CSD}"
        PID_FILE="${MYSQL_PID_PERCONA_MYROCKS_CSD}"
        EXPECTED_ENGINE="ROCKSDB"
        ;;
    percona-myrocks-nvmevirt)
        SOCKET="${MYSQL_SOCKET_PERCONA_MYROCKS_NVMEVIRT}"
        PID_FILE="${MYSQL_PID_PERCONA_MYROCKS_NVMEVIRT}"
        EXPECTED_ENGINE="ROCKSDB"
        ;;
    percona-innodb)
        SOCKET="${MYSQL_SOCKET_PERCONA_INNODB}"
        PID_FILE="${MYSQL_PID_PERCONA_INNODB}"
        EXPECTED_ENGINE="InnoDB"
        ;;
    *)
        echo "Unknown engine: $ENGINE (use percona-myrocks, percona-myrocks-csd, percona-myrocks-nvmevirt, or percona-innodb)" >&2
        exit 1
        ;;
esac

# Helper: true for all MyRocks variants (share all RocksDB perf-context logic)
IS_MYROCKS=false
IS_CSD=false
IS_NVMEVIRT=false
if [ "$ENGINE" = "percona-myrocks" ] || [ "$ENGINE" = "percona-myrocks-csd" ] || [ "$ENGINE" = "percona-myrocks-nvmevirt" ]; then
    IS_MYROCKS=true
fi
[ "$ENGINE" = "percona-myrocks-csd" ] && IS_CSD=true
[ "$ENGINE" = "percona-myrocks-nvmevirt" ] && IS_NVMEVIRT=true
# Structural flag shared by both device-offload engines: their SQL blocks
# insert an extra global-status snapshot before/after the join, which shifts
# how ctx_after must be anchored (see the TABLE_SCHEMA-counting parse below).
IS_DEVICE_OFFLOAD=false
[ "$IS_CSD" = "true" ] || [ "$IS_NVMEVIRT" = "true" ] && IS_DEVICE_OFFLOAD=true

RESULT_DIR="${2:-${RESULTS_DIR}/profiling/htap/${ENGINE}/$(date +%Y%m%d_%H%M%S)}"

# ── Helper functions (same pattern as all profiling scripts) ──────────────────

drop_page_cache() {
    log_info "Dropping OS page cache..."
    sync
    echo "${DROP_CACHES_LEVEL:-3}" | ${BENCH_SUDO-sudo} tee /proc/sys/vm/drop_caches > /dev/null
    log_info "Page cache dropped"
}

start_mysql_cold() {
    ensure_mysql_stopped "$ENGINE"
    drop_page_cache
    # Clear the device-offload debug log so each profiling run starts clean.
    # Without this, tail -N shows entries from previous sessions mixed with current.
    [ "$IS_CSD" = "true" ] && > /tmp/cemu_debug.log 2>/dev/null || true
    # sudo'd: mysqld runs as root in the FLAX sandbox, so a leftover log from
    # a prior run is root-owned -- a plain `> file` here would fail with
    # "Permission denied" as the invoking non-root guest user.
    #
    # delete-then-let-mysqld-recreate, NOT truncate-in-place. Confirmed
    # 2026-08-09: `sudo tee file < /dev/null` (open-for-write, i.e. O_TRUNC)
    # can fail with EACCES even for root, for reasons not fully root-caused
    # (ruled out: chattr immutable/append-only attributes -- lsattr showed
    # only the harmless "extent format" flag; a stray process holding the
    # file open -- lsof showed nothing; AppArmor confining root or mysqld --
    # no relevant profile in `aa-status`; /tmp being a special/FUSE mount --
    # it isn't, `mount` shows no separate /tmp entry). `rm -f` (which only
    # needs write permission on the *directory*, a different check than
    # O_TRUNC on the file itself) succeeded every time this failed, and
    # mysqld's own `fopen(path, "a")` creates the file fresh (root-owned)
    # if it doesn't exist -- strictly more robust than truncating in place,
    # and self-heals whatever ownership/attribute state the file drifts
    # into, without needing to understand why.
    [ "$IS_NVMEVIRT" = "true" ] && { ${BENCH_SUDO-sudo} rm -f /tmp/nvmevirt_debug.log 2>/dev/null || true; }
    # Iterator-recreation diagnostic log (rdb_iterator.cc) -- applies to any
    # MyRocks engine, not just device-offload ones, since it instruments
    # Rdb_iterator_base directly. Same delete-then-recreate reasoning above.
    [ "$IS_MYROCKS" = "true" ] && { ${BENCH_SUDO-sudo} rm -f /tmp/rdb_iterator_debug.log 2>/dev/null || true; }
    log_info "Starting MySQL (cold)..."
    "${SCRIPT_DIR}/../scripts/mysql-control.sh" "$ENGINE" start
    sleep 5
    if ! mysqladmin --socket="$SOCKET" ping &>/dev/null; then
        log_error "MySQL failed to start"
        exit 1
    fi
}

stop_mysql() {
    log_info "Stopping MySQL..."
    "${SCRIPT_DIR}/../scripts/mysql-control.sh" "$ENGINE" stop
    sleep 3
}

verify_storage_engine() {
    local wrong_tables
    wrong_tables=$(mysql --socket="$SOCKET" -N -e "
        SELECT TABLE_NAME, ENGINE
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = '${BENCHMARK_DB}' AND ENGINE != '${EXPECTED_ENGINE}';" 2>/dev/null)
    if [ -n "$wrong_tables" ]; then
        log_error "Storage engine mismatch! Expected ${EXPECTED_ENGINE}."
        echo "$wrong_tables"
        stop_mysql
        exit 1
    fi
    log_info "Storage engine verified: all tables use ${EXPECTED_ENGINE}"
}

capture_data_profile() {
    local result_dir=$1
    log_info "Capturing data profile..."
    {
        echo "table_name,engine,rows,avg_row_bytes,data_mb,index_mb,total_mb"
        timeout 120 mysql --socket="$SOCKET" -N -e "
            SELECT TABLE_NAME, ENGINE, TABLE_ROWS,
                ROUND(AVG_ROW_LENGTH, 2),
                ROUND(DATA_LENGTH / 1024 / 1024, 2),
                ROUND(INDEX_LENGTH / 1024 / 1024, 2),
                ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2)
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = '${BENCHMARK_DB}'
            ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC;" 2>/dev/null | tr '\t' ','
    } > "${result_dir}/data_profile.csv" 2>&1 || true
    log_info "Data profile saved to: ${result_dir}/data_profile.csv"
}

# SST entries per live row in sbtest1-4: the average on-disk version chain length.
# Reads information_schema only, so it does not warm the cache the next run wants cold.
measure_version_amplification() {
    local live_rows=$(( HTAP_TABLE_SIZE * 4 ))
    mysql --socket="$SOCKET" -N --batch 2>/dev/null -e "
        SELECT IFNULL(SUM(f.NUM_ROWS), 0)
        FROM information_schema.ROCKSDB_INDEX_FILE_MAP f
        JOIN information_schema.ROCKSDB_DDL d
          ON d.INDEX_NUMBER = f.INDEX_NUMBER
        WHERE d.TABLE_SCHEMA = '${BENCHMARK_DB}'
          AND d.TABLE_NAME IN ('sbtest1','sbtest2','sbtest3','sbtest4');" \
        | awk -v live="$live_rows" 'NR==1{printf "%d %.3f\n", $1, (live>0 ? $1/live : 0)}'
}

# capture_index_ddl_map: index_number -> (table, index, CF) mapping, needed to
# decode /tmp/rdb_iterator_debug.log's iterator-recreation events -- that log
# only has cf/index_number (no table name available at that point in the
# code), and sbtest1-4 all share the same CF and the same index name
# ("PRIMARY"), so index_number is the only thing that actually distinguishes
# them.
capture_index_ddl_map() {
    local result_dir=$1
    log_info "Capturing index/DDL map (for rdb_iterator_debug.log)..."
    if [ "$IS_MYROCKS" = "true" ]; then
        mysql --socket="$SOCKET" --batch 2>/dev/null \
            -e "SELECT TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, INDEX_NUMBER, COLUMN_FAMILY
                FROM information_schema.ROCKSDB_DDL
                WHERE TABLE_SCHEMA = '${BENCHMARK_DB}'
                ORDER BY TABLE_NAME;" > "${result_dir}/rocksdb_ddl_index_map.txt" 2>&1 || true
    fi
    log_info "Index/DDL map saved to: ${result_dir}/rocksdb_ddl_index_map.txt"
}

# snapshot_perf_context_global: fetch cumulative RocksDB/InnoDB counters.
# For MyRocks: information_schema.rocksdb_perf_context_global (global aggregates).
# For InnoDB:  SHOW GLOBAL STATUS (InnoDB_* counters).
# Output format: "variable_name value" one per line (tab-separated from MySQL,
# but awk matches on $1 so tab vs space doesn't matter).
snapshot_perf_context_global() {
    if [ "$IS_MYROCKS" = "true" ]; then
        local result
        result=$(mysql --socket="$SOCKET" --batch --skip-column-names 2>/dev/null -e "
            SELECT variable_name, variable_value
            FROM information_schema.rocksdb_perf_context_global
            WHERE variable_name IN (
                'internal_key_skipped_count',
                'internal_delete_skipped_count',
                'get_snapshot_time',
                'block_read_count',
                'block_read_byte',
                'block_read_time',
                'get_from_memtable_count',
                'get_from_memtable_time',
                'get_from_output_files_time'
            )
            ORDER BY variable_name;") || true

        if [ -n "$result" ]; then
            echo "$result"
        elif [ "${PERF_CTX_USE_IS_TABLE}" = "true" ]; then
            # Table probed as available at startup but returned empty — transient, skip silently
            true
        else
            mysql --socket="$SOCKET" --batch --skip-column-names 2>/dev/null -e "
                SHOW ENGINE ROCKSDB STATUS;" \
            | grep -E "internal_key_skipped_count|internal_delete_skipped_count|get_snapshot_time|block_read_count|block_read_byte|block_read_time|get_from_memtable_count|get_from_memtable_time|get_from_output_files_time" \
            | awk '{print $1, $NF}' || true
        fi
    else
        mysql --socket="$SOCKET" --batch --skip-column-names 2>/dev/null -e "
            SHOW GLOBAL STATUS WHERE Variable_name IN (
                'Innodb_rows_read',
                'Innodb_rows_deleted',
                'Innodb_buffer_pool_reads',
                'Innodb_buffer_pool_read_requests',
                'Innodb_data_reads',
                'Innodb_data_read'
            );" || true
    fi
}

# get_effective_memory_limit: read the ACTUAL cgroup v2 memory.max for a given
# PID (default: this script's own process), not the CGROUP_MEMORY_LIMIT env
# default -- the two can differ. Added 2026-08-06: run-flax-baremetal-htap.sh
# invokes this script as plain `sudo -E bash`, never `cgexec`, so FLAX
# bare-metal HTAP runs have never actually been memory-limited despite
# profiling_config.log previously echoing the env.sh default (16G) verbatim,
# implying otherwise. This reads ground truth from /proc + /sys/fs/cgroup so
# the log is correct regardless of how the script was invoked. See
# db-eval/CLAUDE.md's join4.sql plan-drift gotcha and
# flax_baremetal_htap_hardened_offload_findings_20260803.md for the
# still-open Run 1/Run 3 investigation this supports.
get_effective_memory_limit() {
    local pid="${1:-self}"
    local cg_path
    cg_path=$(${BENCH_SUDO-sudo} awk -F: '$1=="0"{print $3}' "/proc/${pid}/cgroup" 2>/dev/null)
    if [ -z "$cg_path" ]; then
        echo "unknown (could not read /proc/${pid}/cgroup)"
        return
    fi
    local mem_max_file="/sys/fs/cgroup${cg_path}/memory.max"
    if [ ! -f "$mem_max_file" ]; then
        echo "no memory controller active at cgroup=${cg_path} (unconstrained)"
        return
    fi
    local val
    val=$(${BENCH_SUDO-sudo} cat "$mem_max_file" 2>/dev/null)
    if [ "$val" = "max" ]; then
        echo "unlimited (cgroup=${cg_path}, memory.max=max)"
    else
        awk -v b="$val" -v p="$cg_path" 'BEGIN{printf "%.2f GB (cgroup=%s)\n", b/1024/1024/1024, p}'
    fi
}

# snapshot_memory_state: host + mysqld memory usage, paired before/after each
# OLAP run -- same pattern as the rocksdb_status_run<N>_{before,after}.txt
# LSM-state snapshots. Added 2026-08-06 to check whether real memory pressure
# (page cache eviction, mysqld RSS growth, actual cgroup throttling) plays any
# role in the still-open Run 1/Run 3 cost anomaly, now that the
# CGROUP_MEMORY_LIMIT log line can no longer be trusted to reflect reality
# (see get_effective_memory_limit above). Cheap (/proc + /sys reads only, no
# workload impact).
snapshot_memory_state() {
    local out_file=$1
    {
        echo "=== free -h ==="
        free -h 2>/dev/null
        echo ""
        echo "=== mysqld memory (PID ${MYSQLD_PID}) ==="
        ${BENCH_SUDO-sudo} awk '/^Vm(RSS|Size|Swap|HWM):/' "/proc/${MYSQLD_PID}/status" 2>/dev/null
        echo ""
        echo "=== effective cgroup memory limit (mysqld) ==="
        get_effective_memory_limit "$MYSQLD_PID"
        local cg_path cur_file
        cg_path=$(${BENCH_SUDO-sudo} awk -F: '$1=="0"{print $3}' "/proc/${MYSQLD_PID}/cgroup" 2>/dev/null)
        cur_file="/sys/fs/cgroup${cg_path}/memory.current"
        if [ -n "$cg_path" ] && [ -f "$cur_file" ]; then
            ${BENCH_SUDO-sudo} awk '{printf "current usage: %.2f GB\n", $1/1024/1024/1024}' "$cur_file" 2>/dev/null
        fi
    } > "$out_file" 2>&1
}

# ── Preflight ─────────────────────────────────────────────────────────────────

log_info "=========================================="
log_info "HTAP Profiling (${ENGINE})"
log_info "=========================================="
log_info "Engine   : $ENGINE"
log_info "Cutoff   : $CUTOFF  (~$(awk "BEGIN{printf \"%.0f\", 100*${CUTOFF}/${HTAP_TABLE_SIZE}}")% selectivity)"
log_info "OLTP threads : $HTAP_OLTP_THREADS"
log_info "LLT count    : $HTAP_LLT_COUNT"
log_info "OLAP runs    : $HTAP_OLAP_RUNS"
log_info "Warmup       : ${HTAP_WARMUP_DURATION}s | Duration: ${HTAP_DURATION}s"
log_info "Results  : $RESULT_DIR"
log_info "=========================================="

# Stop system MySQL service if running
log_info "Checking for running MySQL service..."
if systemctl is-active --quiet mysql 2>/dev/null; then
    log_info "MySQL service is running. Stopping it..."
    ${BENCH_SUDO-sudo} systemctl stop mysql
    sleep 3
    if systemctl is-active --quiet mysql 2>/dev/null; then
        log_error "Failed to stop MySQL service"
        exit 1
    fi
    log_info "MySQL service stopped"
else
    log_info "MySQL service is not running"
fi

# Validate prerequisites
check_ssd_mount || { log_error "SSD mount check failed"; exit 1; }

if [ ! -f "${FLAMEGRAPH_DIR}/flamegraph.pl" ]; then
    log_error "FlameGraph not found at ${FLAMEGRAPH_DIR}. Clone brendangregg/FlameGraph there."
    exit 1
fi

if ! command -v sysbench &>/dev/null; then
    log_error "sysbench not found. Install it: sudo apt install sysbench"
    exit 1
fi

if [ ! -f "$JOIN4_SQL" ]; then
    log_error "join4.sql not found at $JOIN4_SQL"
    exit 1
fi

mkdir -p "$RESULT_DIR"

MYSQL_LIB_PATH=$(mysql_config --variable=pkglibdir 2>/dev/null || true)
[ -n "$MYSQL_LIB_PATH" ] && export LD_LIBRARY_PATH="${MYSQL_LIB_PATH}:${LD_LIBRARY_PATH:-}"

# ── Initialise CSVs ───────────────────────────────────────────────────────────

if [ "$ENGINE" = "percona-myrocks" ]; then
    echo "run,query_ok,elapsed_s,cutoff,rows_scanned,sst_entries,version_amp,internal_key_skipped_count_delta,internal_delete_skipped_count_delta,get_snapshot_time_ns_delta,block_read_count_delta,block_read_byte_delta,block_read_time_ns_delta,get_from_memtable_count_delta,get_from_output_files_time_ns_delta" \
        > "${RESULT_DIR}/htap_olap_runs.csv"
elif [ "$ENGINE" = "percona-myrocks-csd" ]; then
    echo "run,query_ok,elapsed_s,cutoff,rows_scanned,sst_entries,version_amp,internal_key_skipped_count_delta,internal_delete_skipped_count_delta,get_snapshot_time_ns_delta,block_read_count_delta,block_read_byte_delta,block_read_time_ns_delta,get_from_memtable_count_delta,get_from_output_files_time_ns_delta,csd_keys_seen,csd_keys_filtered,csd_freeze_ns" \
        > "${RESULT_DIR}/htap_olap_runs.csv"
elif [ "$ENGINE" = "percona-myrocks-nvmevirt" ]; then
    # No freeze_ns column -- FLAX's v1 offload only blocks the calling mysqld
    # thread on its own SST read, not the whole guest, so there's no
    # equivalent counter to report.
    echo "run,query_ok,elapsed_s,cutoff,rows_scanned,sst_entries,version_amp,internal_key_skipped_count_delta,internal_delete_skipped_count_delta,get_snapshot_time_ns_delta,block_read_count_delta,block_read_byte_delta,block_read_time_ns_delta,get_from_memtable_count_delta,get_from_output_files_time_ns_delta,nvmevirt_keys_seen,nvmevirt_keys_filtered" \
        > "${RESULT_DIR}/htap_olap_runs.csv"
else
    echo "run,query_ok,elapsed_s,cutoff,rows_scanned,handler_read_key,innodb_rows_read_delta,innodb_buffer_pool_reads_delta,innodb_buffer_pool_read_requests_delta,innodb_pages_read_delta,innodb_data_reads_delta,innodb_data_read_bytes_delta" \
        > "${RESULT_DIR}/htap_olap_runs.csv"
fi

if [ "$IS_MYROCKS" = "true" ]; then
    echo "snapshot_num,elapsed_s,wall_clock_ts,internal_key_skipped_count,internal_delete_skipped_count,block_read_count,llt_count_active" \
        > "${RESULT_DIR}/htap_version_growth.csv"
else
    echo "snapshot_num,elapsed_s,wall_clock_ts,innodb_rows_read,innodb_rows_deleted,innodb_buffer_pool_reads,llt_count_active" \
        > "${RESULT_DIR}/htap_version_growth.csv"
fi

# ── Phase 1: Cold MySQL start ─────────────────────────────────────────────────

start_mysql_cold
verify_storage_engine

# For CSD engine: honour ROCKSDB_CEMU_ENABLED (default ON).
# CemuTableReader.NewIterator() checks rocksdb_cemu_enabled at iterator-creation
# time, so this flag can be toggled at any point before queries run.
# Set ROCKSDB_CEMU_ENABLED=false to run a no-CSD control with the CSD binary.
# NOTE: my-percona-myrocks-csd-vm.cnf starts mysqld with rocksdb_cemu_enabled=ON,
# so we must actively SET GLOBAL OFF — not just skip the ON — when disabling.
if [ "$IS_CSD" = "true" ]; then
    if [ "${ROCKSDB_CEMU_ENABLED:-true}" != "false" ]; then
        mysql --socket="$SOCKET" \
            -e "SET GLOBAL rocksdb_cemu_enabled = ON;" 2>/dev/null || \
            log_error "  WARNING: failed to enable rocksdb_cemu_enabled"
        log_info "rocksdb_cemu_enabled set to ON"
    else
        mysql --socket="$SOCKET" \
            -e "SET GLOBAL rocksdb_cemu_enabled = OFF;" 2>/dev/null || \
            log_error "  WARNING: failed to disable rocksdb_cemu_enabled"
        log_info "rocksdb_cemu_enabled set to OFF (no-CSD control run)"
    fi
fi

# For the NVMeVirt engine: rocksdb_nvmevirt_enabled is checked in
# NvmeVirtTableReader::NewIterator() at iterator-creation time
# (per-query), so it's safe to toggle at any point before queries run.
# Set ROCKSDB_NVMEVIRT_ENABLED=false to run a no-offload control with the same
# binary/data (my-percona-myrocks-nvmevirt-sandbox.cnf starts with it ON, so we
# must actively SET GLOBAL OFF -- not just skip the ON -- when disabling).
if [ "$IS_NVMEVIRT" = "true" ]; then
    if [ "${ROCKSDB_NVMEVIRT_ENABLED:-true}" != "false" ]; then
        mysql --socket="$SOCKET" \
            -e "SET GLOBAL rocksdb_nvmevirt_enabled = ON;" 2>/dev/null || \
            log_error "  WARNING: failed to enable rocksdb_nvmevirt_enabled"
        log_info "rocksdb_nvmevirt_enabled set to ON"
    else
        mysql --socket="$SOCKET" \
            -e "SET GLOBAL rocksdb_nvmevirt_enabled = OFF;" 2>/dev/null || \
            log_error "  WARNING: failed to disable rocksdb_nvmevirt_enabled"
        log_info "rocksdb_nvmevirt_enabled set to OFF (no-offload control run)"
    fi
fi

capture_data_profile "$RESULT_DIR"
capture_index_ddl_map "$RESULT_DIR"

# Stabilise optimizer statistics before any workload starts.
# ANALYZE TABLE alone is not enough for MyRocks: SST row count estimates can be
# wildly wrong, AND the optimizer needs a histogram on k to estimate the
# selectivity of WHERE k <= @htap_cutoff.  Without a histogram, the optimizer
# uses a default uniform assumption and may choose a suboptimal nested-loop plan
# on run 1 (400k rows scanned) instead of hash join (300k rows).
log_info "Running ANALYZE TABLE + histogram on k to stabilise optimizer..."
# MySQL 8.4: UPDATE HISTOGRAM only accepts a single table — loop over each.
# ANALYZE TABLE (without histogram) accepts multiple tables and must run first
# to refresh RocksDB row-count estimates used by the join-order planner.
mysql --socket="$SOCKET" "$BENCHMARK_DB" 2>/dev/null \
    -e "ANALYZE TABLE sbtest1, sbtest2, sbtest3, sbtest4;" || \
    log_error "  WARNING: ANALYZE TABLE failed"
for _tbl in sbtest1 sbtest2 sbtest3 sbtest4; do
    mysql --socket="$SOCKET" "$BENCHMARK_DB" 2>/dev/null \
        -e "ANALYZE TABLE ${_tbl} UPDATE HISTOGRAM ON k WITH 254 BUCKETS;" || \
        log_error "  WARNING: histogram on ${_tbl} failed (non-fatal)"
done

# Capture the schema of information_schema.ROCKSDB_PERF_CONTEXT for reference.
# Confirmed schema: key-value — (TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, STAT_TYPE, VALUE).
# One row per (table, metric). Parsers use STAT_TYPE for lookup, VALUE for aggregation.
if [ "$IS_MYROCKS" = "true" ]; then
    log_info "Discovering ROCKSDB_PERF_CONTEXT schema..."
    {
        echo "=== DESCRIBE ==="
        mysql --socket="$SOCKET" --batch 2>/dev/null \
            -e "DESCRIBE information_schema.ROCKSDB_PERF_CONTEXT;" || echo "(table missing or error)"
        echo "=== SAMPLE ROW (after ANALYZE, before OLTP) ==="
        mysql --socket="$SOCKET" "$BENCHMARK_DB" --batch 2>/dev/null \
            -e "SELECT * FROM information_schema.ROCKSDB_PERF_CONTEXT
                WHERE TABLE_SCHEMA = '${BENCHMARK_DB}'
                  AND TABLE_NAME = 'sbtest1';" || echo "(no rows or error)"
    } > "${RESULT_DIR}/rocksdb_perf_ctx_schema.txt" 2>&1
    log_info "Schema discovery saved to: ${RESULT_DIR}/rocksdb_perf_ctx_schema.txt"
fi

# ── Phase 2: Configure + Monitors ────────────────────────────────────────────

PERF_CTX_USE_IS_TABLE=false
if [ "$IS_MYROCKS" = "true" ]; then
    mysql --socket="$SOCKET" \
        -e "SET GLOBAL rocksdb_perf_context_level = ${PROFILING_PERF_CONTEXT_LEVEL};" 2>/dev/null
    log_info "rocksdb_perf_context_level set to ${PROFILING_PERF_CONTEXT_LEVEL}"
    _probe=$(mysql --socket="$SOCKET" --batch --skip-column-names 2>/dev/null -e "
        SELECT COUNT(*) FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = 'information_schema'
          AND TABLE_NAME = 'rocksdb_perf_context_global';" || echo "0")
    if [ "${_probe:-0}" -gt 0 ] 2>/dev/null; then
        PERF_CTX_USE_IS_TABLE=true
        log_info "rocksdb_perf_context_global: available (using information_schema)"
    else
        PERF_CTX_USE_IS_TABLE=false
        log_info "rocksdb_perf_context_global: unavailable — will use SHOW ENGINE ROCKSDB STATUS fallback"
    fi
fi
export PERF_CTX_USE_IS_TABLE

start_monitors "$RESULT_DIR" "profiling_htap"

# Total time the LLTs (and OLTP) must stay alive to cover the full OLAP loop.
# Must match the OLAP loop's actual wall-clock structure below: warmup, the
# pre-loop flush+settle, HTAP_OLAP_RUNS query timeouts, AND the per-run
# re-flush+settle (30s) inserted before every run after the first (see the
# "Re-flush the memtable before every run after the first" block in the OLAP
# loop). Previously this only accounted for warmup + OLAP_RUNS*QUERY_TIMEOUT,
# which omitted the flush/settle overhead entirely -- with a fixed LLT sleep
# duration that didn't grow to match, the LLTs died up to ~95s before the
# last OLAP run actually finished (confirmed 2026-07-27,
# percona-myrocks-nvmevirt/20260726_150128: llt_count_active hit 0 mid-Run-3,
# visibly distorting that run's perf-context deltas). FLUSH_SETTLE_OVERHEAD
# covers the (HTAP_OLAP_RUNS - 1) re-flush waits; the initial pre-loop
# flush+settle is already covered by the existing +60s margin below.

# Optional diagnostic "Run 0": one extra join4.sql execution inserted at the
# exact point Run 1 normally occupies (same LLTs/OLTP/warmup/flush/settle),
# for investigating why Run 1 is sometimes far slower than later runs.
# Opt-in via HTAP_DIAGNOSE_RUN0=true (default off, no behavior change).
RUN_START=1
if [ "${HTAP_DIAGNOSE_RUN0:-false}" = "true" ]; then
    RUN_START=0
    log_info "HTAP_DIAGNOSE_RUN0=true: inserting a diagnostic Run 0 before Run 1 (Run 1/Run 3 timeout investigation)"
fi
EFFECTIVE_OLAP_RUNS=$(( HTAP_OLAP_RUNS + (RUN_START == 0 ? 1 : 0) ))
FLUSH_SETTLE_OVERHEAD=$(( 30 * (EFFECTIVE_OLAP_RUNS - 1) ))

# Warmup must outlast the LLT staggering window, or early runs see fewer snapshots
# than later ones.
LLT_STAGGER_WINDOW=$(( HTAP_LLT_COUNT * ${HTAP_LLT_STAGGER_SECS:-0} ))
if [ "$HTAP_WARMUP_DURATION" -lt "$LLT_STAGGER_WINDOW" ]; then
    log_info "HTAP_WARMUP_DURATION=${HTAP_WARMUP_DURATION}s is shorter than the LLT staggering window (${LLT_STAGGER_WINDOW}s), extending it so every OLAP run sees all ${HTAP_LLT_COUNT} snapshots"
    HTAP_WARMUP_DURATION=$LLT_STAGGER_WINDOW
fi

LLT_SLEEP_DURATION=$(( HTAP_WARMUP_DURATION + EFFECTIVE_OLAP_RUNS * HTAP_QUERY_TIMEOUT + FLUSH_SETTLE_OVERHEAD ))

# Log configuration
CONFIG_LOG="${RESULT_DIR}/profiling_config.log"
{
    echo "============================================================"
    echo "HTAP PROFILING CONFIGURATION LOG"
    echo "Generated: $(date)"
    echo "Engine: $ENGINE"
    echo "Workload: HTAP (sysbench OLTP + analytical join)"
    echo "============================================================"
    echo ""
    echo "============================================================"
    echo "SYSTEM INFORMATION"
    echo "============================================================"
    echo "Hostname: $(hostname)"
    echo "Kernel: $(uname -r)"
    echo "OS: $(grep PRETTY_NAME /etc/os-release 2>/dev/null | cut -d= -f2 | tr -d '"')"
    echo ""
    echo "CPU Info:"
    lscpu 2>/dev/null | grep -E "^(Model name|Socket|Core|Thread|CPU\(s\)|CPU MHz)"
    echo ""
    echo "Memory Info:"
    free -h 2>/dev/null
    echo ""
    echo "============================================================"
    echo "HTAP PARAMETERS"
    echo "============================================================"
    echo "HTAP_TABLES: $HTAP_TABLES"
    echo "HTAP_TABLE_SIZE: $HTAP_TABLE_SIZE"
    echo "HTAP_OLTP_THREADS: $HTAP_OLTP_THREADS"
    echo "HTAP_LLT_COUNT: $HTAP_LLT_COUNT"
    echo "HTAP_LLT_STAGGER_SECS: ${HTAP_LLT_STAGGER_SECS:-0}"
    echo "HTAP_OLTP_RAND_TYPE: ${HTAP_OLTP_RAND_TYPE:-uniform}"
    echo "HTAP_OLTP_INDEX_UPDATES: ${HTAP_OLTP_INDEX_UPDATES:-1} (0 = join key k frozen after prepare)"
    echo "HTAP_OLTP_DELETE_INSERTS: ${HTAP_OLTP_DELETE_INSERTS:-1}"
    echo "HTAP_OLTP_NON_INDEX_UPDATES: ${HTAP_OLTP_NON_INDEX_UPDATES:-1}"
    echo "HTAP_WARMUP_DURATION: $HTAP_WARMUP_DURATION"
    echo "HTAP_DURATION: $HTAP_DURATION"
    echo "HTAP_CTX_INTERVAL: $HTAP_CTX_INTERVAL"
    echo "HTAP_OLAP_RUNS: $HTAP_OLAP_RUNS"
    echo "HTAP_DIAGNOSE_RUN0: ${HTAP_DIAGNOSE_RUN0:-false} (Run 1/Run 3 timeout investigation diagnostic; RUN_START=${RUN_START})"
    echo "CUTOFF: $CUTOFF"
    echo "BENCHMARK_DB: $BENCHMARK_DB"
    echo "CGROUP_MEMORY_LIMIT (env.sh default -- NOT necessarily enforced, see next line): $CGROUP_MEMORY_LIMIT"
    echo "Effective cgroup memory limit (actual, this process): $(get_effective_memory_limit self)"
    echo "FLAMEGRAPH_DIR: $FLAMEGRAPH_DIR"
    echo "PERF_EVENT: ${PERF_EVENT}"
    echo "PERF_FREQ: ${HTAP_PERF_FREQ:-499} Hz"
    echo "PERF_DELAY: ${HTAP_PERF_DELAY}s (skip query init before recording)"
    echo "PERF_DURATION: ${HTAP_PERF_DURATION}s (steady-state recording window)"
    echo "PERF_CALL_GRAPH: ${PERF_CALL_GRAPH:-dwarf}"
    echo "NOTE: k index dropped on all tables (non-indexed join per AIDE paper)"
    echo "NOTE: LLTs hold GC back so versions accumulate across OLAP runs (version pressure visible in flamegraphs)"
    echo "NOTE: LLT sleep = HTAP_WARMUP_DURATION + HTAP_OLAP_RUNS * HTAP_QUERY_TIMEOUT + FLUSH_SETTLE_OVERHEAD = ${LLT_SLEEP_DURATION}s (covers full experimental window, including per-run re-flush waits)"
    echo "NOTE: OLTP rand-type=${HTAP_OLTP_RAND_TYPE:-uniform} (uniform spreads versions across all rows; pareto concentrates them on hot rows)"
    echo "NOTE: Analytical sessions use REPEATABLE-READ (per AIDE §6.3 + LLT paper §5.1)"
    echo "NOTE: Memtable flushed before OLAP phase + 30s compaction settling wait (ensures versions in SSTables, stable background I/O)"
    echo "NOTE: ANALYZE TABLE run before OLAP loop to stabilise optimizer row estimates (MyRocks SST sampling unreliable)"
    echo "NOTE: RocksDB perf context captured PER-SESSION inside OLAP heredoc (CTX_SPLIT/TABLE_SCHEMA anchors) — NOT from external monitor"
    echo "NOTE: CSD engine ctx_after parsed via second TABLE_SCHEMA header (QUERY_DONE not emitted by MySQL 8.4 batch mode)"
    echo "NOTE: CEMU counters (rocksdb_cemu_keys_seen/filtered) are global atomics; deltas computed from head-2/tail-2 of SHOW GLOBAL STATUS output"
    echo "NOTE: NVMeVirt counters (rocksdb_nvmevirt_keys_seen/filtered) are global atomics; no freeze_ns equivalent -- FLAX's v1 offload blocks only the calling thread, not the whole guest"
    echo "NOTE: Version growth loop uses probe scan (sbtest1 k<=1000) to measure per-probe internal_key_skipped_count growth; awk detects STAT_TYPE header by content (not NR==1) to handle multi-result-set --batch output"
    echo ""
    echo "============================================================"
    echo "MYSQL SERVER VARIABLES"
    echo "============================================================"
    mysql --socket="$SOCKET" -e "SHOW VARIABLES;" 2>/dev/null
    echo ""
} > "$CONFIG_LOG" 2>&1
log_info "Configuration logged to: $CONFIG_LOG"

MYSQLD_PID=$(cat "${PID_FILE}" 2>/dev/null || true)
# sudo'd: mysqld may run as root (e.g. the FLAX sandbox, MYSQL_DAEMON_USER=root
# -- csdvirt_init_dev() needs root for device-node access) while this script
# runs as a non-root user, which can't kill -0 a root-owned PID (EPERM) even
# though the process is alive -- confirmed 2026-07-21 via mysqld successfully
# answering SHOW VARIABLES just before this check falsely reported it dead.
if [ -z "$MYSQLD_PID" ] || ! ${BENCH_SUDO-sudo} kill -0 "$MYSQLD_PID" 2>/dev/null; then
    log_error "Cannot find mysqld PID"
    exit 1
fi
log_info "mysqld PID: $MYSQLD_PID"

check_mysqld_alive() {
    ${BENCH_SUDO-sudo} kill -0 "$MYSQLD_PID" 2>/dev/null || return 1
    mysql --socket="$SOCKET" --batch --skip-column-names --connect-timeout=5 \
        -e "SELECT 1;" 2>/dev/null | grep -q "^1$"
}

# ── Cleanup trap ──────────────────────────────────────────────────────────────

PERF_PID=""
SNAPSHOT_PID=""
SB_PID=""
LLT_PIDS=()

cleanup() {
    [ -n "$PERF_PID" ]     && { ${BENCH_SUDO-sudo} kill -INT "$PERF_PID" 2>/dev/null; wait "$PERF_PID" 2>/dev/null || true; }
    [ -n "$SNAPSHOT_PID" ] && { kill "$SNAPSHOT_PID" 2>/dev/null; wait "$SNAPSHOT_PID" 2>/dev/null || true; }
    for pid in "${LLT_PIDS[@]}"; do kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null || true; done
    [ -n "$SB_PID" ]       && { kill "$SB_PID" 2>/dev/null; wait "$SB_PID" 2>/dev/null || true; }
    stop_monitors
    generate_resource_summary "$RESULT_DIR" "profiling_htap"
    stop_mysql || true
}
trap cleanup EXIT

# ── Phase 3: Start OLTP background ───────────────────────────────────────────

OLTP_TOTAL=$(( LLT_SLEEP_DURATION + 60 ))
log_info "Starting OLTP background (${HTAP_OLTP_THREADS} threads, ${OLTP_TOTAL}s)..."

sysbench oltp_read_write \
    --mysql-socket="$SOCKET" \
    --mysql-db="$BENCHMARK_DB" \
    --tables="$HTAP_TABLES" \
    --table-size="$HTAP_TABLE_SIZE" \
    --threads="$HTAP_OLTP_THREADS" \
    --time="$OLTP_TOTAL" \
    --rand-type="${HTAP_OLTP_RAND_TYPE:-uniform}" \
    --index_updates="${HTAP_OLTP_INDEX_UPDATES:-1}" \
    --delete_inserts="${HTAP_OLTP_DELETE_INSERTS:-1}" \
    --non_index_updates="${HTAP_OLTP_NON_INDEX_UPDATES:-1}" \
    --report-interval=10 \
    --db-ps-mode=disable \
    run > "${RESULT_DIR}/sysbench_htap_oltp.txt" 2>&1 &
SB_PID=$!
log_info "OLTP sysbench PID: $SB_PID"

# ── Phase 4: Open Long-Lived Transactions ─────────────────────────────────────

log_info "Opening ${HTAP_LLT_COUNT} long-lived transactions (LLTs), staggered ${HTAP_LLT_STAGGER_SECS:-0}s apart..."
LLT_PIDS=()
LLT_STAGGER=${HTAP_LLT_STAGGER_SECS:-0}
for (( i=1; i<=HTAP_LLT_COUNT; i++ )); do
    # wait_timeout covers idle connections; net_read_timeout/net_write_timeout
    # cover active long-running queries (SELECT SLEEP is an active query).
    #
    # The reads are load-bearing: MyRocks acquires the snapshot on the first table
    # access, so `START TRANSACTION; SELECT SLEEP(n)` alone pins nothing. The leading
    # sleep puts each snapshot on a distinct sequence number.
    llt_offset=$(( (i - 1) * LLT_STAGGER ))
    llt_remaining=$(( LLT_SLEEP_DURATION - llt_offset ))
    [ "$llt_remaining" -lt 1 ] && llt_remaining=1
    mysql --socket="$SOCKET" "$BENCHMARK_DB" \
        --batch --force >/dev/null 2>&1 <<SQL &
SET SESSION transaction_isolation='REPEATABLE-READ';
SET SESSION wait_timeout=86400;
SET SESSION net_read_timeout=86400;
SET SESSION net_write_timeout=86400;
SELECT SLEEP(${llt_offset});
START TRANSACTION;
SELECT COUNT(*) FROM sbtest1 WHERE id < 2;
SELECT COUNT(*) FROM sbtest2 WHERE id < 2;
SELECT COUNT(*) FROM sbtest3 WHERE id < 2;
SELECT COUNT(*) FROM sbtest4 WHERE id < 2;
SELECT SLEEP(${llt_remaining});
ROLLBACK;
SQL
    LLT_PIDS+=($!)
    log_info "  LLT $i PID: ${LLT_PIDS[-1]} (snapshot at t+${llt_offset}s)"
done
log_info "LLTs launched; all ${HTAP_LLT_COUNT} snapshots in place by t+$(( (HTAP_LLT_COUNT - 1) * LLT_STAGGER ))s"

# ── Phase 5: Warmup ───────────────────────────────────────────────────────────

log_info "Warming up for ${HTAP_WARMUP_DURATION}s (OLTP writes accumulating versions)..."
sleep "$HTAP_WARMUP_DURATION"

if ! kill -0 "$SB_PID" 2>/dev/null; then
    log_error "OLTP sysbench died during warmup — aborting"
    exit 1
fi
log_info "Warmup complete. OLTP still running."

# Flush RocksDB memtable to SSTables before OLAP phase.
# FindNextUserEntry runs in both cases (memtable and SSTable), but the per-step
# cost differs: memtable versions are traversed via in-memory skip list (cheap),
# while SSTable versions require block cache lookups and possible block reads
# (expensive). Without flushing, FindNextUserEntry accumulates less CPU time and
# appears smaller in the flamegraph than it would under real HTAP pressure where
# versions have been compacted to SSTables.
if [ "$IS_MYROCKS" = "true" ]; then
    log_info "Flushing RocksDB memtable to SSTables before OLAP phase..."
    mysql --socket="$SOCKET" \
        -e "SET GLOBAL rocksdb_force_flush_memtable_now = 1;" 2>/dev/null || \
        log_error "  WARNING: memtable flush failed — version traversal may be underrepresented in flamegraph"
    log_info "Memtable flushed"
    # After flushing, background compaction kicks in to compact the newly-created
    # L0 SSTs.  Without a settling wait, compaction can consume 400+ MB/s write
    # bandwidth throughout the OLAP phase, making resource metrics and run times
    # non-repeatable.  30 s is enough for the immediate compaction burst to pass.
    log_info "Waiting 30s for background compaction to settle..."
    sleep 30
    log_info "Compaction settling wait complete"
fi

# ── Phase 6: Periodic perf context snapshots (background loop) ───────────────

log_info "Starting version growth snapshot loop (interval: ${HTAP_CTX_INTERVAL}s)..."
(
    snapshot_num=0
    while sleep "$HTAP_CTX_INTERVAL"; do
        snapshot_num=$(( snapshot_num + 1 ))
        elapsed=$(( snapshot_num * HTAP_CTX_INTERVAL + HTAP_WARMUP_DURATION ))
        llt_alive=0
        for pid in "${LLT_PIDS[@]}"; do
            kill -0 "$pid" 2>/dev/null && llt_alive=$(( llt_alive + 1 ))
        done
        if [ "$IS_MYROCKS" = "true" ]; then
            # information_schema.rocksdb_perf_context is per-session, not global.
            # Querying it from a background monitor always returns zeros.
            # Instead, run a small probe scan (k <= 1000, ~1% of sbtest1) in a
            # fresh REPEATABLE-READ session.  Each mysql invocation resets the
            # per-session perf context, so the counters reflect only this probe.
            # internal_key_skipped_count will grow over time as OLTP accumulates
            # more versions that the probe scan must traverse and skip.
            # Probe scan: run a small range scan in REPEATABLE-READ, then read the
            # per-session perf context for THIS connection (fresh each interval).
            # ROCKSDB_PERF_CONTEXT is columnar — SELECT * and parse headers.
            # Run the probe scan and perf context read in one session so that
            # the perf context reflects this connection's scan activity.
            # Two queries → two result sets in --batch output; use a heredoc so
            # we can embed newlines cleanly.
            ctx=$(mysql --socket="$SOCKET" "$BENCHMARK_DB" \
                --batch 2>/dev/null <<'PROBE_SQL'
SET SESSION rocksdb_perf_context_level = 3;
SET SESSION transaction_isolation = 'REPEATABLE-READ';
SET SESSION max_execution_time = 5000;
SELECT COUNT(*) FROM sbtest1 WHERE k <= 1000;
SELECT * FROM information_schema.ROCKSDB_PERF_CONTEXT
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'sbtest1';
PROBE_SQL
            ) || true
            # ROCKSDB_PERF_CONTEXT schema: (TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, STAT_TYPE, VALUE)
            # --batch prints both result sets: COUNT(*) first, then the perf context rows.
            # NR==1 would be the COUNT(*) header, so we must detect the perf context header
            # by looking for the line that contains "STAT_TYPE" rather than assuming NR==1.
            _perf_ctx_val() {
                local metric=$1
                echo "$ctx" | awk -v m="$metric" '
                    toupper($0) ~ /STAT_TYPE/ && toupper($0) ~ /VALUE/ && !st {
                        for(i=1;i<=NF;i++) {
                            if(toupper($i)=="STAT_TYPE") st=i
                            if(toupper($i)=="VALUE")     vl=i
                        }
                        next
                    }
                    st && vl && toupper($st)==toupper(m) { print $vl+0; exit }
                '
            }
            iksc=$(_perf_ctx_val "INTERNAL_KEY_SKIPPED_COUNT")
            idsc=$(_perf_ctx_val "INTERNAL_DELETE_SKIPPED_COUNT")
            brc=$( _perf_ctx_val "BLOCK_READ_COUNT")
        else
            ctx=$(snapshot_perf_context_global)
            # InnoDB: map global counters to CSV columns
            iksc=$(echo "$ctx" | awk 'toupper($1)=="INNODB_ROWS_READ"{print $2+0}')
            idsc=$(echo "$ctx" | awk 'toupper($1)=="INNODB_ROWS_DELETED"{print $2+0}')
            brc=$( echo "$ctx" | awk 'toupper($1)=="INNODB_BUFFER_POOL_READS"{print $2+0}')
        fi
        echo "${snapshot_num},${elapsed},$(date +%s),${iksc:-0},${idsc:-0},${brc:-0},${llt_alive}" \
            >> "${RESULT_DIR}/htap_version_growth.csv"
    done
) &
SNAPSHOT_PID=$!
log_info "Snapshot loop PID: $SNAPSHOT_PID"

# ── Phase 7: Analytical query profiling loop ──────────────────────────────────

JOIN4_CONTENT=$(cat "$JOIN4_SQL")
MAX_JOIN_SIZE_SQL=""
[ -n "${HTAP_MAX_JOIN_SIZE:-}" ] && MAX_JOIN_SIZE_SQL="SET SESSION max_join_size=${HTAP_MAX_JOIN_SIZE};"

log_info "Starting OLAP profiling loop (${HTAP_OLAP_RUNS} runs$([ "$RUN_START" -eq 0 ] && echo " + diagnostic Run 0"), cutoff=${CUTOFF})..."

declare -A PERF_ELAPSED  # elapsed time per run, used for deferred flamegraph titles

for RUN in $(seq "$RUN_START" "$HTAP_OLAP_RUNS"); do
    log_info "── OLAP Run ${RUN}/${HTAP_OLAP_RUNS} ──────────────────────────────"

    # Abort immediately if MySQL is gone — remaining runs would produce zeros
    if ! check_mysqld_alive; then
        log_error "MySQL is not responding — aborting remaining runs"
        break
    fi

    # Check OLTP still alive
    if ! kill -0 "$SB_PID" 2>/dev/null; then
        log_error "  WARNING: OLTP sysbench died before run ${RUN}"
    fi

    # Check active LLTs
    llt_alive=0
    for pid in "${LLT_PIDS[@]}"; do
        kill -0 "$pid" 2>/dev/null && llt_alive=$(( llt_alive + 1 ))
    done
    if [ "$llt_alive" -lt "$HTAP_LLT_COUNT" ]; then
        log_error "  WARNING: Only ${llt_alive}/${HTAP_LLT_COUNT} LLTs still alive at run ${RUN}"
    fi

    # Re-flush the memtable before every run after the first.
    # The pre-loop flush (above) only covers Run 1 — OLTP keeps writing for the
    # ~HTAP_QUERY_TIMEOUT duration of each prior run, so by Run 2/3 the memtable
    # has re-accumulated a run's worth of new versions on top of the SSTable
    # chains. Memtable-resident versions are traversed via the host's own
    # unaccelerated FindNextUserEntry regardless of engine (CSD/NVMeVirt offload
    # only covers on-disk SST reads), so leaving them unflushed understates SST
    # version pressure AND adds unaccelerated CPU work each run — a likely
    # contributor to OLAP runs approaching/exceeding HTAP_QUERY_TIMEOUT.
    if [ "$IS_MYROCKS" = "true" ] && [ "$RUN" -gt 1 ]; then
        log_info "Flushing RocksDB memtable to SSTables before run ${RUN}..."
        mysql --socket="$SOCKET" \
            -e "SET GLOBAL rocksdb_force_flush_memtable_now = 1;" 2>/dev/null || \
            log_error "  WARNING: memtable flush failed before run ${RUN}"
        log_info "Waiting 30s for background compaction to settle..."
        sleep 30
    fi

    # Every run measures cold storage reads, not cache hits. Run 1 already
    # starts cold via start_mysql_cold(); reset both cache layers before
    # every later run too.
    if [ "$IS_MYROCKS" = "true" ] && [ "$RUN" -gt 1 ]; then
        drop_page_cache
        orig_cache_size=$(mysql --socket="$SOCKET" -N -e \
            "SELECT @@global.rocksdb_block_cache_size;" 2>/dev/null)
        mysql --socket="$SOCKET" -e "SET GLOBAL rocksdb_block_cache_size = 1024;" 2>/dev/null || \
            log_error "  WARNING: block cache shrink failed before run ${RUN}"
        mysql --socket="$SOCKET" -e "SET GLOBAL rocksdb_block_cache_size = ${orig_cache_size};" 2>/dev/null || \
            log_error "  WARNING: block cache restore failed before run ${RUN}"
        log_info "Cold start before run ${RUN}: page cache dropped, block cache cleared (restored to ${orig_cache_size})"
    fi

    # InnoDB: snapshot global status before query
    innodb_before=""
    if [ "$ENGINE" = "percona-innodb" ]; then
        innodb_before=$(mysql --socket="$SOCKET" --batch --skip-column-names 2>/dev/null -e "
            SHOW GLOBAL STATUS WHERE Variable_name IN (
                'Innodb_rows_read',
                'Innodb_buffer_pool_reads',
                'Innodb_buffer_pool_read_requests',
                'Innodb_pages_read',
                'Innodb_data_reads',
                'Innodb_data_read'
            );") || true
        [ -z "$innodb_before" ] && log_error "  WARNING: innodb_before snapshot empty"
    fi

    # Capture the plan MySQL would choose for this run, and the RocksDB
    # LSM/compaction state going into it, BEFORE the timed execution below.
    # EXPLAIN doesn't execute the query, so this is cheap even though the real
    # run can take minutes to hours. Captured every run (not just once) because
    # RocksDB's own live per-CF cardinality estimates drift under concurrent
    # OLTP writes, and Run 1 has been observed to cost far more than later runs
    # for reasons not yet root-caused -- see
    # flax_baremetal_htap_hardened_offload_findings_20260803.md's open items.
    if [ "$IS_MYROCKS" = "true" ]; then
        mysql --socket="$SOCKET" "$BENCHMARK_DB" --batch 2>/dev/null > "${RESULT_DIR}/explain_run${RUN}.txt" <<SQL
SET @htap_cutoff = ${CUTOFF};
EXPLAIN FORMAT=TREE ${JOIN4_CONTENT}
SQL
        mysql --socket="$SOCKET" --batch --skip-column-names 2>/dev/null \
            -e "SHOW ENGINE ROCKSDB STATUS;" > "${RESULT_DIR}/rocksdb_status_run${RUN}_before.txt"

        read -r sst_entries version_amp <<<"$(measure_version_amplification)"
        log_info "  Run ${RUN}: offload-CF SST entries=${sst_entries:-0} for $(( HTAP_TABLE_SIZE * 4 )) live rows (version amplification ${version_amp:-0}x)"
        # Amplification A caps the achievable filter ratio at (A-1)/A, so only flag
        # values near 1.0, where nothing is droppable.
        if awk "BEGIN{exit !(${version_amp:-0} < 1.2)}"; then
            log_error "  WARNING: version amplification ${version_amp:-0}x leaves at most $(awk "BEGIN{printf \"%.2f\", (${version_amp:-1}-1)/${version_amp:-1}}") of entries droppable."
            log_error "           Check that the LLTs are actually holding RocksDB snapshots (ROCKSDB_TRX should be non-zero)."
        fi
    fi

    # Memory snapshot going into this run -- see snapshot_memory_state's own
    # comment for why (untested variable in the Run 1/Run 3 investigation).
    snapshot_memory_state "${RESULT_DIR}/memory_run${RUN}_before.txt"

    # Start perf record attached to mysqld
    perf_data="${RESULT_DIR}/perf_htap_run${RUN}.data"
    ${BENCH_SUDO-sudo} perf record -F "${HTAP_PERF_FREQ:-499}" -p "$MYSQLD_PID" --call-graph "${PERF_CALL_GRAPH:-dwarf}" \
        -e "${PERF_EVENT}" \
        -D $((HTAP_PERF_DELAY * 1000)) \
        -o "$perf_data" -- sleep $((HTAP_PERF_DURATION + HTAP_PERF_DELAY)) &
    PERF_PID=$!
    sleep 0.5   # let perf attach before query starts

    start_time=$(date +%s.%N)

    # HTAP_DIAGNOSE_RUN0 substitutes EXPLAIN ANALYZE for the plain query on
    # every run -- it genuinely executes, so downstream measurements stay real.
    QUERY_TO_RUN="$JOIN4_CONTENT"
    EXPLAIN_ANALYZE_THIS_RUN=false
    if [ "${HTAP_DIAGNOSE_RUN0:-false}" = "true" ]; then
        QUERY_TO_RUN="EXPLAIN ANALYZE ${JOIN4_CONTENT}"
        EXPLAIN_ANALYZE_THIS_RUN=true
        log_info "  Run ${RUN}: capturing EXPLAIN ANALYZE"
    fi

    # Run the analytical query. information_schema.rocksdb_perf_context is
    # PER-SESSION -- capturing it from an external connection always returns
    # zeros, so it's snapshotted within this session before/after the join,
    # split by the CTX_SPLIT sentinel so the shell can compute per-run deltas.
    # CSD engine additionally reads global CEMU counters (rocksdb_cemu_keys_*,
    # atomics incremented by CemuResultIterator destructors) the same way.
    if [ "$ENGINE" = "percona-myrocks" ]; then
        # ROCKSDB_PERF_CONTEXT schema: (TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, STAT_TYPE, VALUE).
        # Query with SELECT * filtered to the four join tables; _ctx_delta sums VALUE
        # where STAT_TYPE matches the requested metric name.
        raw_output=$(mysql --socket="$SOCKET" "$BENCHMARK_DB" \
            --batch --force 2>"${RESULT_DIR}/olap_query_stderr_run${RUN}.txt" <<SQL
SET SESSION transaction_isolation='REPEATABLE-READ';
SET SESSION rocksdb_perf_context_level=${PROFILING_PERF_CONTEXT_LEVEL};
SET SESSION max_execution_time=$((HTAP_QUERY_TIMEOUT * 1000));
${MAX_JOIN_SIZE_SQL}
SET @htap_cutoff = ${CUTOFF};
SELECT * FROM information_schema.ROCKSDB_PERF_CONTEXT
WHERE TABLE_SCHEMA = '${BENCHMARK_DB}'
  AND TABLE_NAME IN ('sbtest1','sbtest2','sbtest3','sbtest4');
SELECT 'CTX_SPLIT' AS ctx_marker;
FLUSH STATUS;
SELECT 'QUERY_BEGIN' AS q_marker;
${QUERY_TO_RUN}
SELECT 'QUERY_END' AS q_marker;
SELECT * FROM information_schema.ROCKSDB_PERF_CONTEXT
WHERE TABLE_SCHEMA = '${BENCHMARK_DB}'
  AND TABLE_NAME IN ('sbtest1','sbtest2','sbtest3','sbtest4');
SHOW SESSION STATUS LIKE 'Handler_read_first';
SHOW SESSION STATUS LIKE 'Handler_read_next';
SHOW SESSION STATUS LIKE 'Handler_read_rnd_next';
SELECT 'CONN_ID_MARKER' AS marker, CONNECTION_ID() AS olap_connection_id;
SQL
        )
    elif [ "$ENGINE" = "percona-myrocks-csd" ]; then
        raw_output=$(mysql --socket="$SOCKET" "$BENCHMARK_DB" \
            --batch --force 2>"${RESULT_DIR}/olap_query_stderr_run${RUN}.txt" <<SQL
SET SESSION transaction_isolation='REPEATABLE-READ';
SET SESSION rocksdb_perf_context_level=${PROFILING_PERF_CONTEXT_LEVEL};
SET SESSION max_execution_time=$((HTAP_QUERY_TIMEOUT * 1000));
${MAX_JOIN_SIZE_SQL}
SET @htap_cutoff = ${CUTOFF};
SELECT * FROM information_schema.ROCKSDB_PERF_CONTEXT
WHERE TABLE_SCHEMA = '${BENCHMARK_DB}'
  AND TABLE_NAME IN ('sbtest1','sbtest2','sbtest3','sbtest4');
SELECT 'CTX_SPLIT' AS ctx_marker;
SHOW GLOBAL STATUS LIKE 'Rocksdb_cemu_keys%';
SHOW GLOBAL STATUS LIKE 'Rocksdb_cemu_freeze%';
SELECT 'CSD_SPLIT' AS csd_marker;
FLUSH STATUS;
SELECT 'QUERY_BEGIN' AS q_marker;
${QUERY_TO_RUN}
SELECT 'QUERY_END' AS q_marker;
SELECT * FROM information_schema.ROCKSDB_PERF_CONTEXT
WHERE TABLE_SCHEMA = '${BENCHMARK_DB}'
  AND TABLE_NAME IN ('sbtest1','sbtest2','sbtest3','sbtest4');
SHOW GLOBAL STATUS LIKE 'Rocksdb_cemu_keys%';
SHOW GLOBAL STATUS LIKE 'Rocksdb_cemu_freeze%';
SHOW SESSION STATUS LIKE 'Handler_read_first';
SHOW SESSION STATUS LIKE 'Handler_read_next';
SHOW SESSION STATUS LIKE 'Handler_read_rnd_next';
SELECT 'CONN_ID_MARKER' AS marker, CONNECTION_ID() AS olap_connection_id;
SQL
        )
    elif [ "$ENGINE" = "percona-myrocks-nvmevirt" ]; then
        # Same layout as the CSD branch, minus the freeze counter (no VM-wide
        # freeze in FLAX's v1 offload). rocksdb_nvmevirt_olap_session=1 scopes
        # offload eligibility to just this connection, keeping the 24 OLTP
        # threads off g_nvmevirt_exec_mutex; rocksdb_nvmevirt_enabled (GLOBAL,
        # set earlier) is still required too.
        raw_output=$(mysql --socket="$SOCKET" "$BENCHMARK_DB" \
            --batch --force 2>"${RESULT_DIR}/olap_query_stderr_run${RUN}.txt" <<SQL
SET SESSION transaction_isolation='REPEATABLE-READ';
SET SESSION rocksdb_perf_context_level=${PROFILING_PERF_CONTEXT_LEVEL};
SET SESSION max_execution_time=$((HTAP_QUERY_TIMEOUT * 1000));
${MAX_JOIN_SIZE_SQL}
SET SESSION rocksdb_nvmevirt_olap_session=1;
SET @htap_cutoff = ${CUTOFF};
SELECT * FROM information_schema.ROCKSDB_PERF_CONTEXT
WHERE TABLE_SCHEMA = '${BENCHMARK_DB}'
  AND TABLE_NAME IN ('sbtest1','sbtest2','sbtest3','sbtest4');
SELECT 'CTX_SPLIT' AS ctx_marker;
SHOW GLOBAL STATUS LIKE 'Rocksdb_nvmevirt_keys%';
SELECT 'CSD_SPLIT' AS csd_marker;
FLUSH STATUS;
SELECT 'QUERY_BEGIN' AS q_marker;
${QUERY_TO_RUN}
SELECT 'QUERY_END' AS q_marker;
SELECT * FROM information_schema.ROCKSDB_PERF_CONTEXT
WHERE TABLE_SCHEMA = '${BENCHMARK_DB}'
  AND TABLE_NAME IN ('sbtest1','sbtest2','sbtest3','sbtest4');
SHOW GLOBAL STATUS LIKE 'Rocksdb_nvmevirt_keys%';
SHOW SESSION STATUS LIKE 'Handler_read_first';
SHOW SESSION STATUS LIKE 'Handler_read_next';
SHOW SESSION STATUS LIKE 'Handler_read_rnd_next';
SELECT 'CONN_ID_MARKER' AS marker, CONNECTION_ID() AS olap_connection_id;
SQL
        )
    else
        raw_output=$(mysql --socket="$SOCKET" "$BENCHMARK_DB" \
            --batch --skip-column-names --force 2>"${RESULT_DIR}/olap_query_stderr_run${RUN}.txt" <<SQL
SET SESSION transaction_isolation='REPEATABLE-READ';
SET SESSION max_execution_time=$((HTAP_QUERY_TIMEOUT * 1000));
${MAX_JOIN_SIZE_SQL}
SET @htap_cutoff = ${CUTOFF};
FLUSH STATUS;
SELECT 'QUERY_BEGIN' AS q_marker;
${JOIN4_CONTENT}
SELECT 'QUERY_END' AS q_marker;
SHOW SESSION STATUS LIKE 'Handler_read_first';
SHOW SESSION STATUS LIKE 'Handler_read_next';
SHOW SESSION STATUS LIKE 'Handler_read_rnd_next';
SHOW SESSION STATUS LIKE 'Handler_read_key';
SQL
        )
    fi

    end_time=$(date +%s.%N)
    elapsed=$(echo "$end_time - $start_time" | bc)
    PERF_ELAPSED[$RUN]=$elapsed

    # Did the query produce an answer? Without this, a failing query is timed as if it
    # succeeded, since `--force` plus a discarded stderr hides the error.
    query_result=$(echo "$raw_output" \
        | awk '/^QUERY_BEGIN/{f=1;next} /^QUERY_END/{exit} f && !/^q_marker$/' | tail -1)
    if [ -n "$query_result" ]; then
        query_ok=1
        echo "$query_result" > "${RESULT_DIR}/olap_result_run${RUN}.txt"
        log_info "  Run ${RUN}: query returned ${query_result}"
    else
        query_ok=0
        log_error "  ERROR: run ${RUN} produced NO result row; the analytical query did not complete."
        log_error "         elapsed=${elapsed}s is time-to-failure, not query time. Do not compare it."
        if [ -s "${RESULT_DIR}/olap_query_stderr_run${RUN}.txt" ]; then
            log_error "         client stderr: $(head -3 "${RESULT_DIR}/olap_query_stderr_run${RUN}.txt" | tr '\n' ' ')"
        else
            log_error "         client stderr was empty; check max_execution_time (${HTAP_QUERY_TIMEOUT}s) and mysqld_error.log"
        fi
    fi

    # Extract the EXPLAIN ANALYZE tree (actual per-operator row counts/
    # timings/loop counts). It sits between the CSD_SPLIT marker and the
    # next TABLE_SCHEMA header (the ctx_after ROCKSDB_PERF_CONTEXT query).
    if [ "$EXPLAIN_ANALYZE_THIS_RUN" = "true" ]; then
        # TEMPORARY debug dump -- remove once the extraction below is confirmed reliable.
        echo "$raw_output" > "${RESULT_DIR}/raw_output_debug_run${RUN}.txt"
        echo "$raw_output" | awk '/^QUERY_BEGIN/{f=1;next} /^QUERY_END/{exit} f&&/^q_marker/{next} f{print}' \
            > "${RESULT_DIR}/explain_analyze_run${RUN}.txt"
        [ -s "${RESULT_DIR}/explain_analyze_run${RUN}.txt" ] || \
            log_error "  WARNING: explain_analyze_run${RUN}.txt is empty -- check raw_output_debug_run${RUN}.txt"
        log_info "  Run ${RUN}: EXPLAIN ANALYZE saved to ${RESULT_DIR}/explain_analyze_run${RUN}.txt"
    fi

    # This run's own MySQL CONNECTION_ID(), captured via the CONN_ID_MARKER
    # SELECT -- must stay the last statement in every heredoc above (moving it
    # earlier would shift NR==1 for _ctx_delta's header-detection awk below
    # and silently zero out every perf-context metric; confirmed by tracing
    # that awk before adding this). This is the same value rdb_iterator_debug.log's "thd=" field
    # logs as this connection's THD::thread_id() -- confirmed identical via
    # sql_class.cc (CONNECTION_ID() returns pseudo_thread_id, initialized to
    # exactly m_thread_id for any normal connection) -- letting this run's
    # own iterator-recreation events be isolated from the 24 concurrent OLTP
    # threads' events in that log.
    olap_connection_id=$(echo "$raw_output" | awk -F'\t' '$1=="CONN_ID_MARKER"{print $2}')
    echo "$olap_connection_id" > "${RESULT_DIR}/olap_connection_id_run${RUN}.txt"
    log_info "  Run ${RUN}: OLAP connection/thread id = ${olap_connection_id:-UNKNOWN}"

    # RocksDB LSM/compaction state right after the query -- paired with the
    # "_before" snapshot above to see how much L0/compaction backlog built up
    # during this specific run's execution window.
    if [ "$IS_MYROCKS" = "true" ]; then
        mysql --socket="$SOCKET" --batch --skip-column-names 2>/dev/null \
            -e "SHOW ENGINE ROCKSDB STATUS;" > "${RESULT_DIR}/rocksdb_status_run${RUN}_after.txt"
    fi

    # Memory snapshot right after the query -- paired with the "_before" one
    # above to see how much mysqld RSS/page-cache/cgroup usage grew during
    # this specific run's execution window.
    snapshot_memory_state "${RESULT_DIR}/memory_run${RUN}_after.txt"

    # For MyRocks: split raw_output at CTX_SPLIT to get before/after perf context.
    # The ROCKSDB_PERF_CONTEXT table is columnar — column names depend on the build.
    # We store the raw sections verbatim; _ctx_delta reads named columns below.
    ctx_before=""
    ctx_after=""
    csd_seen_delta=0
    csd_filt_delta=0
    csd_freeze_delta=0
    nv_seen_delta=0
    nv_filt_delta=0
    if [ "$IS_MYROCKS" = "true" ]; then
        ctx_before=$(echo "$raw_output" | awk '/^CTX_SPLIT/{exit} {print}')
        ctx_after=$(echo  "$raw_output" | awk 'f && /^Handler_/{exit} f{print} /^CTX_SPLIT/{f=1}')
    fi

    # For either device-offload engine (CSD or NVMeVirt): override ctx_after
    # using the second TABLE_SCHEMA header as anchor. Both engines' SQL layout is:
    #   ctx_before (TABLE_SCHEMA) | CTX_SPLIT | device_counters_before | CSD_SPLIT |
    #   join query | ctx_after (TABLE_SCHEMA) | device_counters_after (Variable_name) | Handler_*
    #
    # MySQL 8.4 batch mode does not emit the join query's result row in the captured
    # output, so QUERY_DONE (removed) cannot serve as a reliable sentinel.  The second
    # TABLE_SCHEMA line is the ctx_after ROCKSDB_PERF_CONTEXT header; we stop at the
    # first Variable_name that follows it (the device_counters_after SHOW GLOBAL STATUS block).
    if [ "$IS_DEVICE_OFFLOAD" = "true" ]; then
        ctx_after=$(echo "$raw_output" | awk '/^TABLE_SCHEMA/{if(++c==2)f=1} f&&/^Variable_name/{exit} f{print}')
    fi

    # For the CSD engine: parse CEMU global counter deltas.
    # SHOW GLOBAL STATUS LIKE 'Rocksdb_cemu_keys%' runs twice (before and after
    # the join), producing exactly four matching lines in raw_output.  head -2 gives
    # the before snapshot; tail -2 gives the after snapshot.
    if [ "$IS_CSD" = "true" ]; then
        csd_raw_before=$(echo "$raw_output" | grep -i "rocksdb_cemu_keys" | head -2)
        csd_raw_after=$( echo "$raw_output" | grep -i "rocksdb_cemu_keys" | tail -2)
        _csd_val() {
            local section=$1 key=$2
            echo "$section" | awk -v k="$key" 'toupper($1)==toupper(k){print $2+0; exit}'
        }
        csd_seen_before=$(_csd_val "$csd_raw_before" "Rocksdb_cemu_keys_seen")
        csd_filt_before=$(_csd_val "$csd_raw_before" "Rocksdb_cemu_keys_filtered")
        csd_seen_after=$( _csd_val "$csd_raw_after"  "Rocksdb_cemu_keys_seen")
        csd_filt_after=$( _csd_val "$csd_raw_after"  "Rocksdb_cemu_keys_filtered")
        csd_seen_delta=$(awk "BEGIN{printf \"%.0f\n\", ${csd_seen_after:-0} - ${csd_seen_before:-0}}")
        csd_filt_delta=$(awk "BEGIN{printf \"%.0f\n\", ${csd_filt_after:-0} - ${csd_filt_before:-0}}")
        csd_freeze_raw_before=$(echo "$raw_output" | grep -i "rocksdb_cemu_freeze" | head -1)
        csd_freeze_raw_after=$( echo "$raw_output" | grep -i "rocksdb_cemu_freeze" | tail -1)
        csd_freeze_before=$(_csd_val "$csd_freeze_raw_before" "Rocksdb_cemu_freeze_ns")
        csd_freeze_after=$( _csd_val "$csd_freeze_raw_after"  "Rocksdb_cemu_freeze_ns")
        csd_freeze_delta=$(awk "BEGIN{printf \"%.0f\n\", ${csd_freeze_after:-0} - ${csd_freeze_before:-0}}")
    fi

    # For the NVMeVirt engine: parse global counter deltas the same way as
    # CSD above (SHOW GLOBAL STATUS LIKE 'Rocksdb_nvmevirt_keys%' runs twice,
    # before and after the join -- head -2/tail -2 gives before/after). No
    # freeze counter to parse.
    if [ "$IS_NVMEVIRT" = "true" ]; then
        nv_raw_before=$(echo "$raw_output" | grep -i "rocksdb_nvmevirt_keys" | head -2)
        nv_raw_after=$( echo "$raw_output" | grep -i "rocksdb_nvmevirt_keys" | tail -2)
        _nv_val() {
            local section=$1 key=$2
            echo "$section" | awk -v k="$key" 'toupper($1)==toupper(k){print $2+0; exit}'
        }
        nv_seen_before=$(_nv_val "$nv_raw_before" "Rocksdb_nvmevirt_keys_seen")
        nv_filt_before=$(_nv_val "$nv_raw_before" "Rocksdb_nvmevirt_keys_filtered")
        nv_seen_after=$( _nv_val "$nv_raw_after"  "Rocksdb_nvmevirt_keys_seen")
        nv_filt_after=$( _nv_val "$nv_raw_after"  "Rocksdb_nvmevirt_keys_filtered")
        nv_seen_delta=$(awk "BEGIN{printf \"%.0f\n\", ${nv_seen_after:-0} - ${nv_seen_before:-0}}")
        nv_filt_delta=$(awk "BEGIN{printf \"%.0f\n\", ${nv_filt_after:-0} - ${nv_filt_before:-0}}")
    fi

    # Save raw output; append parsed before/after for human inspection
    echo "$raw_output" > "${RESULT_DIR}/perf_ctx_raw_run${RUN}.txt"
    if [ "$IS_MYROCKS" = "true" ]; then
        { echo "# ctx_before"; echo "$ctx_before"; echo "# ctx_after"; echo "$ctx_after"; } \
            >> "${RESULT_DIR}/perf_ctx_raw_run${RUN}.txt"
    fi

    # InnoDB: snapshot global status after query
    innodb_after=""
    if [ "$ENGINE" = "percona-innodb" ]; then
        innodb_after=$(mysql --socket="$SOCKET" --batch --skip-column-names 2>/dev/null -e "
            SHOW GLOBAL STATUS WHERE Variable_name IN (
                'Innodb_rows_read',
                'Innodb_buffer_pool_reads',
                'Innodb_buffer_pool_read_requests',
                'Innodb_pages_read',
                'Innodb_data_reads',
                'Innodb_data_read'
            );") || true
        { echo "# before"; echo "$innodb_before"; echo "# after"; echo "$innodb_after"; } \
            > "${RESULT_DIR}/innodb_global_raw_run${RUN}.txt"
    fi

    # Stop perf recording
    ${BENCH_SUDO-sudo} kill -INT "$PERF_PID" 2>/dev/null || true
    wait "$PERF_PID" 2>/dev/null || true
    PERF_PID=""

    # Warn if query was too short for perf to record meaningful data
    elapsed_int=$(echo "$elapsed" | awk '{printf "%d", $1}')
    if [ "${elapsed_int:-0}" -lt 2 ]; then
        log_error "  WARNING: query completed in <2s (${elapsed}s) — perf may not have attached in time"
    elif [ "${elapsed_int:-0}" -lt "${HTAP_PERF_DELAY:-30}" ]; then
        log_error "  WARNING: query completed in ${elapsed}s but PERF_DELAY=${HTAP_PERF_DELAY:-30}s — flamegraph will be empty (perf starts recording after query finishes)"
    fi

    # Extract metrics
    _get() { echo "$raw_output" | awk -v k="$1" 'NF==2 && $1==k{print $2}'; }
    # _ctx_delta: compute per-run delta from ROCKSDB_PERF_CONTEXT output.
    # Schema: (TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, STAT_TYPE, VALUE) — key-value,
    # NOT columnar. We find the STAT_TYPE and VALUE column indices from the header row,
    # sum VALUE across all matching rows (four join tables), and return after - before.
    _ctx_delta() {
        local metric=$1 bv av
        local _awk='NR==1{for(i=1;i<=NF;i++){if(toupper($i)=="STAT_TYPE")st=i;if(toupper($i)=="VALUE")vl=i}next} st&&vl&&toupper($st)==toupper(m){sum+=$vl+0} END{printf "%.0f\n",sum+0}'
        bv=$(echo "$ctx_before" | awk -v m="$metric" "$_awk")
        av=$(echo "$ctx_after"  | awk -v m="$metric" "$_awk")
        awk "BEGIN{printf \"%.0f\n\", ${av:-0} - ${bv:-0}}"
    }

    h_first=$(_get "Handler_read_first")
    h_nxt=$(  _get "Handler_read_next")
    h_rnd=$(  _get "Handler_read_rnd_next")
    rows_scanned=$(( ${h_first:-0} + ${h_nxt:-0} + ${h_rnd:-0} ))

    if [ "$IS_MYROCKS" = "true" ]; then
        # Column names match RocksDB PerfContext field names (case-insensitive).
        # The schema discovery file (rocksdb_perf_ctx_schema.txt) from this run
        # will confirm the exact names used by this Percona build.
        iksc_delta=$(  _ctx_delta "internal_key_skipped_count")
        idsc_delta=$(  _ctx_delta "internal_delete_skipped_count")
        gst_delta=$(   _ctx_delta "get_snapshot_time")
        brc_delta=$(   _ctx_delta "block_read_count")
        brb_delta=$(   _ctx_delta "block_read_byte")
        brt_delta=$(   _ctx_delta "block_read_time")
        gfmc_delta=$(  _ctx_delta "get_from_memtable_count")
        gfoft_delta=$( _ctx_delta "get_from_output_files_time")
        if [ "$IS_CSD" = "true" ]; then
            csd_ratio=$(awk "BEGIN{s=${csd_seen_delta:-0}; if(s>0) printf \"%.3f\", ${csd_filt_delta:-0}/s; else print \"N/A\"}")
            printf "  run=%d elapsed=%.1fs | rows_scanned=%s | key_skipped=%s | csd_filtered=%s/%s (ratio=%s) | freeze_ns=%s | llt_alive=%d\n" \
                "$RUN" "$elapsed" "$rows_scanned" "${iksc_delta:-0}" \
                "${csd_filt_delta:-0}" "${csd_seen_delta:-0}" "$csd_ratio" "${csd_freeze_delta:-0}" "$llt_alive"
            echo "${RUN},${query_ok},${elapsed},${CUTOFF},${rows_scanned},${sst_entries:-0},${version_amp:-0},${iksc_delta:-0},${idsc_delta:-0},${gst_delta:-0},${brc_delta:-0},${brb_delta:-0},${brt_delta:-0},${gfmc_delta:-0},${gfoft_delta:-0},${csd_seen_delta:-0},${csd_filt_delta:-0},${csd_freeze_delta:-0}" \
                >> "${RESULT_DIR}/htap_olap_runs.csv"
        elif [ "$IS_NVMEVIRT" = "true" ]; then
            nv_ratio=$(awk "BEGIN{s=${nv_seen_delta:-0}; if(s>0) printf \"%.3f\", ${nv_filt_delta:-0}/s; else print \"N/A\"}")
            printf "  run=%d elapsed=%.1fs | rows_scanned=%s | key_skipped=%s | nvmevirt_filtered=%s/%s (ratio=%s) | llt_alive=%d\n" \
                "$RUN" "$elapsed" "$rows_scanned" "${iksc_delta:-0}" \
                "${nv_filt_delta:-0}" "${nv_seen_delta:-0}" "$nv_ratio" "$llt_alive"
            echo "${RUN},${query_ok},${elapsed},${CUTOFF},${rows_scanned},${sst_entries:-0},${version_amp:-0},${iksc_delta:-0},${idsc_delta:-0},${gst_delta:-0},${brc_delta:-0},${brb_delta:-0},${brt_delta:-0},${gfmc_delta:-0},${gfoft_delta:-0},${nv_seen_delta:-0},${nv_filt_delta:-0}" \
                >> "${RESULT_DIR}/htap_olap_runs.csv"
        else
            printf "  run=%d elapsed=%.1fs | rows_scanned=%s | key_skipped=%s | block_reads=%s | llt_alive=%d\n" \
                "$RUN" "$elapsed" "$rows_scanned" "${iksc_delta:-0}" "${brc_delta:-0}" "$llt_alive"
            echo "${RUN},${query_ok},${elapsed},${CUTOFF},${rows_scanned},${sst_entries:-0},${version_amp:-0},${iksc_delta:-0},${idsc_delta:-0},${gst_delta:-0},${brc_delta:-0},${brb_delta:-0},${brt_delta:-0},${gfmc_delta:-0},${gfoft_delta:-0}" \
                >> "${RESULT_DIR}/htap_olap_runs.csv"
        fi
    else
        _delta() {
            local varname=$1
            local bv av
            # Use %.0f to avoid scientific notation and to handle counters > INT32_MAX
            # (e.g. Innodb_rows_read exceeds 2^31 after a long OLTP run).
            bv=$(echo "$innodb_before" | awk -v k="$varname" 'toupper($1)==toupper(k){printf "%.0f\n", $2+0}')
            av=$(echo "$innodb_after"  | awk -v k="$varname" 'toupper($1)==toupper(k){printf "%.0f\n", $2+0}')
            awk "BEGIN{printf \"%.0f\n\", ${av:-0} - ${bv:-0}}"
        }
        h_key=$(         _get   "Handler_read_key")
        inno_rows=$(      _delta "Innodb_rows_read")
        inno_bp_reads=$(  _delta "Innodb_buffer_pool_reads")
        inno_bp_req=$(    _delta "Innodb_buffer_pool_read_requests")
        inno_pages=$(     _delta "Innodb_pages_read")
        inno_data_reads=$(_delta "Innodb_data_reads")
        inno_data_bytes=$(_delta "Innodb_data_read")
        printf "  run=%d elapsed=%.1fs | rows_scanned=%s | bp_reads=%s | llt_alive=%d\n" \
            "$RUN" "$elapsed" "$rows_scanned" "${inno_bp_reads:-0}" "$llt_alive"
        echo "${RUN},${query_ok},${elapsed},${CUTOFF},${rows_scanned},${h_key:-0},${inno_rows:-0},${inno_bp_reads:-0},${inno_bp_req:-0},${inno_pages:-0},${inno_data_reads:-0},${inno_data_bytes:-0}" \
            >> "${RESULT_DIR}/htap_olap_runs.csv"
    fi

    # Flamegraph generation deferred to post-loop so it does not block subsequent runs.
    if [ ! -s "$perf_data" ]; then
        log_error "  perf data missing or empty for run ${RUN} — flamegraph will be skipped"
    fi
done

# ── Phase 8: Flamegraph generation (deferred) ────────────────────────────────
log_info "=========================================="
log_info "Generating flamegraphs (deferred — ${HTAP_OLAP_RUNS} runs$([ "$RUN_START" -eq 0 ] && echo " + diagnostic Run 0"))..."
for RUN in $(seq "$RUN_START" "$HTAP_OLAP_RUNS"); do
    perf_data="${RESULT_DIR}/perf_htap_run${RUN}.data"
    if [ -s "$perf_data" ]; then
        svg="${RESULT_DIR}/flamegraph_htap_run${RUN}.svg"
        perf_size=$(du -h "$perf_data" 2>/dev/null | cut -f1)
        log_info "  Run ${RUN}/${HTAP_OLAP_RUNS}: processing ${perf_size} perf data..."
        # --no-inline: resolving the full inline-function chain per address is a
        # much more expensive query than a plain function-level lookup, and perf
        # falls back to a synchronous one-address-at-a-time addr2line-style
        # subprocess for it -- against a debug build with heavy DWARF info, this
        # can turn a ~30s flamegraph generation into a multi-hour stall with the
        # process sitting at ~0% CPU (confirmed 2026-07-31: ps/top showed the
        # perf script process barely accumulating any CPU time over many
        # minutes, and strace confirmed it was blocked on a slow read/write loop
        # with a symbolizer helper, not actually stuck/hung). Flamegraphs don't
        # need per-inline-frame granularity anyway.
        ${BENCH_SUDO-sudo} perf script -i "$perf_data" --no-inline 2>/dev/null \
            | "${FLAMEGRAPH_DIR}/stackcollapse-perf.pl" \
            | "${FLAMEGRAPH_DIR}/flamegraph.pl" \
                --title "${ENGINE} HTAP Join4 run${RUN} cutoff=${CUTOFF} ($(printf '%.1f' "${PERF_ELAPSED[$RUN]:-0}")s)" \
                --width 1800 \
            > "$svg" || log_error "  Flamegraph generation failed for run ${RUN}"
        log_info "  Run ${RUN}/${HTAP_OLAP_RUNS}: flamegraph written → $svg"
        ${BENCH_SUDO-sudo} rm -f "$perf_data"
    else
        log_error "  Run ${RUN}: perf data missing or empty, skipping flamegraph"
    fi
done

# ── Phase 9: Capture mysqld's error log ──────────────────────────────────────
# Queried directly from the running server rather than derived from a
# hardcoded per-engine datadir mapping, so this stays correct regardless of
# engine. This is the only place a query getting killed by
# `max_execution_time` (e.g. HTAP_QUERY_TIMEOUT) would ever be visible --
# the OLAP join queries above run with `2>/dev/null`, so a timed-out query
# leaves no trace anywhere else in this results directory. On NVMeVirt-backed
# datadirs (/mnt/nvme) this file is also the ONLY copy: that filesystem gets
# wiped by nvmev's mount.sh on every guest reboot, so without this copy the
# log is unrecoverable once the guest restarts (confirmed the hard way).
log_info "Capturing mysqld error log..."
ERROR_LOG_PATH=$(mysql --socket="$SOCKET" -N -e "SHOW VARIABLES LIKE 'log_error'" 2>/dev/null | awk '{print $2}')
if [ -n "$ERROR_LOG_PATH" ] && ${BENCH_SUDO-sudo} test -f "$ERROR_LOG_PATH"; then
    ${BENCH_SUDO-sudo} cp "$ERROR_LOG_PATH" "${RESULT_DIR}/mysqld_error.log"
    log_info "  mysqld error log saved to: ${RESULT_DIR}/mysqld_error.log"
else
    log_error "  Could not capture mysqld error log (path='${ERROR_LOG_PATH:-unknown}') -- skipping"
fi

# Fix ownership: perf record runs as root (via sudo), so .data files are root-owned.
# Chown the whole result dir back to the invoking user so VSCode/SCP can read the files.
if [ -n "${SUDO_USER:-}" ]; then
    sudo chown -R "${SUDO_USER}:${SUDO_USER}" "$RESULT_DIR" 2>/dev/null || true
elif [ "$(id -u)" -ne 0 ]; then
    sudo chown -R "$(id -un):$(id -gn)" "$RESULT_DIR" 2>/dev/null || true
fi

log_info "=========================================="
log_info "HTAP profiling complete"
log_info "  OLAP runs CSV    : ${RESULT_DIR}/htap_olap_runs.csv"
log_info "  Version growth   : ${RESULT_DIR}/htap_version_growth.csv"
log_info "  Flamegraphs      : ${RESULT_DIR}/flamegraph_htap_run*.svg"
log_info "  OLTP log         : ${RESULT_DIR}/sysbench_htap_oltp.txt"
log_info "  Memory snapshots : ${RESULT_DIR}/memory_run*_{before,after}.txt"
log_info "  Resource summary : ${RESULT_DIR}/profiling_htap_resource_summary.csv"
log_info "  mysqld error log : ${RESULT_DIR}/mysqld_error.log"
log_info "=========================================="
