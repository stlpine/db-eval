#!/bin/bash
# MyRocks Sysbench Profiling: CPU flamegraph + RocksDB perf context during steady state
#
# Mirrors profile-oltp.sh structure exactly (cold MySQL start, cgroup-aware,
# verify storage engine, capture data profile, sysstat monitors, SSD cooldown).
# Designed for oltp_read_only to show MVCC overhead on point lookups + range scans,
# contrasting with TPC-C (pure point lookups) and TPC-H (pure sequential scans).
#
# Usage:
#   sudo cgexec -g memory:limited_memory_group \
#       bash ./profiling/profile-sysbench.sh [workload] [threads] [result_dir]
#
#   workload    Sysbench workload name (default: oltp_read_only)
#               Options: oltp_read_only, oltp_read_write, oltp_write_only
#   threads     Concurrency (default: 32)
#   result_dir  Output directory (default: results/profiling/sysbench/<timestamp>)
#
# Prerequisites:
#   - Sysbench data loaded for percona-myrocks (run prepare-data.sh -e percona-myrocks -b sysbench)
#   - ~/FlameGraph cloned (brendangregg/FlameGraph)
#   - linux-tools-$(uname -r) installed

# Note: no set -e / set -o pipefail — mirrors tpcc/run.sh pattern.
# Critical failures use explicit exit 1; perf/mysql pipelines are best-effort.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"
source "${SCRIPT_DIR}/../scripts/monitor.sh"

# ── Configuration ─────────────────────────────────────────────────────────────

WORKLOAD="${1:-oltp_read_only}"
THREADS="${2:-32}"
ENGINE="${4:-percona-myrocks}"

case "$ENGINE" in
    percona-myrocks)
        SOCKET="${MYSQL_SOCKET_PERCONA_MYROCKS}"
        PID_FILE="${MYSQL_PID_PERCONA_MYROCKS}"
        EXPECTED_ENGINE="ROCKSDB"
        ;;
    percona-innodb)
        SOCKET="${MYSQL_SOCKET_PERCONA_INNODB}"
        PID_FILE="${MYSQL_PID_PERCONA_INNODB}"
        EXPECTED_ENGINE="InnoDB"
        ;;
    *)
        echo "Unknown engine: $ENGINE (use percona-myrocks or percona-innodb)" >&2
        exit 1
        ;;
esac

RESULT_DIR="${3:-${RESULTS_DIR}/profiling/sysbench/${ENGINE}/$(date +%Y%m%d_%H%M%S)}"

WARMUP_DURATION="${PROFILING_WARMUP_DURATION}"
RECORD_DURATION="${PROFILING_RECORD_DURATION}"
SB_TOTAL=$(( WARMUP_DURATION + RECORD_DURATION + 30 ))

# ── Helper functions (mirrors profile-oltp.sh) ────────────────────────────────

drop_page_cache() {
    log_info "Dropping OS page cache..."
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
    log_info "Page cache dropped"
}

start_mysql_cold() {
    ensure_mysql_stopped "$ENGINE"
    drop_page_cache
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
        log_error "Storage engine mismatch! Expected ${EXPECTED_ENGINE} for all sysbench tables."
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

snapshot_perf_context_global() {
    if [ "$ENGINE" = "percona-myrocks" ]; then
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
        else
            log_info "  rocksdb_perf_context_global unavailable, trying SHOW ENGINE ROCKSDB STATUS" >&2
            mysql --socket="$SOCKET" --batch --skip-column-names 2>/dev/null -e "
                SHOW ENGINE ROCKSDB STATUS;" \
            | grep -E "internal_key_skipped_count|internal_delete_skipped_count|get_snapshot_time|block_read_count|block_read_byte|block_read_time|get_from_memtable_count|get_from_memtable_time|get_from_output_files_time" \
            | awk '{print $1, $NF}' || true
        fi
    else
        mysql --socket="$SOCKET" --batch --skip-column-names 2>/dev/null -e "
            SHOW GLOBAL STATUS WHERE Variable_name IN (
                'Innodb_rows_read',
                'Innodb_rows_inserted',
                'Innodb_rows_updated',
                'Innodb_rows_deleted',
                'Innodb_buffer_pool_reads',
                'Innodb_buffer_pool_read_requests',
                'Innodb_data_reads',
                'Innodb_data_read'
            );" || true
    fi
}

# ── Preflight ─────────────────────────────────────────────────────────────────

log_info "=========================================="
log_info "Sysbench Profiling (${ENGINE})"
log_info "=========================================="
log_info "Engine   : $ENGINE"
log_info "Workload : $WORKLOAD"
log_info "Threads  : $THREADS"
log_info "Warmup   : ${WARMUP_DURATION}s | Record: ${RECORD_DURATION}s"
log_info "Results  : $RESULT_DIR"
log_info "=========================================="

# Stop system MySQL service if running
log_info "Checking for running MySQL service..."
if systemctl is-active --quiet mysql 2>/dev/null; then
    log_info "MySQL service is running. Stopping it..."
    sudo systemctl stop mysql
    sleep 3
    if systemctl is-active --quiet mysql 2>/dev/null; then
        log_error "Failed to stop MySQL service"
        exit 1
    fi
    log_info "MySQL service stopped"
else
    log_info "MySQL service is not running"
fi

# SSD mount check
check_ssd_mount || { log_error "SSD mount check failed"; exit 1; }

# FlameGraph check
if [ ! -f "${FLAMEGRAPH_DIR}/flamegraph.pl" ]; then
    log_error "FlameGraph not found at ${FLAMEGRAPH_DIR}. Clone brendangregg/FlameGraph there."
    exit 1
fi

# sysbench check
if ! command -v sysbench &>/dev/null; then
    log_error "sysbench not found. Install it: sudo apt install sysbench"
    exit 1
fi

mkdir -p "$RESULT_DIR"

MYSQL_LIB_PATH=$(mysql_config --variable=pkglibdir 2>/dev/null || true)
[ -n "$MYSQL_LIB_PATH" ] && export LD_LIBRARY_PATH="${MYSQL_LIB_PATH}:${LD_LIBRARY_PATH:-}"

# ── Cold MySQL start ───────────────────────────────────────────────────────────

start_mysql_cold
verify_storage_engine
capture_data_profile "$RESULT_DIR"

# ── Log profiling configuration ────────────────────────────────────────────────

CONFIG_LOG="${RESULT_DIR}/profiling_config.log"
log_info "Logging configuration to: $CONFIG_LOG"
{
    echo "============================================================"
    echo "PROFILING CONFIGURATION LOG"
    echo "Generated: $(date)"
    echo "Engine: $ENGINE"
    echo "Workload: Sysbench ${WORKLOAD}"
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
    echo "Disk Info:"
    df -h "$SSD_MOUNT" 2>/dev/null
    echo ""
    echo "============================================================"
    echo "PROFILING PARAMETERS"
    echo "============================================================"
    echo "BENCHMARK_DB: $BENCHMARK_DB"
    echo "SYSBENCH_WORKLOAD: $WORKLOAD"
    echo "SYSBENCH_THREADS: $THREADS"
    echo "SYSBENCH_TABLES: $SYSBENCH_TABLES"
    echo "SYSBENCH_TABLE_SIZE: $SYSBENCH_TABLE_SIZE"
    echo "PROFILING_WARMUP_DURATION: ${WARMUP_DURATION}s"
    echo "PROFILING_RECORD_DURATION: ${RECORD_DURATION}s"
    echo "PROFILING_PERF_CONTEXT_LEVEL: $PROFILING_PERF_CONTEXT_LEVEL"
    echo "CGROUP_MEMORY_LIMIT: $CGROUP_MEMORY_LIMIT"
    echo "MYROCKS_BLOOM_FILTER: $MYROCKS_BLOOM_FILTER"
    echo "FLAMEGRAPH_DIR: $FLAMEGRAPH_DIR"
    echo "PERF_EVENT: cpu_core/cycles/"
    echo "PERF_FREQ: 99 Hz"
    echo "PERF_CALL_GRAPH: dwarf"
    echo ""
    echo "============================================================"
    echo "MYSQL SERVER VARIABLES"
    echo "============================================================"
    mysql --socket="$SOCKET" -e "SHOW VARIABLES;" 2>/dev/null
    echo ""
} > "$CONFIG_LOG" 2>&1
log_info "Configuration logged"

MYSQLD_PID=$(cat "${PID_FILE}" 2>/dev/null || true)
if [ -z "$MYSQLD_PID" ] || ! kill -0 "$MYSQLD_PID" 2>/dev/null; then
    log_error "Cannot find mysqld PID"
    exit 1
fi

# ── Enable perf context globally (MyRocks only) ───────────────────────────────

if [ "$ENGINE" = "percona-myrocks" ]; then
    mysql --socket="$SOCKET" \
        -e "SET GLOBAL rocksdb_perf_context_level = ${PROFILING_PERF_CONTEXT_LEVEL};" 2>/dev/null
    log_info "rocksdb_perf_context_level set to ${PROFILING_PERF_CONTEXT_LEVEL}"
fi

# ── Start sysstat monitors ────────────────────────────────────────────────────

start_monitors "$RESULT_DIR" "profiling_sysbench"

# ── SSD cooldown ──────────────────────────────────────────────────────────────

wait_for_ssd_cooldown || log_info "Skipping SSD cooldown (temperature check unavailable)"

# ── Cleanup on exit ───────────────────────────────────────────────────────────

SB_PID=""
cleanup() {
    kill "$SB_PID" 2>/dev/null || true
    wait "$SB_PID" 2>/dev/null || true
    stop_monitors
    generate_resource_summary "$RESULT_DIR" "profiling_sysbench"
    stop_mysql || true
}
trap cleanup EXIT

# ── Start sysbench in background ──────────────────────────────────────────────

log_info "Starting sysbench ${WORKLOAD} (${THREADS} threads, ${SB_TOTAL}s total)..."

sysbench "$WORKLOAD" \
    --mysql-socket="$SOCKET" \
    --mysql-db="$BENCHMARK_DB" \
    --tables="$SYSBENCH_TABLES" \
    --table-size="$SYSBENCH_TABLE_SIZE" \
    --threads="$THREADS" \
    --time="$SB_TOTAL" \
    --report-interval=10 \
    --db-ps-mode=disable \
    run \
    > "${RESULT_DIR}/sysbench_output.txt" 2>&1 &
SB_PID=$!

# ── Wait for warmup ───────────────────────────────────────────────────────────

log_info "Warming up for ${WARMUP_DURATION}s..."
sleep "$WARMUP_DURATION"

if ! kill -0 "$SB_PID" 2>/dev/null; then
    log_error "Sysbench died during warmup. Check ${RESULT_DIR}/sysbench_output.txt"
    exit 1
fi

log_info "Warmup done. Starting ${RECORD_DURATION}s recording window..."

# ── Snapshot perf context before recording window ────────────────────────────

PERF_CTX_BEFORE="${RESULT_DIR}/perf_ctx_before.tmp"
PERF_CTX_AFTER="${RESULT_DIR}/perf_ctx_after.tmp"

snapshot_perf_context_global > "$PERF_CTX_BEFORE" || true
log_info "Perf context snapshot (before) captured"

# ── perf record + perf stat concurrently during steady state ─────────────────

PERF_DATA="${RESULT_DIR}/perf_sysbench.data"
PERF_STAT_OUT="${RESULT_DIR}/perf_stat_sysbench.txt"

sudo perf record -F 99 -p "$MYSQLD_PID" --call-graph dwarf \
    -e cpu_core/cycles/ \
    -o "$PERF_DATA" -- sleep "$RECORD_DURATION" &
PERF_RECORD_PID=$!

sudo perf stat -p "$MYSQLD_PID" \
    -e cpu_core/cycles/,cpu_core/instructions/,cpu_core/cache-misses/,cpu_core/cache-references/,cpu_core/LLC-load-misses/,cpu_core/LLC-loads/ \
    -- sleep "$RECORD_DURATION" \
    > "$PERF_STAT_OUT" 2>&1 &
PERF_STAT_PID=$!

wait "$PERF_RECORD_PID" || true
wait "$PERF_STAT_PID" || true

log_info "Recording window done."

# ── Snapshot perf context after and compute delta ────────────────────────────

snapshot_perf_context_global > "$PERF_CTX_AFTER" || true

PERF_CSV="${RESULT_DIR}/$([ "$ENGINE" = "percona-myrocks" ] && echo "rocksdb_perf_context_delta" || echo "innodb_status_delta").csv"
echo "metric,before,after,delta" > "$PERF_CSV"

awk '
BEGIN { n = 0 }
NR == FNR { name[NR] = $1; bval[NR] = $2+0; n = NR; next }
FNR <= n  { printf "%s,%d,%d,%d\n", name[FNR], bval[FNR], $2+0, $2+0-bval[FNR] }
' "$PERF_CTX_BEFORE" "$PERF_CTX_AFTER" >> "$PERF_CSV"

log_info "RocksDB perf context delta:"
column -t -s, "$PERF_CSV"

# ── Stop sysbench ─────────────────────────────────────────────────────────────

kill "$SB_PID" 2>/dev/null || true
wait "$SB_PID" 2>/dev/null || true
SB_PID=""

# ── Generate flamegraph ───────────────────────────────────────────────────────

if [ -s "$PERF_DATA" ]; then
    SVG="${RESULT_DIR}/flamegraph_sysbench_${WORKLOAD}_${THREADS}t.svg"
    sudo perf script -i "$PERF_DATA" 2>/dev/null \
        | "${FLAMEGRAPH_DIR}/stackcollapse-perf.pl" \
        | "${FLAMEGRAPH_DIR}/flamegraph.pl" \
            --title "${ENGINE} Sysbench ${WORKLOAD} ${THREADS}t" \
            --width 1800 \
        > "$SVG" || log_error "Flamegraph generation failed"
    log_info "Flamegraph: $SVG"
    sudo rm -f "$PERF_DATA"
else
    log_error "perf data missing or empty, skipping flamegraph"
fi

log_info "=========================================="
log_info "Sysbench profiling complete"
log_info "  Perf context delta : $PERF_CSV"
log_info "  perf stat          : $PERF_STAT_OUT"
log_info "  Flamegraph         : ${RESULT_DIR}/flamegraph_sysbench_${WORKLOAD}_${THREADS}t.svg"
log_info "  Sysbench output    : ${RESULT_DIR}/sysbench_output.txt"
log_info "  Resource summary   : ${RESULT_DIR}/profiling_sysbench_resource_summary.csv"
log_info "=========================================="
