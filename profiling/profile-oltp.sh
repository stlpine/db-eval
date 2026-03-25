#!/bin/bash
# MyRocks OLTP Profiling: CPU flamegraph + RocksDB perf context during TPC-C steady state
#
# Mirrors run-benchmark.sh structure exactly (cold MySQL start, cgroup-aware,
# verify storage engine, capture data profile, sysstat monitors, SSD cooldown).
# Profiling layers (perf record, perf stat, rocksdb_perf_context) are added on top.
#
# Usage:
#   sudo cgexec -g memory:limited_memory_group \
#       bash ./profiling/profile-oltp.sh [threads] [result_dir]
#
#   threads     TPC-C concurrency (default: 32)
#   result_dir  Output directory (default: results/profiling/oltp/<timestamp>)
#
# Prerequisites:
#   - TPC-C data loaded for percona-myrocks (run prepare-data.sh -e percona-myrocks -b tpcc)
#   - ~/FlameGraph cloned (brendangregg/FlameGraph)
#   - linux-tools-$(uname -r) installed

set -e
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"
source "${SCRIPT_DIR}/../scripts/monitor.sh"

# ── Configuration ─────────────────────────────────────────────────────────────

THREADS="${1:-32}"
RESULT_DIR="${2:-${RESULTS_DIR}/profiling/oltp/$(date +%Y%m%d_%H%M%S)}"
ENGINE="percona-myrocks"
SOCKET="${MYSQL_SOCKET_PERCONA_MYROCKS}"
TPCC_BIN="${SCRIPT_DIR}/../tpcc/tpcc-mysql/tpcc_start"

WARMUP_DURATION="${PROFILING_WARMUP_DURATION}"
RECORD_DURATION="${PROFILING_RECORD_DURATION}"
TPCC_TOTAL=$(( WARMUP_DURATION + RECORD_DURATION + 30 ))

# ── Helper functions (mirrors run-benchmark.sh) ───────────────────────────────

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
        WHERE TABLE_SCHEMA = '${BENCHMARK_DB}' AND ENGINE != 'ROCKSDB';" 2>/dev/null)
    if [ -n "$wrong_tables" ]; then
        log_error "Storage engine mismatch! Expected ROCKSDB."
        echo "$wrong_tables"
        stop_mysql
        exit 1
    fi
    log_info "Storage engine verified: all tables use ROCKSDB"
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
    mysql --socket="$SOCKET" --batch --skip-column-names 2>/dev/null -e "
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
        ORDER BY variable_name;"
}

# ── Preflight ─────────────────────────────────────────────────────────────────

log_info "=========================================="
log_info "MyRocks OLTP Profiling"
log_info "=========================================="
log_info "Engine  : $ENGINE"
log_info "Threads : $THREADS"
log_info "Warmup  : ${WARMUP_DURATION}s | Record: ${RECORD_DURATION}s"
log_info "Results : $RESULT_DIR"
log_info "=========================================="

# Stop system MySQL service if running (same as run-benchmark.sh)
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

# tpcc_start binary check
if [ ! -f "$TPCC_BIN" ]; then
    log_error "tpcc_start not found at ${TPCC_BIN}. Run tpcc/prepare.sh first."
    exit 1
fi

mkdir -p "$RESULT_DIR"

MYSQL_LIB_PATH=$(mysql_config --variable=pkglibdir 2>/dev/null || true)
[ -n "$MYSQL_LIB_PATH" ] && export LD_LIBRARY_PATH="${MYSQL_LIB_PATH}:${LD_LIBRARY_PATH:-}"

# ── Cold MySQL start (mirrors run-benchmark.sh exactly) ───────────────────────

start_mysql_cold
verify_storage_engine
capture_data_profile "$RESULT_DIR"

MYSQLD_PID=$(cat "${MYSQL_PID_PERCONA_MYROCKS}" 2>/dev/null || true)
if [ -z "$MYSQLD_PID" ] || ! kill -0 "$MYSQLD_PID" 2>/dev/null; then
    log_error "Cannot find mysqld PID"
    exit 1
fi

# ── Enable perf context globally ──────────────────────────────────────────────

mysql --socket="$SOCKET" \
    -e "SET GLOBAL rocksdb_perf_context_level = ${PROFILING_PERF_CONTEXT_LEVEL};" 2>/dev/null
log_info "rocksdb_perf_context_level set to ${PROFILING_PERF_CONTEXT_LEVEL}"

# ── Start sysstat monitors (same as run-benchmark.sh) ────────────────────────

start_monitors "$RESULT_DIR" "profiling_oltp"

# ── SSD cooldown (same as tpcc/run.sh) ────────────────────────────────────────

wait_for_ssd_cooldown || log_info "Skipping SSD cooldown (temperature check unavailable)"

# ── Cleanup on exit ───────────────────────────────────────────────────────────

TPCC_PID=""
cleanup() {
    kill "$TPCC_PID" 2>/dev/null || true
    wait "$TPCC_PID" 2>/dev/null || true
    stop_monitors
    generate_resource_summary "$RESULT_DIR" "profiling_oltp"
    stop_mysql || true
}
trap cleanup EXIT

# ── Start TPC-C in background (same args as tpcc/run.sh) ─────────────────────

log_info "Starting TPC-C (${THREADS} threads, ${TPCC_TOTAL}s total)..."

"$TPCC_BIN" \
    -h localhost \
    -S "$SOCKET" \
    -d "$BENCHMARK_DB" \
    -u root \
    -p "" \
    -w "$TPCC_WAREHOUSES" \
    -c "$THREADS" \
    -r "$WARMUP_DURATION" \
    -l "$TPCC_TOTAL" \
    > "${RESULT_DIR}/tpcc_output.txt" 2>&1 &
TPCC_PID=$!

# ── Wait for warmup ───────────────────────────────────────────────────────────

log_info "Warming up for ${WARMUP_DURATION}s..."
sleep "$WARMUP_DURATION"

if ! kill -0 "$TPCC_PID" 2>/dev/null; then
    log_error "TPC-C died during warmup. Check ${RESULT_DIR}/tpcc_output.txt"
    exit 1
fi

log_info "Warmup done. Starting ${RECORD_DURATION}s recording window..."

# ── Snapshot perf context before recording window ────────────────────────────

BEFORE=$(snapshot_perf_context_global)

# ── perf record + perf stat concurrently during steady state ─────────────────

PERF_DATA="${RESULT_DIR}/perf_oltp.data"
PERF_STAT_OUT="${RESULT_DIR}/perf_stat_oltp.txt"

sudo perf record -F 99 -p "$MYSQLD_PID" --call-graph dwarf \
    -o "$PERF_DATA" -- sleep "$RECORD_DURATION" &
PERF_RECORD_PID=$!

sudo perf stat -p "$MYSQLD_PID" \
    -e cycles,instructions,cache-misses,cache-references,LLC-load-misses,LLC-loads \
    -- sleep "$RECORD_DURATION" \
    > "$PERF_STAT_OUT" 2>&1 &
PERF_STAT_PID=$!

wait "$PERF_RECORD_PID" || true
wait "$PERF_STAT_PID" || true

log_info "Recording window done."

# ── Snapshot perf context after and compute delta ────────────────────────────

AFTER=$(snapshot_perf_context_global)

PERF_CSV="${RESULT_DIR}/rocksdb_perf_context_delta.csv"
echo "metric,before,after,delta" > "$PERF_CSV"

# Join before/after by metric name and emit CSV rows
awk '
BEGIN { n = 0 }
NR == FNR { name[NR] = $1; bval[NR] = $2+0; n = NR; next }
FNR <= n  { printf "%s,%d,%d,%d\n", name[FNR], bval[FNR], $2+0, $2+0-bval[FNR] }
' <(echo "$BEFORE") <(echo "$AFTER") >> "$PERF_CSV"

log_info "RocksDB perf context delta:"
column -t -s, "$PERF_CSV"

# ── Stop TPC-C (cleanup trap handles monitors + mysql) ───────────────────────

kill "$TPCC_PID" 2>/dev/null || true
wait "$TPCC_PID" 2>/dev/null || true
TPCC_PID=""

# ── Generate flamegraph ───────────────────────────────────────────────────────

if [ -s "$PERF_DATA" ]; then
    SVG="${RESULT_DIR}/flamegraph_oltp_${THREADS}t.svg"
    sudo perf script -i "$PERF_DATA" 2>/dev/null \
        | "${FLAMEGRAPH_DIR}/stackcollapse-perf.pl" \
        | "${FLAMEGRAPH_DIR}/flamegraph.pl" \
            --title "MyRocks OLTP TPC-C ${THREADS}t" \
            --width 1800 \
        > "$SVG" || log_error "Flamegraph generation failed"
    log_info "Flamegraph: $SVG"
    sudo rm -f "$PERF_DATA"
else
    log_error "perf data missing or empty, skipping flamegraph"
fi

log_info "=========================================="
log_info "OLTP profiling complete"
log_info "  Perf context delta : $PERF_CSV"
log_info "  perf stat          : $PERF_STAT_OUT"
log_info "  Flamegraph         : ${RESULT_DIR}/flamegraph_oltp_${THREADS}t.svg"
log_info "  TPC-C output       : ${RESULT_DIR}/tpcc_output.txt"
log_info "  Resource summary   : ${RESULT_DIR}/profiling_oltp_resource_summary.csv"
log_info "=========================================="
