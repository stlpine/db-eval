#!/bin/bash
# MyRocks OLAP Profiling: per-query RocksDB perf context + CPU flamegraph
#
# Mirrors run-benchmark.sh structure exactly (cold MySQL start, cgroup-aware,
# verify storage engine, capture data profile, sysstat monitors, SSD cooldown).
# Profiling layers (perf record, rocksdb_perf_context) are added on top.
#
# Usage:
#   sudo cgexec -g memory:limited_memory_group \
#       bash ./profiling/profile-olap.sh [queries] [result_dir]
#
#   queries     Space-separated TPC-H query numbers (default: "1 6 12 19")
#   result_dir  Output directory (default: results/profiling/olap/<timestamp>)
#
# Prerequisites:
#   - TPC-H data loaded for percona-myrocks (run prepare-data.sh -e percona-myrocks -b tpch-olap)
#   - ~/FlameGraph cloned (brendangregg/FlameGraph)
#   - linux-tools-$(uname -r) installed

set -e
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"
source "${SCRIPT_DIR}/../scripts/monitor.sh"

# ── Configuration ─────────────────────────────────────────────────────────────

QUERIES="${1:-${PROFILING_OLAP_QUERIES}}"
RESULT_DIR="${2:-${RESULTS_DIR}/profiling/olap/$(date +%Y%m%d_%H%M%S)}"
ENGINE="percona-myrocks"
SOCKET="${MYSQL_SOCKET_PERCONA_MYROCKS}"
QUERIES_DIR="${SCRIPT_DIR}/../tpch-olap/queries/mysql"

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

# ── Preflight ─────────────────────────────────────────────────────────────────

log_info "=========================================="
log_info "MyRocks OLAP Profiling"
log_info "=========================================="
log_info "Engine  : $ENGINE"
log_info "Queries : $QUERIES"
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

mkdir -p "$RESULT_DIR"

# ── Cold MySQL start (mirrors run-benchmark.sh exactly) ───────────────────────

start_mysql_cold
verify_storage_engine
capture_data_profile "$RESULT_DIR"

MYSQLD_PID=$(cat "${MYSQL_PID_PERCONA_MYROCKS}" 2>/dev/null || true)
if [ -z "$MYSQLD_PID" ] || ! kill -0 "$MYSQLD_PID" 2>/dev/null; then
    log_error "Cannot find mysqld PID"
    exit 1
fi

# Set perf context level globally (rocksdb_perf_context_level is global-only)
mysql --socket="$SOCKET" \
    -e "SET GLOBAL rocksdb_perf_context_level = ${PROFILING_PERF_CONTEXT_LEVEL};" 2>/dev/null
log_info "rocksdb_perf_context_level set to ${PROFILING_PERF_CONTEXT_LEVEL}"

# ── Start sysstat monitors (same as run-benchmark.sh) ────────────────────────

start_monitors "$RESULT_DIR" "profiling_olap"

# ── SSD cooldown (same as tpch-olap/run.sh) ───────────────────────────────────

wait_for_ssd_cooldown || log_info "Skipping SSD cooldown (temperature check unavailable)"

# ── Cleanup on exit ───────────────────────────────────────────────────────────

PERF_PID=""
cleanup() {
    # SIGINT causes perf to flush and write the data file cleanly
    if [ -n "$PERF_PID" ]; then
        sudo kill -INT "$PERF_PID" 2>/dev/null || true
        wait "$PERF_PID" 2>/dev/null || true
    fi
    stop_monitors
    generate_resource_summary "$RESULT_DIR" "profiling_olap"
    stop_mysql || true
}
trap cleanup EXIT

# ── Initialise results CSV ────────────────────────────────────────────────────

PERF_CSV="${RESULT_DIR}/rocksdb_perf_context.csv"
echo "query,elapsed_s,rows_scanned,internal_key_skipped_count,internal_delete_skipped_count,get_snapshot_time_ns,block_read_count,block_read_byte,block_read_time_ns,get_from_memtable_count,get_from_memtable_time_ns,get_from_output_files_time_ns" \
    > "$PERF_CSV"

# ── Per-query profiling ───────────────────────────────────────────────────────

profile_query() {
    local q=$1
    local query_file="${QUERIES_DIR}/${q}.sql"

    if [ ! -f "$query_file" ]; then
        log_error "Query file not found: $query_file"
        return 1
    fi

    log_info "── Q${q} ──────────────────────────────────────────"

    # Flush MyRocks memtable → all reads go to SST files (same cold-read intent
    # as drop_page_cache in start_mysql_cold, but for RocksDB's in-memory state)
    mysql --socket="$SOCKET" \
        -e "SET GLOBAL rocksdb_force_flush_memtable_now = 1;" 2>/dev/null
    log_info "  Memtable flushed to SST"

    # Drop OS page cache so MyRocks reads from SSD (cold I/O)
    drop_page_cache

    # Start perf record attached to mysqld
    local perf_data="${RESULT_DIR}/perf_q${q}.data"
    sudo perf record -F 99 -p "$MYSQLD_PID" --call-graph dwarf \
        -o "$perf_data" -- sleep 86400 &
    PERF_PID=$!
    sleep 0.5   # let perf attach before query starts

    # Run SET + query + perf context SELECT in one session so session-level
    # counters accumulate for exactly this query.
    # FLUSH STATUS resets Handler_* counters for the session.
    # The final SELECT outputs name<TAB>value rows, distinct from numeric
    # query result rows, so we can parse them from mixed stdout.
    local query_sql
    query_sql=$(cat "$query_file")

    local start_time end_time elapsed
    start_time=$(date +%s.%N)

    local raw_output
    raw_output=$(mysql --socket="$SOCKET" "$BENCHMARK_DB" \
        --batch --skip-column-names 2>/dev/null <<SQL
FLUSH STATUS;
${query_sql}
SELECT variable_name, variable_value
    FROM information_schema.rocksdb_perf_context
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
    ORDER BY variable_name;
SHOW SESSION STATUS LIKE 'Handler_read_rnd_next';
SHOW SESSION STATUS LIKE 'Handler_read_next';
SQL
    )

    end_time=$(date +%s.%N)
    elapsed=$(echo "$end_time - $start_time" | bc)

    # SIGINT causes perf to flush and write the data file cleanly
    sudo kill -INT "$PERF_PID" 2>/dev/null || true
    wait "$PERF_PID" 2>/dev/null || true
    PERF_PID=""

    # Parse perf context (tab-separated name<TAB>value lines)
    _get() { echo "$raw_output" | awk -v k="$1" '$1==k{print $2}'; }
    local iksc;  iksc=$(_get  "internal_key_skipped_count")
    local idsc;  idsc=$(_get  "internal_delete_skipped_count")
    local gst;   gst=$(_get   "get_snapshot_time")
    local brc;   brc=$(_get   "block_read_count")
    local brb;   brb=$(_get   "block_read_byte")
    local brt;   brt=$(_get   "block_read_time")
    local gfmc;  gfmc=$(_get  "get_from_memtable_count")
    local gfmt;  gfmt=$(_get  "get_from_memtable_time")
    local gfoft; gfoft=$(_get "get_from_output_files_time")

    # Handler rows: proxy for rows scanned
    local h_rnd h_nxt rows_scanned
    h_rnd=$(echo "$raw_output" | awk '/Handler_read_rnd_next/{print $2}')
    h_nxt=$(echo "$raw_output" | awk '/Handler_read_next/{print $2}')
    rows_scanned=$(( ${h_rnd:-0} + ${h_nxt:-0} ))

    printf "  elapsed=%.1fs | rows_scanned=%s | key_skipped=%s | block_reads=%s\n" \
        "$elapsed" "$rows_scanned" "${iksc:-0}" "${brc:-0}"

    echo "${q},${elapsed},${rows_scanned},${iksc:-0},${idsc:-0},${gst:-0},${brc:-0},${brb:-0},${brt:-0},${gfmc:-0},${gfmt:-0},${gfoft:-0}" \
        >> "$PERF_CSV"

    # Generate flamegraph (|| true so a corrupt perf file doesn't abort the run)
    if [ -s "$perf_data" ]; then
        local svg="${RESULT_DIR}/flamegraph_q${q}.svg"
        sudo perf script -i "$perf_data" 2>/dev/null \
            | "${FLAMEGRAPH_DIR}/stackcollapse-perf.pl" \
            | "${FLAMEGRAPH_DIR}/flamegraph.pl" \
                --title "MyRocks OLAP Q${q} (${elapsed}s)" \
                --width 1800 \
            > "$svg" || log_error "  Flamegraph generation failed for Q${q}"
        log_info "  Flamegraph: $svg"
        sudo rm -f "$perf_data"
    else
        log_error "  perf data missing or empty for Q${q}, skipping flamegraph"
    fi
}

for q in $QUERIES; do
    profile_query "$q"
done

log_info "=========================================="
log_info "OLAP profiling complete"
log_info "  Perf context CSV : $PERF_CSV"
log_info "  Flamegraphs      : ${RESULT_DIR}/flamegraph_q*.svg"
log_info "  Resource summary : ${RESULT_DIR}/profiling_olap_resource_summary.csv"
log_info "=========================================="
