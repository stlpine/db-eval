#!/bin/bash
# MyRocks ClickBench Profiling: per-query RocksDB perf context + CPU flamegraph
#
# Mirrors profile-olap.sh structure exactly (cold MySQL start, cgroup-aware,
# verify storage engine, capture data profile, sysstat monitors, SSD cooldown).
# Designed for scan-heavy single-table queries on the `hits` table (100M rows),
# where MVCC visibility overhead is most visible without join-induced noise.
#
# Usage:
#   sudo cgexec -g memory:limited_memory_group \
#       bash ./profiling/profile-clickbench.sh [queries] [result_dir]
#
#   queries     Space-separated ClickBench query numbers, 1-based (default: "3 8 14 17")
#               Q3:  pure full-scan aggregation (SUM/COUNT/AVG)
#               Q8:  filtered GROUP BY on AdvEngineID
#               Q14: COUNT DISTINCT on SearchPhrase
#               Q17: GROUP BY UserID+SearchPhrase (high-cardinality scan)
#   result_dir  Output directory (default: results/profiling/clickbench/<timestamp>)
#
# Prerequisites:
#   - ClickBench data loaded for percona-myrocks (run prepare-data.sh -e percona-myrocks -b clickbench)
#   - ~/FlameGraph cloned (brendangregg/FlameGraph)
#   - linux-tools-$(uname -r) installed

# Note: no set -e / set -o pipefail — mirrors tpcc/run.sh pattern.
# Critical failures use explicit exit 1; perf/mysql pipelines are best-effort.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"
source "${SCRIPT_DIR}/../scripts/monitor.sh"

# ── Configuration ─────────────────────────────────────────────────────────────

QUERIES="${1:-3 8 14 17}"
RESULT_DIR="${2:-${RESULTS_DIR}/profiling/clickbench/$(date +%Y%m%d_%H%M%S)}"
ENGINE="percona-myrocks"
SOCKET="${MYSQL_SOCKET_PERCONA_MYROCKS}"
QUERIES_SQL="${SCRIPT_DIR}/../clickbench/queries/queries.sql"

# ── Helper functions (mirrors profile-olap.sh) ────────────────────────────────

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

# Extract query text for the given 1-based query number from queries.sql.
# Skips comment lines (--) and blank lines; returns Nth remaining line.
get_query_sql() {
    local n=$1
    awk '
        /^[[:space:]]*--/ { next }
        /^[[:space:]]*$/  { next }
        { count++ }
        count == '"$n"' { print; exit }
    ' "$QUERIES_SQL"
}

# ── Preflight ─────────────────────────────────────────────────────────────────

log_info "=========================================="
log_info "MyRocks ClickBench Profiling"
log_info "=========================================="
log_info "Engine  : $ENGINE"
log_info "Queries : $QUERIES"
log_info "Results : $RESULT_DIR"
log_info "=========================================="

# Validate queries file
if [ ! -f "$QUERIES_SQL" ]; then
    log_error "ClickBench queries file not found: $QUERIES_SQL"
    exit 1
fi

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

mkdir -p "$RESULT_DIR"

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
    echo "Workload: ClickBench (single-table OLAP)"
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
    echo "CLICKBENCH_QUERIES: $QUERIES"
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

MYSQLD_PID=$(cat "${MYSQL_PID_PERCONA_MYROCKS}" 2>/dev/null || true)
if [ -z "$MYSQLD_PID" ] || ! kill -0 "$MYSQLD_PID" 2>/dev/null; then
    log_error "Cannot find mysqld PID"
    exit 1
fi

# Enable perf context globally
mysql --socket="$SOCKET" \
    -e "SET GLOBAL rocksdb_perf_context_level = ${PROFILING_PERF_CONTEXT_LEVEL};" 2>/dev/null
log_info "rocksdb_perf_context_level set to ${PROFILING_PERF_CONTEXT_LEVEL}"

# ── Start sysstat monitors ─────────────────────────────────────────────────────

start_monitors "$RESULT_DIR" "profiling_clickbench"

# ── SSD cooldown ───────────────────────────────────────────────────────────────

wait_for_ssd_cooldown || log_info "Skipping SSD cooldown (temperature check unavailable)"

# ── Cleanup on exit ───────────────────────────────────────────────────────────

PERF_PID=""
cleanup() {
    if [ -n "$PERF_PID" ]; then
        sudo kill -INT "$PERF_PID" 2>/dev/null || true
        wait "$PERF_PID" 2>/dev/null || true
    fi
    stop_monitors
    generate_resource_summary "$RESULT_DIR" "profiling_clickbench"
    stop_mysql || true
}
trap cleanup EXIT

# ── Initialise results CSV ────────────────────────────────────────────────────

PERF_CSV="${RESULT_DIR}/clickbench_perf_context.csv"
echo "query,elapsed_s,rows_scanned,internal_key_skipped_count,internal_delete_skipped_count,get_snapshot_time_ns,block_read_count,block_read_byte,block_read_time_ns,get_from_memtable_count,get_from_memtable_time_ns,get_from_output_files_time_ns" \
    > "$PERF_CSV"

# ── Per-query profiling ───────────────────────────────────────────────────────

profile_query() {
    local q=$1

    local query_sql
    query_sql=$(get_query_sql "$q")
    if [ -z "$query_sql" ]; then
        log_error "Query ${q} not found in ${QUERIES_SQL} (only $(awk '!/^[[:space:]]*--/ && NF' "$QUERIES_SQL" | wc -l) queries available)"
        return 1
    fi

    log_info "── Q${q} ──────────────────────────────────────────"

    # Flush MyRocks memtable → all reads go to SST files
    mysql --socket="$SOCKET" \
        -e "SET GLOBAL rocksdb_force_flush_memtable_now = 1;" 2>/dev/null
    log_info "  Memtable flushed to SST"

    # Drop OS page cache so MyRocks reads from SSD (cold I/O)
    drop_page_cache

    # Start perf record attached to mysqld
    local perf_data="${RESULT_DIR}/perf_cb_q${q}.data"
    sudo perf record -F 99 -p "$MYSQLD_PID" --call-graph dwarf \
        -e cpu_core/cycles/ \
        -o "$perf_data" -- sleep 86400 &
    PERF_PID=$!
    sleep 0.5   # let perf attach before query starts

    local start_time end_time elapsed
    start_time=$(date +%s.%N)

    local raw_output
    raw_output=$(mysql --socket="$SOCKET" "$BENCHMARK_DB" \
        --batch --skip-column-names --force 2>/dev/null <<SQL
FLUSH STATUS;
${query_sql}
SELECT variable_name, variable_value
    FROM information_schema.rocksdb_perf_context
    ORDER BY variable_name;
SHOW SESSION STATUS LIKE 'Handler_read_first';
SHOW SESSION STATUS LIKE 'Handler_read_next';
SHOW SESSION STATUS LIKE 'Handler_read_rnd_next';
SQL
    )

    # Save raw output for debugging
    echo "$raw_output" > "${RESULT_DIR}/perf_ctx_raw_cb_q${q}.txt"

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
    local h_first h_nxt h_rnd rows_scanned
    h_first=$(echo "$raw_output" | awk '$1=="Handler_read_first"{print $2}')
    h_nxt=$(echo "$raw_output"   | awk '$1=="Handler_read_next"{print $2}')
    h_rnd=$(echo "$raw_output"   | awk '$1=="Handler_read_rnd_next"{print $2}')
    rows_scanned=$(( ${h_first:-0} + ${h_nxt:-0} + ${h_rnd:-0} ))

    printf "  elapsed=%.1fs | rows_scanned=%s | key_skipped=%s | block_reads=%s\n" \
        "$elapsed" "$rows_scanned" "${iksc:-0}" "${brc:-0}"

    echo "${q},${elapsed},${rows_scanned},${iksc:-0},${idsc:-0},${gst:-0},${brc:-0},${brb:-0},${brt:-0},${gfmc:-0},${gfmt:-0},${gfoft:-0}" \
        >> "$PERF_CSV"

    # Generate flamegraph
    if [ -s "$perf_data" ]; then
        local svg="${RESULT_DIR}/flamegraph_cb_q${q}.svg"
        sudo perf script -i "$perf_data" 2>/dev/null \
            | "${FLAMEGRAPH_DIR}/stackcollapse-perf.pl" \
            | "${FLAMEGRAPH_DIR}/flamegraph.pl" \
                --title "MyRocks ClickBench Q${q} ($(printf '%.1f' "$elapsed")s)" \
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
log_info "ClickBench profiling complete"
log_info "  Perf context CSV : $PERF_CSV"
log_info "  Flamegraphs      : ${RESULT_DIR}/flamegraph_cb_q*.svg"
log_info "  Resource summary : ${RESULT_DIR}/profiling_clickbench_resource_summary.csv"
log_info "=========================================="
