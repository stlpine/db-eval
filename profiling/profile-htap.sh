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
#   engine      percona-myrocks | percona-innodb (default: percona-myrocks)
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

RESULT_DIR="${2:-${RESULTS_DIR}/profiling/htap/${ENGINE}/$(date +%Y%m%d_%H%M%S)}"

# ── Helper functions (same pattern as all profiling scripts) ──────────────────

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

# snapshot_perf_context_global: fetch cumulative RocksDB/InnoDB counters.
# For MyRocks: information_schema.rocksdb_perf_context_global (global aggregates).
# For InnoDB:  SHOW GLOBAL STATUS (InnoDB_* counters).
# Output format: "variable_name value" one per line (tab-separated from MySQL,
# but awk matches on $1 so tab vs space doesn't matter).
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
    echo "run,elapsed_s,cutoff,rows_scanned,internal_key_skipped_count_delta,internal_delete_skipped_count_delta,get_snapshot_time_ns_delta,block_read_count_delta,block_read_byte_delta,block_read_time_ns_delta,get_from_memtable_count_delta,get_from_output_files_time_ns_delta" \
        > "${RESULT_DIR}/htap_olap_runs.csv"
else
    echo "run,elapsed_s,cutoff,rows_scanned,handler_read_key,innodb_rows_read_delta,innodb_buffer_pool_reads_delta,innodb_buffer_pool_read_requests_delta,innodb_pages_read_delta,innodb_data_reads_delta,innodb_data_read_bytes_delta" \
        > "${RESULT_DIR}/htap_olap_runs.csv"
fi

if [ "$ENGINE" = "percona-myrocks" ]; then
    echo "snapshot_num,elapsed_s,wall_clock_ts,internal_key_skipped_count,internal_delete_skipped_count,block_read_count,llt_count_active" \
        > "${RESULT_DIR}/htap_version_growth.csv"
else
    echo "snapshot_num,elapsed_s,wall_clock_ts,innodb_rows_read,innodb_rows_deleted,innodb_buffer_pool_reads,llt_count_active" \
        > "${RESULT_DIR}/htap_version_growth.csv"
fi

# ── Phase 1: Cold MySQL start ─────────────────────────────────────────────────

start_mysql_cold
verify_storage_engine
capture_data_profile "$RESULT_DIR"

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
if [ "$ENGINE" = "percona-myrocks" ]; then
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
if [ "$ENGINE" = "percona-myrocks" ]; then
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
    echo "HTAP_WARMUP_DURATION: $HTAP_WARMUP_DURATION"
    echo "HTAP_DURATION: $HTAP_DURATION"
    echo "HTAP_CTX_INTERVAL: $HTAP_CTX_INTERVAL"
    echo "HTAP_OLAP_RUNS: $HTAP_OLAP_RUNS"
    echo "CUTOFF: $CUTOFF"
    echo "BENCHMARK_DB: $BENCHMARK_DB"
    echo "CGROUP_MEMORY_LIMIT: $CGROUP_MEMORY_LIMIT"
    echo "FLAMEGRAPH_DIR: $FLAMEGRAPH_DIR"
    echo "PERF_EVENT: cpu_core/cycles/"
    echo "PERF_FREQ: 99 Hz"
    echo "PERF_CALL_GRAPH: dwarf"
    echo "NOTE: k index dropped on all tables (non-indexed join per AIDE paper)"
    echo "NOTE: LLTs hold GC back so versions accumulate across OLAP runs (version pressure visible in flamegraphs)"
    echo "NOTE: LLT sleep = HTAP_WARMUP_DURATION + HTAP_OLAP_RUNS * HTAP_QUERY_TIMEOUT = $((HTAP_WARMUP_DURATION + HTAP_OLAP_RUNS * HTAP_QUERY_TIMEOUT))s (covers full experimental window)"
    echo "NOTE: OLTP rand-type=pareto (skewed, hot rows accumulate long version chains per LLT paper §5.2.1)"
    echo "NOTE: Analytical sessions use REPEATABLE-READ (per AIDE §6.3 + LLT paper §5.1)"
    echo "NOTE: Memtable flushed before OLAP phase + 30s compaction settling wait (ensures versions in SSTables, stable background I/O)"
    echo "NOTE: ANALYZE TABLE run before OLAP loop to stabilise optimizer row estimates (MyRocks SST sampling unreliable)"
    echo "NOTE: RocksDB perf context captured PER-SESSION inside OLAP heredoc (CTX_SPLIT sentinel) — NOT from external monitor"
    echo "NOTE: Version growth loop uses probe scan (sbtest1 k<=1000) to measure per-probe internal_key_skipped_count growth"
    echo ""
    echo "============================================================"
    echo "MYSQL SERVER VARIABLES"
    echo "============================================================"
    mysql --socket="$SOCKET" -e "SHOW VARIABLES;" 2>/dev/null
    echo ""
} > "$CONFIG_LOG" 2>&1
log_info "Configuration logged to: $CONFIG_LOG"

MYSQLD_PID=$(cat "${PID_FILE}" 2>/dev/null || true)
if [ -z "$MYSQLD_PID" ] || ! kill -0 "$MYSQLD_PID" 2>/dev/null; then
    log_error "Cannot find mysqld PID"
    exit 1
fi
log_info "mysqld PID: $MYSQLD_PID"

# ── Cleanup trap ──────────────────────────────────────────────────────────────

PERF_PID=""
SNAPSHOT_PID=""
SB_PID=""
LLT_PIDS=()

cleanup() {
    [ -n "$PERF_PID" ]     && { sudo kill -INT "$PERF_PID" 2>/dev/null; wait "$PERF_PID" 2>/dev/null || true; }
    [ -n "$SNAPSHOT_PID" ] && { kill "$SNAPSHOT_PID" 2>/dev/null; wait "$SNAPSHOT_PID" 2>/dev/null || true; }
    for pid in "${LLT_PIDS[@]}"; do kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null || true; done
    [ -n "$SB_PID" ]       && { kill "$SB_PID" 2>/dev/null; wait "$SB_PID" 2>/dev/null || true; }
    stop_monitors
    generate_resource_summary "$RESULT_DIR" "profiling_htap"
    stop_mysql || true
}
trap cleanup EXIT

# ── Phase 3: Start OLTP background ───────────────────────────────────────────

OLTP_TOTAL=$(( HTAP_WARMUP_DURATION + HTAP_OLAP_RUNS * HTAP_QUERY_TIMEOUT + 60 ))
log_info "Starting OLTP background (${HTAP_OLTP_THREADS} threads, ${OLTP_TOTAL}s)..."

sysbench oltp_read_write \
    --mysql-socket="$SOCKET" \
    --mysql-db="$BENCHMARK_DB" \
    --tables="$HTAP_TABLES" \
    --table-size="$HTAP_TABLE_SIZE" \
    --threads="$HTAP_OLTP_THREADS" \
    --time="$OLTP_TOTAL" \
    --rand-type=pareto \
    --report-interval=10 \
    --db-ps-mode=disable \
    run > "${RESULT_DIR}/sysbench_htap_oltp.txt" 2>&1 &
SB_PID=$!
log_info "OLTP sysbench PID: $SB_PID"

# ── Phase 4: Open Long-Lived Transactions ─────────────────────────────────────

log_info "Opening ${HTAP_LLT_COUNT} long-lived transactions (LLTs)..."
LLT_PIDS=()
for (( i=1; i<=HTAP_LLT_COUNT; i++ )); do
    # Each LLT: SET REPEATABLE READ, extend timeouts, START TRANSACTION, SLEEP.
    # wait_timeout covers idle connections; net_read_timeout/net_write_timeout
    # cover active long-running queries (SELECT SLEEP is an active query).
    # All three are set for belt-and-suspenders coverage.
    mysql --socket="$SOCKET" "$BENCHMARK_DB" \
        --batch --force 2>/dev/null <<SQL &
SET SESSION transaction_isolation='REPEATABLE-READ';
SET SESSION wait_timeout=86400;
SET SESSION net_read_timeout=86400;
SET SESSION net_write_timeout=86400;
START TRANSACTION;
SELECT SLEEP($((HTAP_WARMUP_DURATION + HTAP_OLAP_RUNS * HTAP_QUERY_TIMEOUT)));
ROLLBACK;
SQL
    LLT_PIDS+=($!)
    log_info "  LLT $i PID: ${LLT_PIDS[-1]}"
done
log_info "LLTs opened (hold GC snapshot at current sequence number)"

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
if [ "$ENGINE" = "percona-myrocks" ]; then
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
        if [ "$ENGINE" = "percona-myrocks" ]; then
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
            ctx=$(mysql --socket="$SOCKET" "$BENCHMARK_DB" \
                --batch 2>/dev/null \
                -e "SET SESSION rocksdb_perf_context_level = ${PROFILING_PERF_CONTEXT_LEVEL};
                    SET SESSION transaction_isolation = 'REPEATABLE-READ';
                    SET SESSION max_execution_time = 5000;
                    SELECT COUNT(*) FROM sbtest1 WHERE k <= 1000;
                    SELECT * FROM information_schema.ROCKSDB_PERF_CONTEXT
                    WHERE TABLE_SCHEMA = '${BENCHMARK_DB}'
                      AND TABLE_NAME = 'sbtest1';") || true
            # ROCKSDB_PERF_CONTEXT schema: (TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, STAT_TYPE, VALUE)
            # One row per (table, metric). Find the STAT_TYPE and VALUE column indices from
            # the header row, then pick rows where STAT_TYPE matches the desired metric name.
            _perf_ctx_val() {
                local metric=$1
                echo "$ctx" | awk -v m="$metric" '
                    NR==1 {
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

log_info "Starting OLAP profiling loop (${HTAP_OLAP_RUNS} runs, cutoff=${CUTOFF})..."

for RUN in $(seq 1 "$HTAP_OLAP_RUNS"); do
    log_info "── OLAP Run ${RUN}/${HTAP_OLAP_RUNS} ──────────────────────────────"

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

    # Start perf record attached to mysqld
    perf_data="${RESULT_DIR}/perf_htap_run${RUN}.data"
    sudo perf record -F 99 -p "$MYSQLD_PID" --call-graph dwarf \
        -e cpu_core/cycles/ \
        -o "$perf_data" -- sleep 86400 &
    PERF_PID=$!
    sleep 0.5   # let perf attach before query starts

    start_time=$(date +%s.%N)

    # Run the analytical query.
    # For MyRocks: information_schema.rocksdb_perf_context is PER-SESSION.
    # Capturing it from an external connection always returns zeros (observed in
    # all 4 prior runs).  Instead, snapshot it WITHIN this session before and
    # after the join query.  The CTX_SPLIT sentinel separates before/after in
    # the raw output so the shell can compute per-run deltas.
    if [ "$ENGINE" = "percona-myrocks" ]; then
        # ROCKSDB_PERF_CONTEXT schema: (TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, STAT_TYPE, VALUE).
        # Query with SELECT * filtered to the four join tables; _ctx_delta sums VALUE
        # where STAT_TYPE matches the requested metric name.
        raw_output=$(mysql --socket="$SOCKET" "$BENCHMARK_DB" \
            --batch --force 2>/dev/null <<SQL
SET SESSION transaction_isolation='REPEATABLE-READ';
SET SESSION rocksdb_perf_context_level=${PROFILING_PERF_CONTEXT_LEVEL};
SET SESSION max_execution_time=$((HTAP_QUERY_TIMEOUT * 1000));
-- Force hash join so the plan is consistent across all 5 runs regardless of
-- MyRocks' inaccurate TABLE_ROWS estimates at cold start (run 1 would otherwise
-- use block nested loop, scanning ~400k rows instead of the hash-join ~300k).
SET SESSION optimizer_switch='block_nested_loop=off';
SET @htap_cutoff = ${CUTOFF};
SELECT * FROM information_schema.ROCKSDB_PERF_CONTEXT
WHERE TABLE_SCHEMA = '${BENCHMARK_DB}'
  AND TABLE_NAME IN ('sbtest1','sbtest2','sbtest3','sbtest4');
SELECT 'CTX_SPLIT' AS ctx_marker;
FLUSH STATUS;
${JOIN4_CONTENT}
SELECT * FROM information_schema.ROCKSDB_PERF_CONTEXT
WHERE TABLE_SCHEMA = '${BENCHMARK_DB}'
  AND TABLE_NAME IN ('sbtest1','sbtest2','sbtest3','sbtest4');
SHOW SESSION STATUS LIKE 'Handler_read_first';
SHOW SESSION STATUS LIKE 'Handler_read_next';
SHOW SESSION STATUS LIKE 'Handler_read_rnd_next';
SQL
        )
    else
        raw_output=$(mysql --socket="$SOCKET" "$BENCHMARK_DB" \
            --batch --skip-column-names --force 2>/dev/null <<SQL
SET SESSION transaction_isolation='REPEATABLE-READ';
SET SESSION max_execution_time=$((HTAP_QUERY_TIMEOUT * 1000));
SET @htap_cutoff = ${CUTOFF};
FLUSH STATUS;
${JOIN4_CONTENT}
SHOW SESSION STATUS LIKE 'Handler_read_first';
SHOW SESSION STATUS LIKE 'Handler_read_next';
SHOW SESSION STATUS LIKE 'Handler_read_rnd_next';
SHOW SESSION STATUS LIKE 'Handler_read_key';
SQL
        )
    fi

    end_time=$(date +%s.%N)
    elapsed=$(echo "$end_time - $start_time" | bc)

    # For MyRocks: split raw_output at CTX_SPLIT to get before/after perf context.
    # The ROCKSDB_PERF_CONTEXT table is columnar — column names depend on the build.
    # We store the raw sections verbatim; _ctx_delta reads named columns below.
    ctx_before=""
    ctx_after=""
    if [ "$ENGINE" = "percona-myrocks" ]; then
        ctx_before=$(echo "$raw_output" | awk '/^CTX_SPLIT/{exit} {print}')
        ctx_after=$(echo  "$raw_output" | awk 'f && /^Handler_/{exit} f{print} /^CTX_SPLIT/{f=1}')
    fi

    # Save raw output; append parsed before/after for human inspection
    echo "$raw_output" > "${RESULT_DIR}/perf_ctx_raw_run${RUN}.txt"
    if [ "$ENGINE" = "percona-myrocks" ]; then
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
    sudo kill -INT "$PERF_PID" 2>/dev/null || true
    wait "$PERF_PID" 2>/dev/null || true
    PERF_PID=""

    # Warn if query was very short (perf may not have attached in time)
    elapsed_int=$(echo "$elapsed" | awk '{printf "%d", $1}')
    if [ "${elapsed_int:-0}" -lt 2 ]; then
        log_error "  WARNING: query completed in <2s (${elapsed}s) — perf may not have attached in time"
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

    if [ "$ENGINE" = "percona-myrocks" ]; then
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
        printf "  run=%d elapsed=%.1fs | rows_scanned=%s | key_skipped=%s | block_reads=%s | llt_alive=%d\n" \
            "$RUN" "$elapsed" "$rows_scanned" "${iksc_delta:-0}" "${brc_delta:-0}" "$llt_alive"
        echo "${RUN},${elapsed},${CUTOFF},${rows_scanned},${iksc_delta:-0},${idsc_delta:-0},${gst_delta:-0},${brc_delta:-0},${brb_delta:-0},${brt_delta:-0},${gfmc_delta:-0},${gfoft_delta:-0}" \
            >> "${RESULT_DIR}/htap_olap_runs.csv"
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
        echo "${RUN},${elapsed},${CUTOFF},${rows_scanned},${h_key:-0},${inno_rows:-0},${inno_bp_reads:-0},${inno_bp_req:-0},${inno_pages:-0},${inno_data_reads:-0},${inno_data_bytes:-0}" \
            >> "${RESULT_DIR}/htap_olap_runs.csv"
    fi

    # Generate flamegraph
    if [ -s "$perf_data" ]; then
        svg="${RESULT_DIR}/flamegraph_htap_run${RUN}.svg"
        sudo perf script -i "$perf_data" 2>/dev/null \
            | "${FLAMEGRAPH_DIR}/stackcollapse-perf.pl" \
            | "${FLAMEGRAPH_DIR}/flamegraph.pl" \
                --title "${ENGINE} HTAP Join4 run${RUN} cutoff=${CUTOFF} ($(printf '%.1f' "$elapsed")s)" \
                --width 1800 \
            > "$svg" || log_error "  Flamegraph generation failed for run ${RUN}"
        log_info "  Flamegraph: $svg"
        sudo rm -f "$perf_data"
    else
        log_error "  perf data missing or empty for run ${RUN}, skipping flamegraph"
    fi
done

log_info "=========================================="
log_info "HTAP profiling complete"
log_info "  OLAP runs CSV    : ${RESULT_DIR}/htap_olap_runs.csv"
log_info "  Version growth   : ${RESULT_DIR}/htap_version_growth.csv"
log_info "  Flamegraphs      : ${RESULT_DIR}/flamegraph_htap_run*.svg"
log_info "  OLTP log         : ${RESULT_DIR}/sysbench_htap_oltp.txt"
log_info "  Resource summary : ${RESULT_DIR}/profiling_htap_resource_summary.csv"
log_info "=========================================="
