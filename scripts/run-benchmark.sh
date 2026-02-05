#!/bin/bash
# Run benchmark (assumes data is already prepared)
# Starts MySQL, runs benchmark, stops MySQL
# Does NOT cleanup data - allows multiple runs without re-preparing

set -e
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"

usage() {
    cat << EOF
Usage: $0 [options]

Options:
    -b, --benchmark <type>    Benchmark type (comma-separated or 'all'):
                              OLTP benchmarks:
                              - sysbench
                              - tpcc
                              - sysbench-tpcc
                              OLAP benchmarks:
                              - clickbench
                              - tpch-olap
                              Special:
                              - all (runs all OLTP benchmarks, default)
                              - all-olap (runs all OLAP benchmarks)
    -e, --engine <engine>     Engine to use (default: vanilla-innodb):
                              - vanilla-innodb
                              - percona-innodb
                              - percona-myrocks
    -h, --help                Show this help message

Prerequisites:
    Data must be prepared first using prepare-data.sh

Examples:
    $0 -e percona-innodb -b tpcc
    $0 -e percona-myrocks -b sysbench,tpcc
    $0 -e percona-innodb -b all
    $0 -e vanilla-innodb -b clickbench
    $0 -e percona-myrocks -b tpch-olap
    $0 -e vanilla-innodb -b all-olap
EOF
    exit 1
}

# Default options
BENCHMARK="all"
ENGINE="vanilla-innodb"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -b|--benchmark)
            BENCHMARK="$2"
            shift 2
            ;;
        -e|--engine)
            ENGINE="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            ;;
    esac
done

# Validate engine
case $ENGINE in
    vanilla-innodb|percona-innodb|percona-myrocks)
        ;;
    *)
        log_error "Invalid engine: $ENGINE"
        usage
        ;;
esac

# Determine which benchmarks to run
RUN_SYSBENCH=false
RUN_TPCC=false
RUN_SYSBENCH_TPCC=false
RUN_CLICKBENCH=false
RUN_TPCH_OLAP=false

if [ "$BENCHMARK" = "all" ]; then
    RUN_SYSBENCH=true
    RUN_TPCC=true
    RUN_SYSBENCH_TPCC=true
elif [ "$BENCHMARK" = "all-olap" ]; then
    RUN_CLICKBENCH=true
    RUN_TPCH_OLAP=true
else
    IFS=',' read -ra BENCH_ARRAY <<< "$BENCHMARK"
    for bench in "${BENCH_ARRAY[@]}"; do
        case $bench in
            sysbench)
                RUN_SYSBENCH=true
                ;;
            tpcc)
                RUN_TPCC=true
                ;;
            sysbench-tpcc)
                RUN_SYSBENCH_TPCC=true
                ;;
            clickbench)
                RUN_CLICKBENCH=true
                ;;
            tpch-olap)
                RUN_TPCH_OLAP=true
                ;;
            *)
                log_error "Invalid benchmark: $bench"
                usage
                ;;
        esac
    done
fi

log_info "=========================================="
log_info "Run Benchmark"
log_info "=========================================="
log_info "Engine: $ENGINE"
log_info "Benchmarks to run:"
[ "$RUN_SYSBENCH" = true ] && log_info "  - sysbench (OLTP)"
[ "$RUN_TPCC" = true ] && log_info "  - tpcc (OLTP)"
[ "$RUN_SYSBENCH_TPCC" = true ] && log_info "  - sysbench-tpcc (OLTP)"
[ "$RUN_CLICKBENCH" = true ] && log_info "  - clickbench (OLAP)"
[ "$RUN_TPCH_OLAP" = true ] && log_info "  - tpch-olap (OLAP)"
log_info "=========================================="
echo ""

# Check if MySQL service is already running and stop it
log_info "Checking for running MySQL service..."
if systemctl is-active --quiet mysql; then
    log_info "MySQL service is already running. Stopping it..."
    sudo systemctl stop mysql
    sleep 3

    if systemctl is-active --quiet mysql; then
        log_error "Failed to stop MySQL service. Please stop it manually and try again."
        exit 1
    fi
    log_info "MySQL service stopped successfully"
else
    log_info "MySQL service is not running"
fi
echo ""

# Verify SSD is mounted
check_ssd_mount || {
    log_error "SSD mount check failed"
    exit 1
}

# Set socket based on engine
case $ENGINE in
    vanilla-innodb)
        SOCKET="${MYSQL_SOCKET_VANILLA_INNODB}"
        ;;
    percona-innodb)
        SOCKET="${MYSQL_SOCKET_PERCONA_INNODB}"
        ;;
    percona-myrocks)
        SOCKET="${MYSQL_SOCKET_PERCONA_MYROCKS}"
        ;;
esac

# Helper function: Verify tables use the expected storage engine
verify_storage_engine() {
    local expected_engine
    case $ENGINE in
        percona-myrocks)
            expected_engine="ROCKSDB"
            ;;
        *)
            expected_engine="InnoDB"
            ;;
    esac

    local wrong_tables
    wrong_tables=$(mysql --socket="$SOCKET" -N -e "
        SELECT TABLE_NAME, ENGINE
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = '${BENCHMARK_DB}'
          AND ENGINE != '${expected_engine}';" 2>/dev/null)

    if [ -n "$wrong_tables" ]; then
        log_error "Storage engine mismatch! Expected $expected_engine for engine=$ENGINE"
        log_error "Tables using wrong engine:"
        echo "$wrong_tables" | while read -r table engine; do
            log_error "  $table: $engine"
        done
        log_error "Re-run prepare-data.sh with the correct engine."
        stop_mysql
        exit 1
    fi

    log_info "Storage engine verified: all tables use $expected_engine"
}

# Helper function: Drop OS page cache
drop_page_cache() {
    log_info "Dropping OS page cache..."
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
    log_info "Page cache dropped"
}

# Helper function: Start MySQL with cold buffer pool
start_mysql_cold() {
    ensure_mysql_stopped "$ENGINE"
    drop_page_cache
    log_info "Starting MySQL (cold)..."
    "${SCRIPT_DIR}/mysql-control.sh" "$ENGINE" start
    sleep 5

    if ! mysqladmin --socket="$SOCKET" ping &>/dev/null; then
        log_error "MySQL failed to start"
        exit 1
    fi
}

# Helper function: Stop MySQL
stop_mysql() {
    log_info "Stopping MySQL..."
    "${SCRIPT_DIR}/mysql-control.sh" "$ENGINE" stop
    sleep 3
}

# Helper function: Capture data profile (lightweight version to avoid memory issues under cgroup)
capture_data_profile() {
    local result_dir=$1
    local profile_file="${result_dir}/data_profile.txt"
    local profile_csv="${result_dir}/data_profile.csv"

    log_info "Capturing data profile..."

    # Generate CSV first - this query caches information_schema stats for subsequent queries
    # Using longer timeout (120s) for large datasets with many tables
    {
        echo "table_name,engine,rows,avg_row_bytes,data_mb,index_mb,total_mb"
        timeout 120 mysql --socket="$SOCKET" -N -e "
            SELECT
                TABLE_NAME,
                ENGINE,
                TABLE_ROWS,
                ROUND(AVG_ROW_LENGTH, 2),
                ROUND(DATA_LENGTH / 1024 / 1024, 2),
                ROUND(INDEX_LENGTH / 1024 / 1024, 2),
                ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2)
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = '${BENCHMARK_DB}'
            ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC;" 2>/dev/null | tr '\t' ','
    } > "$profile_csv" 2>&1 || true

    # Generate human-readable txt file using cached stats
    {
        echo "============================================================"
        echo "DATA PROFILE"
        echo "Generated: $(date)"
        echo "Database: $BENCHMARK_DB"
        echo "Engine: $ENGINE"
        echo "============================================================"
        echo ""

        echo "============================================================"
        echo "DATABASE SUMMARY"
        echo "============================================================"
        timeout 60 mysql --socket="$SOCKET" -e "
            SELECT
                COUNT(*) AS total_tables,
                SUM(TABLE_ROWS) AS total_rows,
                ROUND(SUM(DATA_LENGTH) / 1024 / 1024, 2) AS total_data_mb,
                ROUND(SUM(INDEX_LENGTH) / 1024 / 1024, 2) AS total_index_mb,
                ROUND((SUM(DATA_LENGTH) + SUM(INDEX_LENGTH)) / 1024 / 1024, 2) AS total_size_mb,
                ROUND((SUM(DATA_LENGTH) + SUM(INDEX_LENGTH)) / 1024 / 1024 / 1024, 2) AS total_size_gb
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = '${BENCHMARK_DB}';" 2>/dev/null || echo "(query failed or timed out)"
        echo ""

        echo "============================================================"
        echo "TABLE DETAILS"
        echo "============================================================"
        # Use data from CSV file (already generated above) to avoid redundant query
        if [ -s "$profile_csv" ] && [ "$(wc -l < "$profile_csv")" -gt 1 ]; then
            # Print header
            printf "%-20s %-10s %15s %15s %12s %12s %12s\n" \
                "TABLE_NAME" "ENGINE" "ROWS" "AVG_ROW_BYTES" "DATA_MB" "INDEX_MB" "TOTAL_MB"
            printf "%s\n" "$(printf '%.0s-' {1..100})"
            # Print data rows (skip header line)
            tail -n +2 "$profile_csv" | while IFS=',' read -r tname engine rows avgrow datamb idxmb totalmb; do
                printf "%-20s %-10s %15s %15s %12s %12s %12s\n" \
                    "$tname" "$engine" "$rows" "$avgrow" "$datamb" "$idxmb" "$totalmb"
            done
        else
            echo "(no data available)"
        fi
        echo ""

        echo "============================================================"
        echo "DISK USAGE (filesystem level)"
        echo "============================================================"
        case $ENGINE in
            vanilla-innodb)
                echo "Database directory:"
                du -sh "${MYSQL_DATADIR_VANILLA_INNODB}/${BENCHMARK_DB}" 2>/dev/null || echo "N/A"
                ;;
            percona-innodb)
                echo "Database directory:"
                du -sh "${MYSQL_DATADIR_PERCONA_INNODB}/${BENCHMARK_DB}" 2>/dev/null || echo "N/A"
                ;;
            percona-myrocks)
                echo "Database directory:"
                du -sh "${MYSQL_DATADIR_PERCONA_MYROCKS}/${BENCHMARK_DB}" 2>/dev/null || echo "N/A"
                echo ""
                echo "RocksDB data directory:"
                du -sh "${MYSQL_DATADIR_PERCONA_MYROCKS}/.rocksdb" 2>/dev/null || echo "N/A"
                ;;
        esac
        echo ""

    } > "$profile_file" 2>&1

    log_info "Data profile saved to: $profile_file"
    log_info "Data profile CSV saved to: $profile_csv"
}

# Helper function: Capture engine statistics
capture_engine_stats() {
    local result_dir=$1

    if [ "$ENGINE" = "percona-myrocks" ]; then
        capture_rocksdb_stats "$result_dir"
    else
        capture_innodb_stats "$result_dir"
    fi
}

# Helper function: Capture engine statistics with per-thread filename
capture_engine_stats_per_thread() {
    local result_dir=$1
    local threads=$2

    # Free up memory before running mysql client for stats collection
    # This helps when running under cgroup memory limits
    log_info "Waiting for memory to settle before capturing stats..."
    sleep 3
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null 2>&1 || true

    if [ "$ENGINE" = "percona-myrocks" ]; then
        capture_rocksdb_stats "$result_dir" "$threads"
    else
        capture_innodb_stats "$result_dir" "$threads"
    fi
}

# Helper function: Capture RocksDB statistics (for MyRocks only)
capture_rocksdb_stats() {
    local result_dir=$1
    local threads=${2:-}
    local query_timeout=30  # seconds per query

    log_info "Capturing RocksDB metrics..."

    local stats_file
    if [ -n "$threads" ]; then
        stats_file="${result_dir}/rocksdb_metrics_threads${threads}.txt"
    else
        stats_file="${result_dir}/rocksdb_metrics.txt"
    fi

    {
        echo "=== RocksDB Metrics (captured at $(date)) ==="
        echo ""
        echo "=== Engine Status (SHOW ENGINE ROCKSDB STATUS) ==="
        timeout "$query_timeout" mysql --socket="$SOCKET" -e "SHOW ENGINE ROCKSDB STATUS\G" 2>/dev/null || echo "(query timed out or failed)"
        echo ""
        echo "=== Column Family Statistics (ROCKSDB_CFSTATS) ==="
        timeout "$query_timeout" mysql --socket="$SOCKET" -e "SELECT * FROM INFORMATION_SCHEMA.ROCKSDB_CFSTATS;" 2>/dev/null || echo "(query timed out or failed)"
        echo ""
        echo "=== Compaction Statistics (ROCKSDB_COMPACTION_STATS) ==="
        timeout "$query_timeout" mysql --socket="$SOCKET" -e "SELECT * FROM INFORMATION_SCHEMA.ROCKSDB_COMPACTION_STATS;" 2>/dev/null || echo "(query timed out or failed)"
        echo ""
        echo "=== DB Statistics (ROCKSDB_DBSTATS) ==="
        timeout "$query_timeout" mysql --socket="$SOCKET" -e "SELECT * FROM INFORMATION_SCHEMA.ROCKSDB_DBSTATS;" 2>/dev/null || echo "(query timed out or failed)"
        echo ""
        echo "=== Performance Context (ROCKSDB_PERF_CONTEXT) ==="
        timeout "$query_timeout" mysql --socket="$SOCKET" -e "SELECT * FROM INFORMATION_SCHEMA.ROCKSDB_PERF_CONTEXT;" 2>/dev/null || echo "(query timed out or failed)"
        echo ""
        echo "=== Global Info (ROCKSDB_GLOBAL_INFO) ==="
        timeout "$query_timeout" mysql --socket="$SOCKET" -e "SELECT * FROM INFORMATION_SCHEMA.ROCKSDB_GLOBAL_INFO;" 2>/dev/null || echo "(query timed out or failed)"
        echo ""
        echo "=== SST File Info (ROCKSDB_INDEX_FILE_MAP) ==="
        timeout "$query_timeout" mysql --socket="$SOCKET" -e "SELECT CF_NAME, COUNT(*) as sst_count, SUM(NUM_ENTRIES) as total_entries FROM INFORMATION_SCHEMA.ROCKSDB_INDEX_FILE_MAP GROUP BY CF_NAME;" 2>/dev/null || echo "(query timed out or failed)"
    } > "$stats_file"

    log_info "RocksDB metrics saved to: $stats_file"
}

# Helper function: Capture InnoDB statistics
capture_innodb_stats() {
    local result_dir=$1
    local threads=${2:-}
    local query_timeout=30  # seconds per query

    log_info "Capturing InnoDB metrics..."

    local stats_file
    if [ -n "$threads" ]; then
        stats_file="${result_dir}/innodb_metrics_threads${threads}.txt"
    else
        stats_file="${result_dir}/innodb_metrics.txt"
    fi

    {
        echo "=== InnoDB Metrics (captured at $(date)) ==="
        echo ""
        echo "=== Engine Status (SHOW ENGINE INNODB STATUS) ==="
        timeout "$query_timeout" mysql --socket="$SOCKET" -e "SHOW ENGINE INNODB STATUS\G" 2>/dev/null || echo "(query timed out or failed)"
        echo ""
        echo "=== Global Status ==="
        timeout "$query_timeout" mysql --socket="$SOCKET" -e "SHOW GLOBAL STATUS;" 2>/dev/null || echo "(query timed out or failed)"
        echo ""
        echo "=== Buffer Pool Wait/Stall Metrics ==="
        timeout "$query_timeout" mysql --socket="$SOCKET" -e "SELECT NAME, COUNT, STATUS FROM INFORMATION_SCHEMA.INNODB_METRICS WHERE NAME IN ('buffer_pool_wait_free', 'buffer_pool_reads', 'buffer_pool_read_requests', 'buffer_pool_write_requests');" 2>/dev/null || echo "(query timed out or failed)"
        echo ""
        echo "=== Log Wait Metrics ==="
        timeout "$query_timeout" mysql --socket="$SOCKET" -e "SELECT NAME, COUNT, STATUS FROM INFORMATION_SCHEMA.INNODB_METRICS WHERE NAME LIKE '%log_wait%' OR NAME LIKE '%log_pending%' OR NAME IN ('log_waits', 'log_write_requests', 'log_writes');" 2>/dev/null || echo "(query timed out or failed)"
        echo ""
        echo "=== Checkpoint/Flush Metrics ==="
        timeout "$query_timeout" mysql --socket="$SOCKET" -e "SELECT NAME, COUNT, STATUS FROM INFORMATION_SCHEMA.INNODB_METRICS WHERE NAME LIKE '%checkpoint%' OR NAME LIKE '%flush%';" 2>/dev/null || echo "(query timed out or failed)"
        echo ""
        echo "=== Lock/Mutex Wait Metrics ==="
        timeout "$query_timeout" mysql --socket="$SOCKET" -e "SELECT NAME, COUNT, STATUS FROM INFORMATION_SCHEMA.INNODB_METRICS WHERE NAME LIKE '%lock_wait%' OR NAME LIKE '%mutex%';" 2>/dev/null || echo "(query timed out or failed)"
        echo ""
        echo "=== All InnoDB Metrics ==="
        timeout "$query_timeout" mysql --socket="$SOCKET" -e "SELECT NAME, COUNT, STATUS FROM INFORMATION_SCHEMA.INNODB_METRICS;" 2>/dev/null || echo "(query timed out or failed)"
    } > "$stats_file"

    log_info "InnoDB metrics saved to: $stats_file"
}

START_TIME=$(date +%s)

# Run benchmarks (MySQL restarted between each for cold buffer pool)
if [ "$RUN_SYSBENCH" = true ]; then
    log_info "=========================================="
    log_info "Running Sysbench benchmark..."
    log_info "=========================================="

    # Create result directory and log config once (need MySQL running for config capture)
    start_mysql_cold

    # Check if sysbench data exists
    if ! mysql --socket="$SOCKET" -e "SELECT 1 FROM ${BENCHMARK_DB}.sbtest1 LIMIT 1" &>/dev/null; then
        log_error "Sysbench data not found. Run prepare-data.sh first."
        stop_mysql
        exit 1
    fi

    verify_storage_engine

    # Create result directory
    SB_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    SB_RESULT_DIR="${RESULTS_DIR}/sysbench/${ENGINE}/${SB_TIMESTAMP}"
    mkdir -p "$SB_RESULT_DIR"
    log_info "Sysbench results directory: $SB_RESULT_DIR"

    # Log configuration (uses running MySQL to capture server variables)
    SB_CONFIG_LOG="${SB_RESULT_DIR}/benchmark_config.log"
    log_info "Logging configuration to: $SB_CONFIG_LOG"
    {
        echo "============================================================"
        echo "BENCHMARK CONFIGURATION LOG"
        echo "Generated: $(date)"
        echo "Engine: $ENGINE"
        echo "============================================================"
        echo ""
        echo "============================================================"
        echo "SYSTEM INFORMATION"
        echo "============================================================"
        echo "Hostname: $(hostname)"
        echo "Kernel: $(uname -r)"
        echo "OS: $(cat /etc/os-release 2>/dev/null | grep PRETTY_NAME | cut -d= -f2 | tr -d '"')"
        echo ""
        echo "CPU Info:"
        lscpu 2>/dev/null | grep -E "^(Model name|Socket|Core|Thread|CPU\(s\)|CPU MHz)" || cat /proc/cpuinfo | grep -E "^(model name|cpu cores|siblings)" | head -4
        echo ""
        echo "Memory Info:"
        free -h 2>/dev/null || cat /proc/meminfo | head -3
        echo ""
        echo "Disk Info:"
        df -h "$SSD_MOUNT" 2>/dev/null
        echo ""
        echo "============================================================"
        echo "BENCHMARK PARAMETERS (env.sh)"
        echo "============================================================"
        echo "BENCHMARK_DB: $BENCHMARK_DB"
        echo "BENCHMARK_THREADS: $BENCHMARK_THREADS"
        echo "BENCHMARK_DURATION: $BENCHMARK_DURATION"
        echo "SYSBENCH_TABLE_SIZE: $SYSBENCH_TABLE_SIZE"
        echo "SYSBENCH_TABLES: $SYSBENCH_TABLES"
        echo "SYSBENCH_WORKLOADS: $SYSBENCH_WORKLOADS"
        echo ""
        echo "============================================================"
        echo "MYSQL SERVER VARIABLES"
        echo "============================================================"
        mysql --socket="$SOCKET" -e "SHOW VARIABLES;" 2>/dev/null
        echo ""
    } > "$SB_CONFIG_LOG" 2>&1

    # Capture data profile (once, before benchmark runs)
    capture_data_profile "$SB_RESULT_DIR"

    stop_mysql

    # Per-thread loop: cold restart MySQL before each thread count
    for threads in $BENCHMARK_THREADS; do
        log_info "------------------------------------------"
        log_info "Sysbench: Starting cold run for $threads threads"
        log_info "------------------------------------------"

        start_mysql_cold
        verify_storage_engine

        if ! "${SCRIPT_DIR}/../sysbench/run.sh" "$ENGINE" "all" "$threads" "$SB_RESULT_DIR"; then
            log_error "Sysbench benchmark failed for $threads threads"
            exit 1
        fi

        capture_engine_stats_per_thread "$SB_RESULT_DIR" "$threads"
        stop_mysql
    done

    # Consolidate results
    log_info "Consolidating Sysbench results..."
    SB_CONSOLIDATED_CSV="${SB_RESULT_DIR}/consolidated_results.csv"
    echo "engine,workload,threads,tps,qps,latency_avg,latency_95p,latency_99p" > "$SB_CONSOLIDATED_CSV"

    for stats_file in "${SB_RESULT_DIR}"/*_stats.csv; do
        if [ -f "$stats_file" ]; then
            tail -n +2 "$stats_file" >> "$SB_CONSOLIDATED_CSV"
        fi
    done

    log_info "Sysbench benchmark completed!"
    log_info "Sysbench results: $SB_RESULT_DIR"
    log_info "Consolidated results: $SB_CONSOLIDATED_CSV"
fi

if [ "$RUN_TPCC" = true ]; then
    log_info "=========================================="
    log_info "Running TPC-C benchmark..."
    log_info "=========================================="

    # Create result directory and log config once (need MySQL running for config capture)
    start_mysql_cold

    # Check if tpcc data exists
    if ! mysql --socket="$SOCKET" -e "SELECT 1 FROM ${BENCHMARK_DB}.warehouse LIMIT 1" &>/dev/null; then
        log_error "TPC-C data not found. Run prepare-data.sh first."
        stop_mysql
        exit 1
    fi

    verify_storage_engine

    # Create result directory
    TPCC_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    TPCC_RESULT_DIR="${RESULTS_DIR}/tpcc/${ENGINE}/${TPCC_TIMESTAMP}"
    mkdir -p "$TPCC_RESULT_DIR"
    log_info "TPC-C results directory: $TPCC_RESULT_DIR"

    # Log configuration (uses running MySQL to capture server variables)
    TPCC_CONFIG_LOG="${TPCC_RESULT_DIR}/benchmark_config.log"
    log_info "Logging configuration to: $TPCC_CONFIG_LOG"
    {
        echo "============================================================"
        echo "BENCHMARK CONFIGURATION LOG"
        echo "Generated: $(date)"
        echo "Engine: $ENGINE"
        echo "============================================================"
        echo ""
        echo "============================================================"
        echo "SYSTEM INFORMATION"
        echo "============================================================"
        echo "Hostname: $(hostname)"
        echo "Kernel: $(uname -r)"
        echo "OS: $(cat /etc/os-release 2>/dev/null | grep PRETTY_NAME | cut -d= -f2 | tr -d '"')"
        echo ""
        echo "CPU Info:"
        lscpu 2>/dev/null | grep -E "^(Model name|Socket|Core|Thread|CPU\(s\)|CPU MHz)" || cat /proc/cpuinfo | grep -E "^(model name|cpu cores|siblings)" | head -4
        echo ""
        echo "Memory Info:"
        free -h 2>/dev/null || cat /proc/meminfo | head -3
        echo ""
        echo "Disk Info:"
        df -h "$SSD_MOUNT" 2>/dev/null
        echo ""
        echo "============================================================"
        echo "BENCHMARK PARAMETERS (env.sh)"
        echo "============================================================"
        echo "BENCHMARK_DB: $BENCHMARK_DB"
        echo "BENCHMARK_THREADS: $BENCHMARK_THREADS"
        echo "TPCC_WAREHOUSES: $TPCC_WAREHOUSES"
        echo "TPCC_DURATION: $TPCC_DURATION"
        echo ""
        echo "============================================================"
        echo "MYSQL SERVER VARIABLES"
        echo "============================================================"
        mysql --socket="$SOCKET" -e "SHOW VARIABLES;" 2>/dev/null
        echo ""
    } > "$TPCC_CONFIG_LOG" 2>&1

    # Capture data profile (once, before benchmark runs)
    capture_data_profile "$TPCC_RESULT_DIR"

    stop_mysql

    # Per-thread loop: cold restart MySQL before each thread count
    for threads in $BENCHMARK_THREADS; do
        log_info "------------------------------------------"
        log_info "TPC-C: Starting cold run for $threads threads"
        log_info "------------------------------------------"

        start_mysql_cold
        verify_storage_engine

        if ! "${SCRIPT_DIR}/../tpcc/run.sh" "$ENGINE" "$threads" "$TPCC_RESULT_DIR"; then
            log_error "TPC-C benchmark failed for $threads threads"
            exit 1
        fi

        capture_engine_stats_per_thread "$TPCC_RESULT_DIR" "$threads"
        stop_mysql
    done

    # Consolidate results
    log_info "Consolidating TPC-C results..."
    CONSOLIDATED_CSV="${TPCC_RESULT_DIR}/consolidated_results.csv"
    echo "engine,threads,warehouses,duration,tpmC,tpmTotal,latency_avg,latency_95" > "$CONSOLIDATED_CSV"

    for stats_file in "${TPCC_RESULT_DIR}"/tpcc_threads*_stats.csv; do
        if [ -f "$stats_file" ]; then
            tail -n +2 "$stats_file" >> "$CONSOLIDATED_CSV"
        fi
    done

    log_info "TPC-C benchmark completed!"
    log_info "TPC-C results: $TPCC_RESULT_DIR"
    log_info "Consolidated results: $CONSOLIDATED_CSV"
fi

if [ "$RUN_SYSBENCH_TPCC" = true ]; then
    log_info "=========================================="
    log_info "Running Sysbench-TPCC benchmark..."
    log_info "=========================================="

    # Create result directory and log config once (need MySQL running for config capture)
    start_mysql_cold

    # Check if sysbench-tpcc data exists
    if ! mysql --socket="$SOCKET" -e "SELECT 1 FROM ${BENCHMARK_DB}.warehouse1 LIMIT 1" &>/dev/null; then
        log_error "Sysbench-TPCC data not found. Run prepare-data.sh first."
        stop_mysql
        exit 1
    fi

    verify_storage_engine

    # Create result directory
    SBTPCC_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    SBTPCC_RESULT_DIR="${RESULTS_DIR}/sysbench-tpcc/${ENGINE}/${SBTPCC_TIMESTAMP}"
    mkdir -p "$SBTPCC_RESULT_DIR"
    log_info "Sysbench-TPCC results directory: $SBTPCC_RESULT_DIR"

    # Log configuration (uses running MySQL to capture server variables)
    SBTPCC_CONFIG_LOG="${SBTPCC_RESULT_DIR}/benchmark_config.log"
    log_info "Logging configuration to: $SBTPCC_CONFIG_LOG"
    {
        echo "============================================================"
        echo "BENCHMARK CONFIGURATION LOG"
        echo "Generated: $(date)"
        echo "Engine: $ENGINE"
        echo "============================================================"
        echo ""
        echo "============================================================"
        echo "SYSTEM INFORMATION"
        echo "============================================================"
        echo "Hostname: $(hostname)"
        echo "Kernel: $(uname -r)"
        echo "OS: $(cat /etc/os-release 2>/dev/null | grep PRETTY_NAME | cut -d= -f2 | tr -d '"')"
        echo ""
        echo "CPU Info:"
        lscpu 2>/dev/null | grep -E "^(Model name|Socket|Core|Thread|CPU\(s\)|CPU MHz)" || cat /proc/cpuinfo | grep -E "^(model name|cpu cores|siblings)" | head -4
        echo ""
        echo "Memory Info:"
        free -h 2>/dev/null || cat /proc/meminfo | head -3
        echo ""
        echo "Disk Info:"
        df -h "$SSD_MOUNT" 2>/dev/null
        echo ""
        echo "============================================================"
        echo "BENCHMARK PARAMETERS (env.sh)"
        echo "============================================================"
        echo "BENCHMARK_DB: $BENCHMARK_DB"
        echo "SYSBENCH_TPCC_TABLES: $SYSBENCH_TPCC_TABLES"
        echo "SYSBENCH_TPCC_SCALE: $SYSBENCH_TPCC_SCALE"
        echo "SYSBENCH_TPCC_THREADS: $SYSBENCH_TPCC_THREADS"
        echo "SYSBENCH_TPCC_DURATION: $SYSBENCH_TPCC_DURATION"
        echo "SYSBENCH_TPCC_WARMUP: $SYSBENCH_TPCC_WARMUP"
        echo "SYSBENCH_TPCC_REPORT_INTERVAL: $SYSBENCH_TPCC_REPORT_INTERVAL"
        echo ""
        echo "============================================================"
        echo "MYSQL SERVER VARIABLES"
        echo "============================================================"
        mysql --socket="$SOCKET" -e "SHOW VARIABLES;" 2>/dev/null
        echo ""
    } > "$SBTPCC_CONFIG_LOG" 2>&1

    # Capture data profile (once, before benchmark runs)
    capture_data_profile "$SBTPCC_RESULT_DIR"

    stop_mysql

    # Per-thread loop: cold restart MySQL before each thread count
    for threads in $SYSBENCH_TPCC_THREADS; do
        log_info "------------------------------------------"
        log_info "Sysbench-TPCC: Starting cold run for $threads threads"
        log_info "------------------------------------------"

        start_mysql_cold
        verify_storage_engine

        if ! "${SCRIPT_DIR}/../sysbench-tpcc/run.sh" "$ENGINE" "$threads" "$SBTPCC_RESULT_DIR"; then
            log_error "Sysbench-TPCC benchmark failed for $threads threads"
            exit 1
        fi

        capture_engine_stats_per_thread "$SBTPCC_RESULT_DIR" "$threads"
        stop_mysql
    done

    # Consolidate results
    log_info "Consolidating Sysbench-TPCC results..."
    SBTPCC_CONSOLIDATED_CSV="${SBTPCC_RESULT_DIR}/consolidated_results.csv"
    echo "engine,threads,tables,scale,warehouses,duration,tps,qps,latency_avg,latency_95p,latency_99p,tpmC,tpmTotal" > "$SBTPCC_CONSOLIDATED_CSV"

    for stats_file in "${SBTPCC_RESULT_DIR}"/tpcc_threads*_stats.csv; do
        if [ -f "$stats_file" ]; then
            tail -n +2 "$stats_file" >> "$SBTPCC_CONSOLIDATED_CSV"
        fi
    done

    log_info "Sysbench-TPCC benchmark completed!"
    log_info "Sysbench-TPCC results: $SBTPCC_RESULT_DIR"
    log_info "Consolidated results: $SBTPCC_CONSOLIDATED_CSV"
fi

# ============================================================
# OLAP BENCHMARKS
# Note: OLAP benchmarks run queries sequentially (no thread scaling)
# ============================================================

if [ "$RUN_CLICKBENCH" = true ]; then
    log_info "=========================================="
    log_info "Running ClickBench benchmark (OLAP)..."
    log_info "=========================================="

    # Start MySQL with cold buffer pool
    start_mysql_cold

    # Check if ClickBench data exists
    if ! mysql --socket="$SOCKET" -e "SELECT 1 FROM ${BENCHMARK_DB}.hits LIMIT 1" &>/dev/null; then
        log_error "ClickBench data not found. Run prepare-data.sh -b clickbench first."
        stop_mysql
        exit 1
    fi

    verify_storage_engine

    # Create result directory
    CB_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    CB_RESULT_DIR="${RESULTS_DIR}/clickbench/${ENGINE}/${CB_TIMESTAMP}"
    mkdir -p "$CB_RESULT_DIR"
    log_info "ClickBench results directory: $CB_RESULT_DIR"

    # Log configuration
    CB_CONFIG_LOG="${CB_RESULT_DIR}/benchmark_config.log"
    log_info "Logging configuration to: $CB_CONFIG_LOG"
    {
        echo "============================================================"
        echo "BENCHMARK CONFIGURATION LOG"
        echo "Generated: $(date)"
        echo "Engine: $ENGINE"
        echo "Benchmark: ClickBench (OLAP)"
        echo "============================================================"
        echo ""
        echo "============================================================"
        echo "SYSTEM INFORMATION"
        echo "============================================================"
        echo "Hostname: $(hostname)"
        echo "Kernel: $(uname -r)"
        echo "OS: $(cat /etc/os-release 2>/dev/null | grep PRETTY_NAME | cut -d= -f2 | tr -d '"')"
        echo ""
        echo "CPU Info:"
        lscpu 2>/dev/null | grep -E "^(Model name|Socket|Core|Thread|CPU\(s\)|CPU MHz)" || cat /proc/cpuinfo | grep -E "^(model name|cpu cores|siblings)" | head -4
        echo ""
        echo "Memory Info:"
        free -h 2>/dev/null || cat /proc/meminfo | head -3
        echo ""
        echo "Disk Info:"
        df -h "$SSD_MOUNT" 2>/dev/null
        echo ""
        echo "============================================================"
        echo "BENCHMARK PARAMETERS (env.sh)"
        echo "============================================================"
        echo "BENCHMARK_DB: $BENCHMARK_DB"
        echo "CLICKBENCH_QUERY_RUNS: $CLICKBENCH_QUERY_RUNS"
        echo "CLICKBENCH_QUERY_TIMEOUT: $CLICKBENCH_QUERY_TIMEOUT"
        echo ""
        echo "============================================================"
        echo "MYSQL SERVER VARIABLES"
        echo "============================================================"
        mysql --socket="$SOCKET" -e "SHOW VARIABLES;" 2>/dev/null
        echo ""
    } > "$CB_CONFIG_LOG" 2>&1

    # Capture data profile
    capture_data_profile "$CB_RESULT_DIR"

    # Run ClickBench (no thread loop - sequential query execution)
    if ! "${SCRIPT_DIR}/../clickbench/run.sh" "$ENGINE" "$CB_RESULT_DIR"; then
        log_error "ClickBench benchmark failed"
        stop_mysql
        exit 1
    fi

    capture_engine_stats "$CB_RESULT_DIR"
    stop_mysql

    log_info "ClickBench benchmark completed!"
    log_info "ClickBench results: $CB_RESULT_DIR"
fi

if [ "$RUN_TPCH_OLAP" = true ]; then
    log_info "=========================================="
    log_info "Running TPC-H benchmark (OLAP)..."
    log_info "=========================================="

    # Start MySQL with cold buffer pool
    start_mysql_cold

    # Check if TPC-H data exists (check lineitem as the largest table)
    if ! mysql --socket="$SOCKET" -e "SELECT 1 FROM ${BENCHMARK_DB}.lineitem LIMIT 1" &>/dev/null; then
        log_error "TPC-H data not found. Run prepare-data.sh -b tpch-olap first."
        stop_mysql
        exit 1
    fi

    verify_storage_engine

    # Create result directory
    TPCH_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    TPCH_RESULT_DIR="${RESULTS_DIR}/tpch-olap/${ENGINE}/${TPCH_TIMESTAMP}"
    mkdir -p "$TPCH_RESULT_DIR"
    log_info "TPC-H results directory: $TPCH_RESULT_DIR"

    # Log configuration
    TPCH_CONFIG_LOG="${TPCH_RESULT_DIR}/benchmark_config.log"
    log_info "Logging configuration to: $TPCH_CONFIG_LOG"
    {
        echo "============================================================"
        echo "BENCHMARK CONFIGURATION LOG"
        echo "Generated: $(date)"
        echo "Engine: $ENGINE"
        echo "Benchmark: TPC-H (OLAP)"
        echo "============================================================"
        echo ""
        echo "============================================================"
        echo "SYSTEM INFORMATION"
        echo "============================================================"
        echo "Hostname: $(hostname)"
        echo "Kernel: $(uname -r)"
        echo "OS: $(cat /etc/os-release 2>/dev/null | grep PRETTY_NAME | cut -d= -f2 | tr -d '"')"
        echo ""
        echo "CPU Info:"
        lscpu 2>/dev/null | grep -E "^(Model name|Socket|Core|Thread|CPU\(s\)|CPU MHz)" || cat /proc/cpuinfo | grep -E "^(model name|cpu cores|siblings)" | head -4
        echo ""
        echo "Memory Info:"
        free -h 2>/dev/null || cat /proc/meminfo | head -3
        echo ""
        echo "Disk Info:"
        df -h "$SSD_MOUNT" 2>/dev/null
        echo ""
        echo "============================================================"
        echo "BENCHMARK PARAMETERS (env.sh)"
        echo "============================================================"
        echo "BENCHMARK_DB: $BENCHMARK_DB"
        echo "TPCH_SCALE_FACTOR: $TPCH_SCALE_FACTOR"
        echo "TPCH_QUERY_RUNS: $TPCH_QUERY_RUNS"
        echo "TPCH_QUERY_TIMEOUT: $TPCH_QUERY_TIMEOUT"
        echo ""
        echo "============================================================"
        echo "MYSQL SERVER VARIABLES"
        echo "============================================================"
        mysql --socket="$SOCKET" -e "SHOW VARIABLES;" 2>/dev/null
        echo ""
    } > "$TPCH_CONFIG_LOG" 2>&1

    # Capture data profile
    capture_data_profile "$TPCH_RESULT_DIR"

    # Run TPC-H (no thread loop - sequential query execution)
    if ! "${SCRIPT_DIR}/../tpch-olap/run.sh" "$ENGINE" "$TPCH_RESULT_DIR"; then
        log_error "TPC-H benchmark failed"
        stop_mysql
        exit 1
    fi

    capture_engine_stats "$TPCH_RESULT_DIR"
    stop_mysql

    log_info "TPC-H benchmark completed!"
    log_info "TPC-H results: $TPCH_RESULT_DIR"
fi

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

log_info ""
log_info "=========================================="
log_info "Benchmark completed!"
log_info "Duration: $DURATION seconds ($((DURATION / 60)) minutes)"
log_info "=========================================="
log_info ""
log_info "Results saved in: ${RESULTS_DIR}/"
log_info ""
log_info "Data is preserved. You can:"
log_info "  - Run again:  ./scripts/run-benchmark.sh -e $ENGINE -b $BENCHMARK"
log_info "  - Cleanup:    ./scripts/cleanup-data.sh -e $ENGINE -b $BENCHMARK"
log_info ""
