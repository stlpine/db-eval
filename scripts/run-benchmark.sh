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
                              - sysbench
                              - tpcc
                              - sysbench-tpcc
                              - all (default)
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

if [ "$BENCHMARK" = "all" ]; then
    RUN_SYSBENCH=true
    RUN_TPCC=true
    RUN_SYSBENCH_TPCC=true
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
[ "$RUN_SYSBENCH" = true ] && log_info "  - sysbench"
[ "$RUN_TPCC" = true ] && log_info "  - tpcc"
[ "$RUN_SYSBENCH_TPCC" = true ] && log_info "  - sysbench-tpcc"
log_info "=========================================="
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

START_TIME=$(date +%s)

# Run benchmarks (MySQL restarted between each for cold buffer pool)
if [ "$RUN_SYSBENCH" = true ]; then
    log_info "=========================================="
    log_info "Running Sysbench benchmark..."
    log_info "=========================================="

    start_mysql_cold

    # Check if sysbench data exists
    if ! mysql --socket="$SOCKET" -e "SELECT 1 FROM ${BENCHMARK_DB}.sbtest1 LIMIT 1" &>/dev/null; then
        log_error "Sysbench data not found. Run prepare-data.sh first."
        stop_mysql
        exit 1
    fi

    "${SCRIPT_DIR}/../sysbench/run.sh" "$ENGINE"

    LATEST_RESULT=$(ls -td "${RESULTS_DIR}/sysbench/${ENGINE}"/* 2>/dev/null | head -1)
    log_info "Sysbench results: $LATEST_RESULT"

    stop_mysql
fi

if [ "$RUN_TPCC" = true ]; then
    log_info "=========================================="
    log_info "Running TPC-C benchmark..."
    log_info "=========================================="

    start_mysql_cold

    # Check if tpcc data exists
    if ! mysql --socket="$SOCKET" -e "SELECT 1 FROM ${BENCHMARK_DB}.warehouse LIMIT 1" &>/dev/null; then
        log_error "TPC-C data not found. Run prepare-data.sh first."
        stop_mysql
        exit 1
    fi

    "${SCRIPT_DIR}/../tpcc/run.sh" "$ENGINE"

    LATEST_RESULT=$(ls -td "${RESULTS_DIR}/tpcc/${ENGINE}"/* 2>/dev/null | head -1)
    log_info "TPC-C results: $LATEST_RESULT"

    stop_mysql
fi

if [ "$RUN_SYSBENCH_TPCC" = true ]; then
    log_info "=========================================="
    log_info "Running Sysbench-TPCC benchmark..."
    log_info "=========================================="

    start_mysql_cold

    # Check if sysbench-tpcc data exists
    if ! mysql --socket="$SOCKET" -e "SELECT 1 FROM ${BENCHMARK_DB}.warehouse1 LIMIT 1" &>/dev/null; then
        log_error "Sysbench-TPCC data not found. Run prepare-data.sh first."
        stop_mysql
        exit 1
    fi

    "${SCRIPT_DIR}/../sysbench-tpcc/run.sh" "$ENGINE"

    LATEST_RESULT=$(ls -td "${RESULTS_DIR}/sysbench-tpcc/${ENGINE}"/* 2>/dev/null | head -1)
    log_info "Sysbench-TPCC results: $LATEST_RESULT"

    stop_mysql
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
