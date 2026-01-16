#!/bin/bash
# Cleanup benchmark data
# Starts MySQL, drops benchmark tables, stops MySQL

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

Examples:
    $0 -e percona-innodb -b tpcc
    $0 -e percona-myrocks -b all
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

# Determine which benchmarks to cleanup
CLEANUP_SYSBENCH=false
CLEANUP_TPCC=false
CLEANUP_SYSBENCH_TPCC=false

if [ "$BENCHMARK" = "all" ]; then
    CLEANUP_SYSBENCH=true
    CLEANUP_TPCC=true
    CLEANUP_SYSBENCH_TPCC=true
else
    IFS=',' read -ra BENCH_ARRAY <<< "$BENCHMARK"
    for bench in "${BENCH_ARRAY[@]}"; do
        case $bench in
            sysbench)
                CLEANUP_SYSBENCH=true
                ;;
            tpcc)
                CLEANUP_TPCC=true
                ;;
            sysbench-tpcc)
                CLEANUP_SYSBENCH_TPCC=true
                ;;
            *)
                log_error "Invalid benchmark: $bench"
                usage
                ;;
        esac
    done
fi

log_info "=========================================="
log_info "Cleanup Benchmark Data"
log_info "=========================================="
log_info "Engine: $ENGINE"
log_info "Benchmarks to cleanup:"
[ "$CLEANUP_SYSBENCH" = true ] && log_info "  - sysbench"
[ "$CLEANUP_TPCC" = true ] && log_info "  - tpcc"
[ "$CLEANUP_SYSBENCH_TPCC" = true ] && log_info "  - sysbench-tpcc"
log_info "=========================================="
echo ""

# Check if SSD is mounted
check_ssd_mount || {
    log_error "SSD mount check failed"
    exit 1
}

# Stop any running MySQL and start fresh
ensure_mysql_stopped "$ENGINE"

# Start MySQL
log_info "Starting MySQL..."
"${SCRIPT_DIR}/mysql-control.sh" "$ENGINE" start
sleep 5

# Run cleanup for each benchmark type
if [ "$CLEANUP_SYSBENCH" = true ]; then
    log_info "Cleaning up Sysbench data..."
    "${SCRIPT_DIR}/../sysbench/cleanup.sh" "$ENGINE" || true
fi

if [ "$CLEANUP_TPCC" = true ]; then
    log_info "Cleaning up TPC-C data..."
    "${SCRIPT_DIR}/../tpcc/cleanup.sh" "$ENGINE" || true
fi

if [ "$CLEANUP_SYSBENCH_TPCC" = true ]; then
    log_info "Cleaning up Sysbench-TPCC data..."
    "${SCRIPT_DIR}/../sysbench-tpcc/cleanup.sh" "$ENGINE" || true
fi

# Stop MySQL
log_info "Stopping MySQL..."
"${SCRIPT_DIR}/mysql-control.sh" "$ENGINE" stop
sleep 3

log_info ""
log_info "=========================================="
log_info "Cleanup completed!"
log_info "=========================================="
log_info ""
log_info "To prepare new data, run:"
log_info "  ./scripts/prepare-data.sh -e $ENGINE -b $BENCHMARK"
log_info ""
