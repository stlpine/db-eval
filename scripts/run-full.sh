#!/bin/bash
# Full benchmark runner (prepare + run)
#
# For each benchmark type, this wrapper runs:
#   1. prepare-data.sh (includes SSD reset)
#   2. run-benchmark.sh
#
# When multiple benchmarks are specified, each is processed sequentially
# with a fresh SSD format before each, ensuring identical disk conditions.

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
    -s, --skip-prepare        Skip data preparation (use existing data)
    -h, --help                Show this help message

Each benchmark type is processed sequentially: prepare (with SSD reset) -> run.
This ensures identical disk conditions for each benchmark.

For memory-limited benchmarks, use run-cgroup.sh instead.

Examples:
    $0 -e percona-innodb -b tpcc
    $0 -e percona-myrocks -b all
    $0 -e percona-innodb -b tpcc --skip-prepare
EOF
    exit 1
}

# Default options
BENCHMARK="all"
ENGINE="vanilla-innodb"
SKIP_PREPARE=false

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
        -s|--skip-prepare)
            SKIP_PREPARE=true
            shift
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

# Build array of benchmark types
BENCH_ARRAY=()
if [ "$BENCHMARK" = "all" ]; then
    BENCH_ARRAY=("sysbench" "tpcc" "sysbench-tpcc")
else
    IFS=',' read -ra BENCH_ARRAY <<< "$BENCHMARK"
    # Validate benchmark types
    for bench in "${BENCH_ARRAY[@]}"; do
        case $bench in
            sysbench|tpcc|sysbench-tpcc)
                ;;
            *)
                log_error "Invalid benchmark: $bench"
                usage
                ;;
        esac
    done
fi

log_info "=========================================="
log_info "Full Benchmark Runner"
log_info "=========================================="
log_info "Engine: $ENGINE"
log_info "Benchmarks: ${BENCH_ARRAY[*]}"
log_info "Skip prepare: $SKIP_PREPARE"
log_info "=========================================="
echo ""

START_TIME=$(date +%s)

TOTAL_BENCHMARKS=${#BENCH_ARRAY[@]}
CURRENT=0

for bench in "${BENCH_ARRAY[@]}"; do
    CURRENT=$((CURRENT + 1))

    log_info "=========================================="
    log_info "Processing benchmark $CURRENT/$TOTAL_BENCHMARKS: $bench"
    log_info "=========================================="
    echo ""

    # Phase 1: Data preparation
    if [ "$SKIP_PREPARE" = false ]; then
        log_info "Phase 1: Data Preparation for $bench"
        log_info "  - SSD will be reset for clean state"
        echo ""

        "${SCRIPT_DIR}/prepare-data.sh" -e "$ENGINE" -b "$bench"

        echo ""
        log_info "Data preparation for $bench completed"
        echo ""
    fi

    # Phase 2: Run benchmark
    log_info "Phase 2: Run $bench Benchmark"
    echo ""

    "${SCRIPT_DIR}/run-benchmark.sh" -e "$ENGINE" -b "$bench"

    echo ""
    log_info "Benchmark $bench completed"
    echo ""
done

END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))

log_info ""
log_info "=========================================="
log_info "All benchmarks completed for $ENGINE!"
log_info "Benchmarks: ${BENCH_ARRAY[*]}"
log_info "Total duration: $TOTAL_DURATION seconds ($((TOTAL_DURATION / 60)) minutes)"
log_info "=========================================="
log_info ""
log_info "Results saved in: ${RESULTS_DIR}/"
log_info ""
