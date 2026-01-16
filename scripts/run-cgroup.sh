#!/bin/bash
# Run benchmarks with cgroup memory limit
#
# This wrapper runs:
#   1. prepare-data.sh WITHOUT cgroup (fast data loading)
#   2. run-benchmark.sh WITH cgroup (memory-limited benchmark)
#
# This ensures data preparation is fast while the actual benchmark
# runs under realistic memory constraints.

set -e
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"

CGROUP_PATH="/sys/fs/cgroup/${CGROUP_NAME}"

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
    --skip-prepare            Skip data preparation (data must already exist)
    -h, --help                Show this help message

This script runs data preparation WITHOUT cgroup limits (for speed),
then runs the actual benchmark WITH cgroup limits (for measurement).

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
        --skip-prepare)
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

# Check if cgroup exists
if [ ! -d "$CGROUP_PATH" ]; then
    log_error "Cgroup '${CGROUP_NAME}' does not exist"
    log_error "Run './scripts/setup-cgroup.sh' first"
    exit 1
fi

MEMORY_LIMIT=$(cat ${CGROUP_PATH}/memory.max 2>/dev/null | awk '{printf "%.2f GB", $1/1024/1024/1024}')

log_info "=========================================="
log_info "Benchmark with Cgroup Memory Limit"
log_info "=========================================="
log_info "Engine: $ENGINE"
log_info "Benchmark: $BENCHMARK"
log_info "Cgroup: ${CGROUP_NAME}"
log_info "Memory limit: ${MEMORY_LIMIT}"
log_info "Skip prepare: $SKIP_PREPARE"
log_info "=========================================="
echo ""

# Phase 1: Data preparation (WITHOUT cgroup - fast)
if [ "$SKIP_PREPARE" = false ]; then
    log_info "=========================================="
    log_info "Phase 1: Data Preparation (no cgroup limit)"
    log_info "=========================================="
    echo ""

    "${SCRIPT_DIR}/prepare-data.sh" -e "$ENGINE" -b "$BENCHMARK"

    echo ""
    log_info "Data preparation completed"
    echo ""
fi

# Phase 2: Run benchmark (WITH cgroup - memory limited)
log_info "=========================================="
log_info "Phase 2: Run Benchmark (cgroup limit: ${MEMORY_LIMIT})"
log_info "=========================================="
echo ""

exec sudo cgexec -g memory:${CGROUP_NAME} "${SCRIPT_DIR}/run-benchmark.sh" -e "$ENGINE" -b "$BENCHMARK"
