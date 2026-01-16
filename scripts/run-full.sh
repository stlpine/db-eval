#!/bin/bash
# Full benchmark runner (prepare + run)
# Convenience wrapper that calls prepare-data.sh followed by run-benchmark.sh

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

This script runs the full benchmark workflow:
  1. Reset SSD and prepare data (unless --skip-prepare)
  2. Run benchmarks

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

log_info "=========================================="
log_info "Full Benchmark Runner"
log_info "=========================================="
log_info "Engine: $ENGINE"
log_info "Benchmark: $BENCHMARK"
log_info "Skip prepare: $SKIP_PREPARE"
log_info "=========================================="
echo ""

START_TIME=$(date +%s)

# Phase 1: Data preparation
if [ "$SKIP_PREPARE" = false ]; then
    "${SCRIPT_DIR}/prepare-data.sh" -e "$ENGINE" -b "$BENCHMARK"
    echo ""
fi

# Phase 2: Run benchmark
"${SCRIPT_DIR}/run-benchmark.sh" -e "$ENGINE" -b "$BENCHMARK"

END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))

log_info ""
log_info "=========================================="
log_info "Full benchmark completed for $ENGINE!"
log_info "Total duration: $TOTAL_DURATION seconds ($((TOTAL_DURATION / 60)) minutes)"
log_info "=========================================="
log_info ""
log_info "Results saved in: ${RESULTS_DIR}/"
log_info ""
