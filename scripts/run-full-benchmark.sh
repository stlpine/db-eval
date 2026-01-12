#!/bin/bash
# Automated Full Benchmark Runner
# Runs complete benchmark suite for both InnoDB and MyRocks

set -e          # Exit immediately on error
set -o pipefail # Fail on pipe errors

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"

usage() {
    cat << EOF
Usage: $0 [options]

Options:
    -b, --benchmark <type>    Benchmark type: sysbench, tpcc, sysbench-tpcc, or all (default: all)
    -e, --engine <engine>     Engine to test (default: vanilla-innodb):
                              - vanilla-innodb  : Vanilla MySQL with InnoDB
                              - percona-innodb  : Percona Server with InnoDB
                              - percona-myrocks : Percona Server with MyRocks
    -s, --skip-prepare        Skip data preparation phase
    -h, --help                Show this help message

Examples:
    $0                                          # Run all benchmarks for vanilla-innodb (default)
    $0 -e vanilla-innodb                        # Run all benchmarks for vanilla MySQL InnoDB
    $0 -e percona-innodb                        # Run all benchmarks for Percona Server InnoDB
    $0 -e percona-myrocks                       # Run all benchmarks for Percona Server MyRocks
    $0 -b sysbench -e vanilla-innodb            # Run only sysbench for vanilla MySQL InnoDB
    $0 -b sysbench-tpcc -e percona-myrocks      # Run only sysbench-tpcc for Percona MyRocks
    $0 -e percona-myrocks -s                    # Run all benchmarks, skip data preparation

This script will:
1. Reset the SSD (unmount → format → mount)
2. Initialize MySQL data directory
3. Start MySQL
4. Prepare benchmark data (unless -s specified)
5. Run benchmarks
6. Cleanup and stop MySQL
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

# Determine which benchmarks to run
case $BENCHMARK in
    sysbench|tpcc|sysbench-tpcc|all)
        ;;
    *)
        log_error "Invalid benchmark: $BENCHMARK"
        usage
        ;;
esac

log_info "=========================================="
log_info "Full Benchmark Runner"
log_info "=========================================="
log_info "Benchmark: $BENCHMARK"
log_info "Engine: $ENGINE"
log_info "Skip Prepare: $SKIP_PREPARE"
log_info "=========================================="
log_info ""

# Verify SSD device and mount before starting
log_info "Verifying SSD setup..."
check_ssd_device || {
    log_error "SSD device check failed. Run: ${SCRIPT_DIR}/setup-ssd.sh check"
    exit 1
}
check_ssd_mount || {
    log_error "SSD mount check failed. Run: sudo ${SCRIPT_DIR}/setup-ssd.sh mount"
    exit 1
}
log_info "SSD verification completed successfully"
log_info ""

# Wait for SSD to cool down if needed
if [ "$SSD_COOLDOWN_ENABLED" = "true" ]; then
    log_info "Checking SSD temperature..."
    wait_for_ssd_cooldown || {
        log_error "SSD cooldown check failed or was interrupted"
        exit 1
    }
    log_info ""
fi

# Function to run sysbench for an engine
run_sysbench() {
    local engine=$1

    log_info "=========================================="
    log_info "Running Sysbench for $engine"
    log_info "=========================================="

    # Initialize MySQL data directory (required after SSD reset)
    log_info "Initializing MySQL data directory..."
    "${SCRIPT_DIR}/mysql-control.sh" "$engine" init

    # Start MySQL
    log_info "Starting MySQL..."
    "${SCRIPT_DIR}/mysql-control.sh" "$engine" start
    sleep 5

    # Prepare data
    if [ "$SKIP_PREPARE" = false ]; then
        log_info "Preparing sysbench data..."
        "${SCRIPT_DIR}/../sysbench/prepare.sh" "$engine"
    fi

    # Run benchmark
    log_info "Running sysbench benchmark..."
    "${SCRIPT_DIR}/../sysbench/run.sh" "$engine"

    # Show result directory
    LATEST_RESULT=$(ls -td "${RESULTS_DIR}/sysbench/${engine}"/* 2>/dev/null | head -1)
    log_info "Results saved to: $LATEST_RESULT"

    # Cleanup
    log_info "Cleaning up..."
    "${SCRIPT_DIR}/../sysbench/cleanup.sh" "$engine"

    # Stop MySQL
    log_info "Stopping MySQL..."
    "${SCRIPT_DIR}/mysql-control.sh" "$engine" stop
    sleep 5

    log_info "Sysbench for $engine completed"
}

# Function to run TPC-C for an engine
run_tpcc() {
    local engine=$1

    log_info "=========================================="
    log_info "Running TPC-C for $engine"
    log_info "=========================================="

    # Initialize MySQL data directory (required after SSD reset)
    log_info "Initializing MySQL data directory..."
    "${SCRIPT_DIR}/mysql-control.sh" "$engine" init

    # Start MySQL
    log_info "Starting MySQL..."
    "${SCRIPT_DIR}/mysql-control.sh" "$engine" start
    sleep 5

    # Prepare data
    if [ "$SKIP_PREPARE" = false ]; then
        log_info "Preparing TPC-C data (this will take a long time)..."
        "${SCRIPT_DIR}/../tpcc/prepare.sh" "$engine"
    fi

    # Run benchmark
    log_info "Running TPC-C benchmark..."
    "${SCRIPT_DIR}/../tpcc/run.sh" "$engine"

    # Show result directory
    LATEST_RESULT=$(ls -td "${RESULTS_DIR}/tpcc/${engine}"/* 2>/dev/null | head -1)
    log_info "Results saved to: $LATEST_RESULT"

    # Cleanup
    log_info "Cleaning up..."
    "${SCRIPT_DIR}/../tpcc/cleanup.sh" "$engine"

    # Stop MySQL
    log_info "Stopping MySQL..."
    "${SCRIPT_DIR}/mysql-control.sh" "$engine" stop
    sleep 5

    log_info "TPC-C for $engine completed"
}

# Function to run sysbench-tpcc for an engine
run_sysbench_tpcc() {
    local engine=$1

    log_info "=========================================="
    log_info "Running Sysbench-TPCC for $engine"
    log_info "=========================================="

    # Initialize MySQL data directory (required after SSD reset)
    log_info "Initializing MySQL data directory..."
    "${SCRIPT_DIR}/mysql-control.sh" "$engine" init

    # Start MySQL
    log_info "Starting MySQL..."
    "${SCRIPT_DIR}/mysql-control.sh" "$engine" start
    sleep 5

    # Prepare data
    if [ "$SKIP_PREPARE" = false ]; then
        log_info "Preparing sysbench-tpcc data..."
        "${SCRIPT_DIR}/../sysbench-tpcc/prepare.sh" "$engine"
    fi

    # Run benchmark
    log_info "Running sysbench-tpcc benchmark..."
    "${SCRIPT_DIR}/../sysbench-tpcc/run.sh" "$engine"

    # Show result directory
    LATEST_RESULT=$(ls -td "${RESULTS_DIR}/sysbench-tpcc/${engine}"/* 2>/dev/null | head -1)
    log_info "Results saved to: $LATEST_RESULT"

    # Cleanup
    log_info "Cleaning up..."
    "${SCRIPT_DIR}/../sysbench-tpcc/cleanup.sh" "$engine"

    # Stop MySQL
    log_info "Stopping MySQL..."
    "${SCRIPT_DIR}/mysql-control.sh" "$engine" stop
    sleep 5

    log_info "Sysbench-TPCC for $engine completed"
}

# Main execution
START_TIME=$(date +%s)

# Stop any running MySQL instances
ensure_mysql_stopped "$ENGINE"

log_info ""
log_info "=========================================="
log_info "Preparing SSD for $ENGINE benchmarks"
log_info "=========================================="

# Reset SSD to ensure clean state
# This includes: unmount, format (ext4), mount, wait for settle, check temperature
log_info "Resetting SSD (unmount → format → mount)..."
sudo "${SCRIPT_DIR}/setup-ssd.sh" reset --force || {
    log_error "SSD reset failed"
    exit 1
}

log_info "SSD is ready for benchmarking"
log_info ""

if [ "$BENCHMARK" = "sysbench" ] || [ "$BENCHMARK" = "all" ]; then
    run_sysbench "$ENGINE"
fi

if [ "$BENCHMARK" = "tpcc" ] || [ "$BENCHMARK" = "all" ]; then
    run_tpcc "$ENGINE"
fi

if [ "$BENCHMARK" = "sysbench-tpcc" ] || [ "$BENCHMARK" = "all" ]; then
    run_sysbench_tpcc "$ENGINE"
fi

END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))

log_info ""
log_info "=========================================="
log_info "Benchmark completed for $ENGINE!"
log_info "Total duration: $TOTAL_DURATION seconds ($((TOTAL_DURATION / 60)) minutes)"
log_info "=========================================="
log_info ""
log_info "Results saved in: ${RESULTS_DIR}/"
log_info ""
log_info "To compare results after running multiple engines, use:"
log_info "  ./scripts/compare-results.sh sysbench <result_dir1> <result_dir2>"
log_info "=========================================="
