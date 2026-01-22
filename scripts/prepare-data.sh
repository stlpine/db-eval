#!/bin/bash
# Prepare benchmark data
# Resets SSD, initializes MySQL, loads benchmark data, then stops MySQL
# Data persists for multiple benchmark runs

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
    --skip-reset              Skip SSD reset (use existing data directory)
    -h, --help                Show this help message

Examples:
    $0 -e percona-innodb -b tpcc
    $0 -e percona-myrocks -b sysbench,tpcc
    $0 -e percona-innodb -b all --skip-reset
EOF
    exit 1
}

# Default options
BENCHMARK="all"
ENGINE="vanilla-innodb"
SKIP_RESET=false

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
        --skip-reset)
            SKIP_RESET=true
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

# Determine which benchmarks to prepare
PREPARE_SYSBENCH=false
PREPARE_TPCC=false
PREPARE_SYSBENCH_TPCC=false

if [ "$BENCHMARK" = "all" ]; then
    PREPARE_SYSBENCH=true
    PREPARE_TPCC=true
    PREPARE_SYSBENCH_TPCC=true
else
    IFS=',' read -ra BENCH_ARRAY <<< "$BENCHMARK"
    for bench in "${BENCH_ARRAY[@]}"; do
        case $bench in
            sysbench)
                PREPARE_SYSBENCH=true
                ;;
            tpcc)
                PREPARE_TPCC=true
                ;;
            sysbench-tpcc)
                PREPARE_SYSBENCH_TPCC=true
                ;;
            *)
                log_error "Invalid benchmark: $bench"
                usage
                ;;
        esac
    done
fi

log_info "=========================================="
log_info "Prepare Benchmark Data"
log_info "=========================================="
log_info "Engine: $ENGINE"
log_info "Skip SSD Reset: $SKIP_RESET"
log_info "Benchmarks to prepare:"
[ "$PREPARE_SYSBENCH" = true ] && log_info "  - sysbench"
[ "$PREPARE_TPCC" = true ] && log_info "  - tpcc"
[ "$PREPARE_SYSBENCH_TPCC" = true ] && log_info "  - sysbench-tpcc"
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

# Stop any running MySQL instances
ensure_mysql_stopped "$ENGINE"

# Reset SSD if not skipped
if [ "$SKIP_RESET" = false ]; then
    log_info "Verifying SSD setup..."
    check_ssd_device || {
        log_error "SSD device check failed"
        exit 1
    }

    if [ "$SSD_COOLDOWN_ENABLED" = "true" ]; then
        log_info "Checking SSD temperature..."
        wait_for_ssd_cooldown || {
            log_error "SSD cooldown check failed"
            exit 1
        }
    fi

    log_info "Resetting SSD (unmount -> format -> mount)..."
    sudo "${SCRIPT_DIR}/setup-ssd.sh" reset --force || {
        log_error "SSD reset failed"
        exit 1
    }

    # Wait for filesystem to settle after mount
    wait_for_mount_settle

    log_info "SSD is ready"
else
    log_info "Skipping SSD reset (--skip-reset)"
    check_ssd_mount || {
        log_error "SSD mount check failed"
        exit 1
    }
fi

echo ""

# Initialize MySQL data directory
log_info "Initializing MySQL data directory..."
"${SCRIPT_DIR}/mysql-control.sh" "$ENGINE" init

# Start MySQL with bulk load configuration for faster data loading
log_info "Starting MySQL with bulk load configuration..."
"${SCRIPT_DIR}/mysql-control.sh" "$ENGINE" start --mode bulkload
sleep 5

# Prepare data for each benchmark type
START_TIME=$(date +%s)

if [ "$PREPARE_SYSBENCH" = true ]; then
    log_info "=========================================="
    log_info "Preparing Sysbench data..."
    log_info "=========================================="
    "${SCRIPT_DIR}/../sysbench/prepare.sh" "$ENGINE"
fi

if [ "$PREPARE_TPCC" = true ]; then
    log_info "=========================================="
    log_info "Preparing TPC-C data (this may take a while)..."
    log_info "=========================================="
    "${SCRIPT_DIR}/../tpcc/prepare.sh" "$ENGINE"
fi

if [ "$PREPARE_SYSBENCH_TPCC" = true ]; then
    log_info "=========================================="
    log_info "Preparing Sysbench-TPCC data..."
    log_info "=========================================="
    "${SCRIPT_DIR}/../sysbench-tpcc/prepare.sh" "$ENGINE"
fi

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

# Stop MySQL
log_info "Stopping MySQL..."
"${SCRIPT_DIR}/mysql-control.sh" "$ENGINE" stop
sleep 3

log_info ""
log_info "=========================================="
log_info "Data preparation completed!"
log_info "Duration: $DURATION seconds ($((DURATION / 60)) minutes)"
log_info "=========================================="
log_info ""
log_info "Data is ready. Run benchmarks with:"
log_info "  ./scripts/run-benchmark.sh -e $ENGINE -b $BENCHMARK"
log_info ""
