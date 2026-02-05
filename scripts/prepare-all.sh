#!/bin/bash
# Prepare and backup all benchmark data for all engines
#
# This script prepares data for each engine+benchmark combination and backs it up.
# Since all benchmarks use the same database, they are prepared separately.
#
# Backup structure:
#   /mnt/nvme-backup/mysql-backup/
#   ├── percona-innodb-sysbench/
#   ├── percona-innodb-tpcc/
#   ├── percona-innodb-sysbench-tpcc/
#   ├── percona-myrocks-sysbench/
#   ├── percona-myrocks-tpcc/
#   └── percona-myrocks-sysbench-tpcc/

set -e
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"

usage() {
    cat << EOF
Usage: $0 [options]

Prepare and backup all benchmark data for all engine+benchmark combinations.

Options:
    -e, --engine <engine>     Engine to prepare (default: all):
                              - percona-innodb
                              - percona-myrocks
                              - all
    -b, --benchmark <type>    Benchmark type (comma-separated, default: all):
                              OLTP benchmarks:
                              - sysbench
                              - tpcc
                              - sysbench-tpcc
                              OLAP benchmarks:
                              - clickbench
                              - tpch-olap
                              Special:
                              - all (all OLTP benchmarks, default)
                              - all-olap (all OLAP benchmarks)
    -j, --jobs <num>          Number of parallel copy jobs for backup (default: 96)
    --skip-backup             Skip backup step (only prepare data)
    -h, --help                Show this help message

This script will:
  1. For each engine+benchmark combination:
     a. Reset SSD and prepare fresh data
     b. Stop MySQL
     c. Backup data to backup SSD (excluding binary logs)

Examples:
    $0                                      # Prepare and backup everything
    $0 -e percona-innodb                    # Only percona-innodb, all benchmarks
    $0 -b tpcc                              # Only tpcc, all engines
    $0 -b tpcc,sysbench-tpcc                # TPC-C benchmarks only, all engines
    $0 -e percona-myrocks -b sysbench-tpcc  # Specific combination
EOF
    exit 1
}

# Default options
ENGINE="all"
BENCHMARK="all"
PARALLEL_JOBS=96
SKIP_BACKUP=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--engine)
            ENGINE="$2"
            shift 2
            ;;
        -b|--benchmark)
            BENCHMARK="$2"
            shift 2
            ;;
        -j|--jobs)
            PARALLEL_JOBS="$2"
            shift 2
            ;;
        --skip-backup)
            SKIP_BACKUP=true
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

# Build engine list
ENGINES=()
case $ENGINE in
    all)
        ENGINES=("percona-innodb" "percona-myrocks")
        ;;
    percona-innodb|percona-myrocks)
        ENGINES=("$ENGINE")
        ;;
    *)
        log_error "Invalid engine: $ENGINE"
        usage
        ;;
esac

# Build benchmark list
BENCHMARKS=()
if [ "$BENCHMARK" = "all" ]; then
    BENCHMARKS=("sysbench" "tpcc" "sysbench-tpcc")
elif [ "$BENCHMARK" = "all-olap" ]; then
    BENCHMARKS=("clickbench" "tpch-olap")
else
    IFS=',' read -ra BENCHMARKS <<< "$BENCHMARK"
    for bench in "${BENCHMARKS[@]}"; do
        case $bench in
            sysbench|tpcc|sysbench-tpcc|clickbench|tpch-olap)
                ;;
            *)
                log_error "Invalid benchmark: $bench"
                usage
                ;;
        esac
    done
fi

# Calculate total combinations
TOTAL=$((${#ENGINES[@]} * ${#BENCHMARKS[@]}))

log_info "=========================================="
log_info "Prepare and Backup All Benchmark Data"
log_info "=========================================="
log_info "Engines: ${ENGINES[*]}"
log_info "Benchmarks: ${BENCHMARKS[*]}"
log_info "Total combinations: $TOTAL"
log_info "Parallel jobs: $PARALLEL_JOBS"
log_info "Skip backup: $SKIP_BACKUP"
log_info "=========================================="
echo ""

# Check backup SSD is mounted (if not skipping backup)
if [ "$SKIP_BACKUP" = false ]; then
    if [ -z "$BACKUP_SSD_DEVICE" ]; then
        log_error "Backup SSD device not found"
        log_error "Check that the Samsung 990 PRO is connected"
        exit 1
    fi

    if ! findmnt -n "$BACKUP_SSD_DEVICE" &>/dev/null; then
        log_error "Backup SSD is not mounted"
        log_error "Run: sudo ./scripts/setup-backup-ssd.sh mount"
        exit 1
    fi

    log_info "Backup SSD verified: $BACKUP_SSD_DEVICE -> $BACKUP_SSD_MOUNT"
    echo ""
fi

START_TIME=$(date +%s)
CURRENT=0
FAILED=()

for engine in "${ENGINES[@]}"; do
    for bench in "${BENCHMARKS[@]}"; do
        CURRENT=$((CURRENT + 1))

        log_info "=========================================="
        log_info "[$CURRENT/$TOTAL] Preparing: $engine + $bench"
        log_info "=========================================="
        echo ""

        # Phase 1: Prepare data
        log_info "Phase 1: Preparing data..."
        if ! "${SCRIPT_DIR}/prepare-data.sh" -e "$engine" -b "$bench" --full; then
            log_error "Failed to prepare data for $engine + $bench"
            FAILED+=("$engine-$bench (prepare)")
            continue
        fi
        echo ""

        # Phase 2: Backup data
        if [ "$SKIP_BACKUP" = false ]; then
            log_info "Phase 2: Backing up data..."
            if ! "${SCRIPT_DIR}/backup-data.sh" -e "$engine" -b "$bench" -j "$PARALLEL_JOBS"; then
                log_error "Failed to backup data for $engine + $bench"
                FAILED+=("$engine-$bench (backup)")
                continue
            fi
            echo ""
        fi

        log_info "Completed: $engine + $bench"
        echo ""
    done
done

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

log_info ""
log_info "=========================================="
log_info "All Preparations Complete!"
log_info "=========================================="
log_info "Duration: $DURATION seconds ($((DURATION / 60)) minutes)"
log_info "Completed: $((CURRENT - ${#FAILED[@]}))/$TOTAL"

if [ ${#FAILED[@]} -gt 0 ]; then
    log_error "Failed combinations:"
    for f in "${FAILED[@]}"; do
        log_error "  - $f"
    done
fi

if [ "$SKIP_BACKUP" = false ]; then
    echo ""
    log_info "Backup location: $BACKUP_DIR"
    ls -la "$BACKUP_DIR" 2>/dev/null || true
fi

log_info ""
log_info "To run benchmarks, use:"
log_info "  ./scripts/run-cgroup.sh -e <engine> -b <benchmark>"
log_info ""
