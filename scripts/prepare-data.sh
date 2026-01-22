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
    --from-backup             Restore data from backup SSD instead of full preparation
    -j, --jobs <num>          Number of parallel copy jobs for restore (default: 96)
    --skip-reset              Skip SSD reset (use existing data directory)
    --full                    Force full data preparation (ignore backup)
    -h, --help                Show this help message

Examples:
    $0 -e percona-innodb -b tpcc
    $0 -e percona-myrocks --from-backup
    $0 -e percona-innodb -b all --skip-reset
    $0 -e vanilla-innodb --full
EOF
    exit 1
}

# Default options
BENCHMARK="all"
ENGINE="vanilla-innodb"
SKIP_RESET=false
FROM_BACKUP=false
FORCE_FULL=false
PARALLEL_JOBS=96

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
        --from-backup)
            FROM_BACKUP=true
            shift
            ;;
        -j|--jobs)
            PARALLEL_JOBS="$2"
            shift 2
            ;;
        --skip-reset)
            SKIP_RESET=true
            shift
            ;;
        --full)
            FORCE_FULL=true
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

# Get data directory for engine
get_datadir() {
    local engine=$1
    case $engine in
        vanilla-innodb)
            echo "${MYSQL_DATADIR_VANILLA_INNODB}"
            ;;
        percona-innodb)
            echo "${MYSQL_DATADIR_PERCONA_INNODB}"
            ;;
        percona-myrocks)
            echo "${MYSQL_DATADIR_PERCONA_MYROCKS}"
            ;;
    esac
}

# Check if backup exists for engine
check_backup_exists() {
    local engine=$1
    local backup_path="${BACKUP_DIR}/${engine}"

    if [ -d "$backup_path" ] && [ "$(ls -A "$backup_path" 2>/dev/null)" ]; then
        return 0
    fi
    return 1
}

# Check backup SSD is mounted
check_backup_ssd_mounted() {
    if [ -z "$BACKUP_SSD_DEVICE" ]; then
        return 1
    fi

    if ! findmnt -n "$BACKUP_SSD_DEVICE" &>/dev/null; then
        return 1
    fi

    return 0
}

# Restore data from backup SSD using parallel copy
restore_from_backup() {
    local engine=$1
    local backup_path="${BACKUP_DIR}/${engine}"
    local datadir=$(get_datadir "$engine")
    local parent_dir=$(dirname "$datadir")

    log_info "Restoring $engine from backup..."
    log_info "  Source: $backup_path"
    log_info "  Destination: $datadir"

    # Get backup size for progress info
    local backup_size=$(du -sh "$backup_path" 2>/dev/null | cut -f1)
    log_info "  Backup size: $backup_size"

    # Create parent directory if needed
    mkdir -p "$parent_dir"

    # Remove existing data directory if exists
    if [ -d "$datadir" ]; then
        log_info "  Removing existing data directory..."
        rm -rf "$datadir"
    fi

    # Create data directory with proper permissions
    mkdir -p -m 0700 "$datadir"

    # Parallel copy from backup
    log_info "  Copying with $PARALLEL_JOBS parallel jobs..."
    cd "$backup_path"

    # Create directory structure first
    find . -type d -exec mkdir -p "$datadir/{}" \;

    # Copy files in parallel
    find . -type f | parallel -j "$PARALLEL_JOBS" --will-cite --progress \
        cp --preserve=all {} "$datadir/{}"

    # Set proper permissions
    chmod 0700 "$datadir"

    log_info "  Restore complete for $engine"
}

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

# Determine if we should use backup
USE_BACKUP=false
if [ "$FROM_BACKUP" = true ]; then
    USE_BACKUP=true
elif [ "$FORCE_FULL" = false ] && check_backup_ssd_mounted && check_backup_exists "$ENGINE"; then
    # Auto-detect: use backup if available and --full not specified
    USE_BACKUP=true
    log_info "Backup detected for $ENGINE, using restore mode (use --full to force full preparation)"
fi

log_info "=========================================="
log_info "Prepare Benchmark Data"
log_info "=========================================="
log_info "Engine: $ENGINE"
log_info "Mode: $([ "$USE_BACKUP" = true ] && echo "Restore from backup" || echo "Full preparation")"
log_info "Skip SSD Reset: $SKIP_RESET"
if [ "$USE_BACKUP" = false ]; then
    log_info "Benchmarks to prepare:"
    [ "$PREPARE_SYSBENCH" = true ] && log_info "  - sysbench"
    [ "$PREPARE_TPCC" = true ] && log_info "  - tpcc"
    [ "$PREPARE_SYSBENCH_TPCC" = true ] && log_info "  - sysbench-tpcc"
fi
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

START_TIME=$(date +%s)

if [ "$USE_BACKUP" = true ]; then
    # ========================================
    # RESTORE FROM BACKUP MODE
    # ========================================

    # Verify backup SSD is mounted
    if ! check_backup_ssd_mounted; then
        log_error "Backup SSD is not mounted"
        log_error "Run: sudo ./scripts/setup-backup-ssd.sh mount"
        exit 1
    fi

    # Verify backup exists
    if ! check_backup_exists "$ENGINE"; then
        log_error "No backup found for $ENGINE at ${BACKUP_DIR}/${ENGINE}"
        log_error "Run backup-data.sh first or use --full for full preparation"
        exit 1
    fi

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

    # Restore data from backup
    log_info "=========================================="
    log_info "Restoring data from backup..."
    log_info "=========================================="
    restore_from_backup "$ENGINE"

else
    # ========================================
    # FULL PREPARATION MODE
    # ========================================

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

    # Stop MySQL
    log_info "Stopping MySQL..."
    "${SCRIPT_DIR}/mysql-control.sh" "$ENGINE" stop
    sleep 3
fi

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

log_info ""
log_info "=========================================="
log_info "Data preparation completed!"
log_info "Duration: $DURATION seconds ($((DURATION / 60)) minutes)"
log_info "=========================================="
log_info ""
log_info "Data is ready. Run benchmarks with:"
log_info "  ./scripts/run-benchmark.sh -e $ENGINE -b $BENCHMARK"
log_info ""
