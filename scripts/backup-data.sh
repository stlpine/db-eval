#!/bin/bash
# Backup MySQL benchmark data to backup SSD
# Run this manually after prepare-data.sh completes

set -e
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"

usage() {
    cat << EOF
Usage: $0 [options]

Backup prepared MySQL benchmark data to backup SSD for fast restore.

Options:
    -e, --engine <engine>     Engine to backup (default: all engines with data):
                              - vanilla-innodb
                              - percona-innodb
                              - percona-myrocks
                              - all
    -j, --jobs <num>          Number of parallel copy jobs (default: 96)
    -h, --help                Show this help message

Prerequisites:
    1. Data must be prepared using prepare-data.sh
    2. MySQL must be stopped
    3. Backup SSD must be mounted (run: sudo ./scripts/setup-backup-ssd.sh mount)

Examples:
    $0 -e percona-myrocks
    $0 -e all
    $0 -e vanilla-innodb -j 64
EOF
    exit 1
}

# Default options
ENGINE="all"
PARALLEL_JOBS=96

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--engine)
            ENGINE="$2"
            shift 2
            ;;
        -j|--jobs)
            PARALLEL_JOBS="$2"
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

# Check backup SSD is available
check_backup_ssd() {
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

    CURRENT_MOUNT=$(findmnt -n -o TARGET "$BACKUP_SSD_DEVICE")
    if [ "$CURRENT_MOUNT" != "$BACKUP_SSD_MOUNT" ]; then
        log_error "Backup SSD is mounted at $CURRENT_MOUNT, expected $BACKUP_SSD_MOUNT"
        exit 1
    fi

    log_info "Backup SSD verified: $BACKUP_SSD_DEVICE -> $BACKUP_SSD_MOUNT"
}

# Check MySQL is stopped for given engine
check_mysql_stopped() {
    local engine=$1
    local pid_file=""

    case $engine in
        vanilla-innodb)
            pid_file="${MYSQL_PID_VANILLA_INNODB}"
            ;;
        percona-innodb)
            pid_file="${MYSQL_PID_PERCONA_INNODB}"
            ;;
        percona-myrocks)
            pid_file="${MYSQL_PID_PERCONA_MYROCKS}"
            ;;
    esac

    if [ -n "$pid_file" ] && [ -f "$pid_file" ]; then
        if kill -0 $(cat "$pid_file") 2>/dev/null; then
            log_error "MySQL ($engine) is running. Stop it first."
            return 1
        fi
    fi
    return 0
}

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

# Backup data for one engine using parallel copy
backup_engine() {
    local engine=$1
    local src_dir=$(get_datadir "$engine")
    local dst_dir="${BACKUP_DIR}/${engine}"

    if [ ! -d "$src_dir" ]; then
        log_info "No data found for $engine at $src_dir, skipping"
        return 0
    fi

    check_mysql_stopped "$engine" || exit 1

    log_info "Backing up $engine..."
    log_info "  Source: $src_dir"
    log_info "  Destination: $dst_dir"

    # Remove old backup if exists
    if [ -d "$dst_dir" ]; then
        log_info "  Removing old backup..."
        rm -rf "$dst_dir"
    fi

    # Create backup directory structure
    mkdir -p "$dst_dir"

    # Get source size for progress info
    local src_size=$(du -sh "$src_dir" 2>/dev/null | cut -f1)
    log_info "  Data size: $src_size"

    # Parallel copy
    log_info "  Copying with $PARALLEL_JOBS parallel jobs..."
    cd "$src_dir"

    # Create directory structure first
    find . -type d -exec mkdir -p "$dst_dir/{}" \;

    # Copy files in parallel
    find . -type f | parallel -j "$PARALLEL_JOBS" --will-cite --progress \
        cp --preserve=all {} "$dst_dir/{}"

    # Set proper permissions
    chmod 0700 "$dst_dir"

    log_info "  Backup complete for $engine"

    # Verify backup size
    local dst_size=$(du -sh "$dst_dir" 2>/dev/null | cut -f1)
    log_info "  Backup size: $dst_size"
}

# Main
log_info "=========================================="
log_info "Backup MySQL Benchmark Data"
log_info "=========================================="
log_info "Engine: $ENGINE"
log_info "Parallel jobs: $PARALLEL_JOBS"
log_info "Backup directory: $BACKUP_DIR"
log_info "=========================================="
echo ""

# Check prerequisites
check_backup_ssd

# Create backup directory
mkdir -p "$BACKUP_DIR"

# Determine engines to backup
ENGINES_TO_BACKUP=()
if [ "$ENGINE" = "all" ]; then
    for e in vanilla-innodb percona-innodb percona-myrocks; do
        datadir=$(get_datadir "$e")
        if [ -d "$datadir" ]; then
            ENGINES_TO_BACKUP+=("$e")
        fi
    done
else
    ENGINES_TO_BACKUP=("$ENGINE")
fi

if [ ${#ENGINES_TO_BACKUP[@]} -eq 0 ]; then
    log_error "No data found to backup"
    log_error "Run prepare-data.sh first"
    exit 1
fi

log_info "Engines to backup: ${ENGINES_TO_BACKUP[*]}"
echo ""

START_TIME=$(date +%s)

for engine in "${ENGINES_TO_BACKUP[@]}"; do
    backup_engine "$engine"
    echo ""
done

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

log_info "=========================================="
log_info "Backup completed!"
log_info "Duration: $DURATION seconds ($((DURATION / 60)) minutes)"
log_info "=========================================="
log_info ""
log_info "Backup location: $BACKUP_DIR"
ls -la "$BACKUP_DIR"
log_info ""
log_info "To restore, use: ./scripts/prepare-data.sh -e <engine> --from-backup"
log_info ""
