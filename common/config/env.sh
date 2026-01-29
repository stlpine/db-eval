#!/bin/bash
# Common environment configuration for MySQL benchmarking

# Ensure proper terminal line discipline for subprocess output
# This is critical when running under sudo/cgexec where terminal settings may not propagate properly
# Note: opost must be enabled for onlcr to take effect
if [ -t 1 ] || [ -t 0 ]; then
    stty opost onlcr 2>/dev/null || true
fi

# Force line buffering and proper newline handling for piped/redirected output
export PYTHONUNBUFFERED=1
# Ensure awk/sed output proper line endings
export LC_ALL=C.UTF-8 2>/dev/null || export LC_ALL=C

# MySQL Configuration
export MYSQL_VERSION="8.4.7"
export MYSQL_USER="benchuser"
export MYSQL_PASSWORD=""
export MYSQL_HOST="localhost"
export MYSQL_PORT="3306"

# SSD Device Discovery
_ENV_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export SSD_DEVICE="$("${_ENV_DIR}/../../scripts/find_ssd.sh")"

# SSD Mount Point - MODIFY THIS TO YOUR SSD MOUNT POINT
export SSD_MOUNT="/mnt/nvme"

# Backup SSD Configuration
export BACKUP_SSD_ID="nvme-Samsung_SSD_990_PRO_1TB_S6Z1NJ0XC01846F"
export BACKUP_SSD_DEVICE="$("${_ENV_DIR}/../../scripts/find_backup_ssd.sh" 2>/dev/null)"
export BACKUP_SSD_MOUNT="/mnt/nvme-backup"
export BACKUP_DIR="${BACKUP_SSD_MOUNT}/mysql-backup"

# SSD Temperature Settings
# Target temperature for cooldown before benchmarking (in Celsius)
export SSD_TARGET_TEMP="48"
# Enable/disable automatic cooldown before benchmarks
export SSD_COOLDOWN_ENABLED="true"

# SSD Mount Wait Settings
# Time to wait after mounting (in seconds) to allow filesystem to settle
export SSD_MOUNT_WAIT="60"

# Cgroup Configuration (for memory-limited benchmarking)
export CGROUP_NAME="limited_memory_group"
export CGROUP_MEMORY_LIMIT="16G"

# MySQL Data Directories
export MYSQL_DATADIR_VANILLA_INNODB="${SSD_MOUNT}/mysql-vanilla-innodb/data"
export MYSQL_DATADIR_PERCONA_INNODB="${SSD_MOUNT}/mysql-percona-innodb/data"
export MYSQL_DATADIR_PERCONA_MYROCKS="${SSD_MOUNT}/mysql-percona-myrocks/data"

# MySQL Socket and PID
export MYSQL_SOCKET_VANILLA_INNODB="/tmp/mysql_vanilla_innodb.sock"
export MYSQL_SOCKET_PERCONA_INNODB="/tmp/mysql_percona_innodb.sock"
export MYSQL_SOCKET_PERCONA_MYROCKS="/tmp/mysql_percona_myrocks.sock"

export MYSQL_PID_VANILLA_INNODB="/tmp/mysql_vanilla_innodb.pid"
export MYSQL_PID_PERCONA_INNODB="/tmp/mysql_percona_innodb.pid"
export MYSQL_PID_PERCONA_MYROCKS="/tmp/mysql_percona_myrocks.pid"

# Benchmark Configuration
export BENCHMARK_DB="benchmarkdb"
export BENCHMARK_THREADS="1 4 16 32"
export BENCHMARK_DURATION="300"  # seconds
export BENCHMARK_WARMUP="60"     # seconds

# Sysbench Configuration
export SYSBENCH_TABLE_SIZE="10000000"  # 10M rows per table
export SYSBENCH_TABLES="10"
export SYSBENCH_WORKLOADS="oltp_read_write oltp_read_only oltp_write_only"

# TPC-C Configuration
export TPCC_WAREHOUSES="2000"
export TPCC_DURATION="600"  # seconds

# Sysbench-TPCC Configuration
export SYSBENCH_TPCC_TABLES="1"          # Number of warehouse tables
export SYSBENCH_TPCC_SCALE="2000"          # Warehouses per table (total: tables × scale = 2000)
export SYSBENCH_TPCC_THREADS="1 4 16 32"  # Thread levels to test
export SYSBENCH_TPCC_DURATION="600"       # Benchmark duration (10 minutes, matching tpcc-mysql)
export SYSBENCH_TPCC_WARMUP="60"          # Warmup period in seconds
export SYSBENCH_TPCC_REPORT_INTERVAL="10" # Report interval in seconds
export SYSBENCH_TPCC_USE_FK="0"           # Disable foreign keys for fair MyRocks comparison
# Note: transaction-isolation and collation are set in my-*.cnf server configs

# Results Directory
export RESULTS_DIR="$(dirname $(dirname $(dirname $(readlink -f "${BASH_SOURCE[0]}"))))/results"

# Utility functions
log_info() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] INFO: $*"
}

log_error() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2
}

check_ssd_device() {
    if [ -z "$SSD_DEVICE" ]; then
        log_error "SSD device not found. Check scripts/find_ssd.sh"
        return 1
    fi
    if [ ! -b "$SSD_DEVICE" ]; then
        log_error "SSD device $SSD_DEVICE is not a valid block device!"
        return 1
    fi
    log_info "SSD device found: ${SSD_DEVICE}"
    return 0
}

check_ssd_mount() {
    if [ ! -d "${SSD_MOUNT}" ]; then
        log_error "SSD mount point ${SSD_MOUNT} does not exist!"
        log_error "You may need to mount the SSD first."
        log_error "Run: sudo mkdir -p ${SSD_MOUNT} && sudo mount ${SSD_DEVICE} ${SSD_MOUNT}"
        return 1
    fi

    # Verify the mount point is actually on the SSD
    if [ -n "$SSD_DEVICE" ]; then
        MOUNT_DEV=$(findmnt -n -o SOURCE -T "${SSD_MOUNT}" 2>/dev/null)
        if [ "$MOUNT_DEV" != "$SSD_DEVICE" ]; then
            log_error "Warning: ${SSD_MOUNT} is not mounted on ${SSD_DEVICE}"
            log_error "Currently mounted on: ${MOUNT_DEV}"
        fi
    fi

    log_info "SSD mount point verified: ${SSD_MOUNT}"
    return 0
}

wait_for_mount_settle() {
    # Check if mount was recent and wait for it to settle if needed
    if [ ! -d "${SSD_MOUNT}" ]; then
        return 0
    fi

    # Try to get mount timestamp (when the mount occurred)
    # This uses /proc/mounts which shows active mounts
    if [ -n "$SSD_DEVICE" ] && findmnt -n "$SSD_DEVICE" &>/dev/null; then
        # Check if there's a marker file we created after mount
        local marker_file="${SSD_MOUNT}/.mount_timestamp"

        if [ -f "$marker_file" ]; then
            local mount_time=$(cat "$marker_file" 2>/dev/null)
            local current_time=$(date +%s)

            if [ -n "$mount_time" ]; then
                local elapsed=$((current_time - mount_time))

                # If mount was recent (within settle time), wait for remaining time
                if [ $elapsed -lt $SSD_MOUNT_WAIT ]; then
                    local remaining=$((SSD_MOUNT_WAIT - elapsed))
                    log_info "Mount detected ${elapsed}s ago, waiting ${remaining}s more for filesystem to settle..."

                    for ((i=$remaining; i>0; i--)); do
                        if [ $i -eq 30 ] || [ $i -eq 10 ] || [ $i -eq 5 ]; then
                            echo "  ${i} seconds remaining..."
                        fi
                        sleep 1
                    done

                    log_info "Filesystem settle wait completed"
                fi
            fi

            # Remove the marker file as we've handled it
            rm -f "$marker_file" 2>/dev/null
        fi
    fi

    return 0
}

get_ssd_temp() {
    if [ -n "$SSD_DEVICE" ] && [ -b "$SSD_DEVICE" ]; then
        if command -v nvme &> /dev/null; then
            TEMP=$(sudo nvme smart-log "$SSD_DEVICE" 2>/dev/null | awk '/temperature/ {print $3}')
            if [ -n "$TEMP" ]; then
                echo "${TEMP}"
                return 0
            fi
        fi
    fi
    echo "N/A"
    return 1
}

get_ssd_info() {
    if [ -n "$SSD_DEVICE" ] && [ -b "$SSD_DEVICE" ]; then
        echo "SSD Device: $SSD_DEVICE"
        echo "Mount Point: $SSD_MOUNT"

        # Try to get temperature
        TEMP=$(get_ssd_temp)
        if [ "$TEMP" != "N/A" ]; then
            echo "Temperature: ${TEMP}°C (target: ${SSD_TARGET_TEMP}°C)"
        fi

        echo ""
        lsblk "$SSD_DEVICE" 2>/dev/null || true
        echo ""
        df -h "$SSD_MOUNT" 2>/dev/null || echo "Not mounted"
    else
        echo "SSD device not found or not configured"
    fi
}

wait_for_ssd_cooldown() {
    if [ "$SSD_COOLDOWN_ENABLED" != "true" ]; then
        log_info "SSD cooldown is disabled (SSD_COOLDOWN_ENABLED=false)"
        return 0
    fi

    log_info "Checking SSD temperature before benchmarking..."

    TEMP=$(get_ssd_temp)
    if [ "$TEMP" = "N/A" ]; then
        log_error "Cannot read SSD temperature. Install nvme-cli: sudo apt install nvme-cli"
        log_error "Or disable cooldown: export SSD_COOLDOWN_ENABLED=false"
        return 1
    fi

    log_info "Current SSD temperature: ${TEMP}°C (target: ${SSD_TARGET_TEMP}°C)"

    if [ "$TEMP" -le "$SSD_TARGET_TEMP" ]; then
        log_info "SSD temperature is within target range"
        return 0
    fi

    log_info "SSD needs to cool down. Running cooldown script..."
    "${_ENV_DIR}/../../scripts/wait_for_cooldown.sh" "$SSD_TARGET_TEMP"
    return $?
}

ensure_mysql_stopped() {
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
        kill $(cat "$pid_file") 2>/dev/null || true
        rm -f "$pid_file"
    fi

    sleep 2
}
