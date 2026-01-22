#!/bin/bash
# MySQL Control Script - Start/Stop/Restart MySQL instances with different storage engines

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"

usage() {
    cat << EOF
Usage: $0 <engine> <action> [--mode <mode>]

Engine:
    vanilla-innodb    - Vanilla MySQL 8.4.7 with InnoDB
    percona-innodb    - Percona Server 8.4 with InnoDB
    percona-myrocks   - Percona Server 8.4 with MyRocks (RocksDB)

Action:
    start       - Start MySQL server
    stop        - Stop MySQL server
    restart     - Restart MySQL server
    status      - Check MySQL server status
    init        - Initialize MySQL data directory

Mode (optional, for start/restart only):
    --mode bulkload   - Use bulk load configuration (fast but unsafe)
                        Optimized for data preparation phase
    --mode benchmark  - Use benchmark configuration (default)
                        Safe settings for actual benchmarking

Example:
    $0 vanilla-innodb start
    $0 percona-innodb start --mode bulkload
    $0 percona-myrocks stop
EOF
    exit 1
}

if [ $# -lt 2 ]; then
    usage
fi

ENGINE=$1
ACTION=$2
CONFIG_MODE="benchmark"  # Default mode

# Parse optional --mode argument
shift 2
while [[ $# -gt 0 ]]; do
    case $1 in
        --mode)
            CONFIG_MODE="$2"
            if [[ "$CONFIG_MODE" != "bulkload" && "$CONFIG_MODE" != "benchmark" ]]; then
                log_error "Invalid mode: $CONFIG_MODE (must be 'bulkload' or 'benchmark')"
                exit 1
            fi
            shift 2
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            ;;
    esac
done

# Set engine-specific variables
case $ENGINE in
    vanilla-innodb)
        DATADIR="${MYSQL_DATADIR_VANILLA_INNODB}"
        SOCKET="${MYSQL_SOCKET_VANILLA_INNODB}"
        PID_FILE="${MYSQL_PID_VANILLA_INNODB}"
        if [ "$CONFIG_MODE" = "bulkload" ]; then
            CONFIG_FILE="${SCRIPT_DIR}/../common/config/my-vanilla-innodb-bulkload.cnf"
        else
            CONFIG_FILE="${SCRIPT_DIR}/../common/config/my-vanilla-innodb.cnf"
        fi
        ;;
    percona-innodb)
        DATADIR="${MYSQL_DATADIR_PERCONA_INNODB}"
        SOCKET="${MYSQL_SOCKET_PERCONA_INNODB}"
        PID_FILE="${MYSQL_PID_PERCONA_INNODB}"
        if [ "$CONFIG_MODE" = "bulkload" ]; then
            CONFIG_FILE="${SCRIPT_DIR}/../common/config/my-percona-innodb-bulkload.cnf"
        else
            CONFIG_FILE="${SCRIPT_DIR}/../common/config/my-percona-innodb.cnf"
        fi
        ;;
    percona-myrocks)
        DATADIR="${MYSQL_DATADIR_PERCONA_MYROCKS}"
        SOCKET="${MYSQL_SOCKET_PERCONA_MYROCKS}"
        PID_FILE="${MYSQL_PID_PERCONA_MYROCKS}"
        if [ "$CONFIG_MODE" = "bulkload" ]; then
            CONFIG_FILE="${SCRIPT_DIR}/../common/config/my-percona-myrocks-bulkload.cnf"
        else
            CONFIG_FILE="${SCRIPT_DIR}/../common/config/my-percona-myrocks.cnf"
        fi
        ;;
    *)
        log_error "Unknown engine: $ENGINE"
        usage
        ;;
esac

# Resolve config file to absolute path (required for mysqld)
CONFIG_FILE="$(realpath "$CONFIG_FILE")"

# Copy config file to /tmp so mysql user can read it
RUNTIME_CONFIG="/tmp/my-${ENGINE}.cnf"
cp "$CONFIG_FILE" "$RUNTIME_CONFIG"
chmod 644 "$RUNTIME_CONFIG"
CONFIG_FILE="$RUNTIME_CONFIG"

init_mysql() {
    log_info "Initializing MySQL data directory for $ENGINE at $DATADIR"

    # Check SSD device and mount
    log_info "Verifying SSD device and mount..."
    check_ssd_device || exit 1
    check_ssd_mount || exit 1

    # Wait for mount to settle if recently mounted
    wait_for_mount_settle

    # Remove existing data directory if it exists
    if [ -d "$DATADIR" ]; then
        log_info "Removing existing data directory..."
        sudo rm -rf "$DATADIR"
    fi

    # Create parent directory and set ownership (MySQL will create data dir itself)
    PARENT_DIR="$(dirname "$DATADIR")"
    sudo mkdir -p "$PARENT_DIR"
    sudo chown mysql:mysql "$PARENT_DIR" || exit 1

    # For MyRocks: create a temp config without RocksDB-specific options
    # because --initialize can't use RocksDB (system tables need InnoDB, plugins not loaded yet)
    INIT_CONFIG="$CONFIG_FILE"
    if [ "$ENGINE" = "percona-myrocks" ]; then
        INIT_CONFIG="/tmp/my-myrocks-init.cnf"
        grep -v -E "^default-storage-engine|^plugin-load|^rocksdb" "$CONFIG_FILE" > "$INIT_CONFIG"
        chmod 644 "$INIT_CONFIG"
    fi

    # Initialize MySQL
    log_info "Running mysqld --initialize-insecure..."
    sudo mysqld --defaults-file="$INIT_CONFIG" --initialize-insecure --user=mysql || {
        log_error "MySQL initialization failed"
        exit 1
    }

    log_info "MySQL data directory initialized successfully"
}

start_mysql() {
    log_info "Starting MySQL with $ENGINE engine (mode: $CONFIG_MODE)"
    if [ "$CONFIG_MODE" = "bulkload" ]; then
        log_info "WARNING: Using bulk load configuration"
    fi

    # Check SSD device and mount
    log_info "Verifying SSD device and mount..."
    check_ssd_device || exit 1
    check_ssd_mount || exit 1

    # Wait for mount to settle if recently mounted
    wait_for_mount_settle

    # Check if already running
    if [ -f "$PID_FILE" ] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
        log_error "MySQL is already running (PID: $(cat $PID_FILE))"
        exit 1
    fi

    # Check data directory exists
    if [ ! -d "$DATADIR" ]; then
        log_error "Data directory $DATADIR does not exist. Run 'init' first."
        exit 1
    fi

    # Ensure mysql user owns the data directory
    sudo chown -R mysql:mysql "$DATADIR"

    # Error log file
    ERROR_LOG="${DATADIR}/error.log"
    sudo rm -f "$ERROR_LOG"
    sudo touch "$ERROR_LOG"
    sudo chown mysql:mysql "$ERROR_LOG"

    # Start MySQL with error logging
    sudo mysqld --defaults-file="$CONFIG_FILE" --user=mysql --log-error="$ERROR_LOG" > /dev/null 2>&1 &

    # Wait for MySQL to start (MyRocks may take longer)
    log_info "Waiting for MySQL to start..."
    TIMEOUT=60
    for i in $(seq 1 $TIMEOUT); do
        if mysqladmin --socket="$SOCKET" ping &>/dev/null; then
            log_info "MySQL started successfully with $ENGINE engine"
            return 0
        fi
        sleep 1
    done

    log_error "MySQL failed to start within $TIMEOUT seconds"
    log_error "Check error log: $ERROR_LOG"
    if [ -f "$ERROR_LOG" ]; then
        log_error "Last 20 lines of error log:"
        tail -20 "$ERROR_LOG" >&2
    fi
    exit 1
}

stop_mysql() {
    log_info "Stopping MySQL with $ENGINE engine"

    if [ ! -f "$PID_FILE" ]; then
        log_error "MySQL is not running (no PID file found)"
        exit 1
    fi

    # Graceful shutdown
    mysqladmin --socket="$SOCKET" shutdown

    # Wait for shutdown
    log_info "Waiting for MySQL to stop..."
    for i in {1..30}; do
        if [ ! -f "$PID_FILE" ]; then
            log_info "MySQL stopped successfully"
            return 0
        fi
        sleep 1
    done

    # Force kill if still running
    if [ -f "$PID_FILE" ]; then
        log_error "MySQL did not stop gracefully, force killing..."
        sudo kill -9 $(cat "$PID_FILE")
        sudo rm -f "$PID_FILE"
    fi
}

restart_mysql() {
    stop_mysql
    sleep 2
    start_mysql
}

status_mysql() {
    if [ -f "$PID_FILE" ] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
        log_info "MySQL is running with $ENGINE engine (PID: $(cat $PID_FILE))"
        mysqladmin --socket="$SOCKET" status 2>/dev/null || true
    else
        log_info "MySQL is not running"
    fi
}

# Execute action
case $ACTION in
    init)
        init_mysql
        ;;
    start)
        start_mysql
        ;;
    stop)
        stop_mysql
        ;;
    restart)
        restart_mysql
        ;;
    status)
        status_mysql
        ;;
    *)
        log_error "Unknown action: $ACTION"
        usage
        ;;
esac
