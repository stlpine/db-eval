#!/bin/bash
# Sysbench Data Preparation Script

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"

usage() {
    echo "Usage: $0 <engine>"
    echo "Engines: vanilla-innodb, percona-innodb, percona-myrocks"
    exit 1
}

if [ $# -ne 1 ]; then
    usage
fi

ENGINE=$1

# Set engine-specific variables
case $ENGINE in
    vanilla-innodb)
        SOCKET="${MYSQL_SOCKET_VANILLA_INNODB}"
        STORAGE_ENGINE="innodb"
        ;;
    percona-innodb)
        SOCKET="${MYSQL_SOCKET_PERCONA_INNODB}"
        STORAGE_ENGINE="innodb"
        ;;
    percona-myrocks)
        SOCKET="${MYSQL_SOCKET_PERCONA_MYROCKS}"
        STORAGE_ENGINE="rocksdb"
        ;;
    *)
        log_error "Unknown engine: $ENGINE"
        usage
        ;;
esac

# Check if MySQL is running
if ! mysqladmin --socket="$SOCKET" ping &>/dev/null; then
    log_error "MySQL is not running. Please start MySQL first using: ./scripts/mysql-control.sh $ENGINE start"
    exit 1
fi

log_info "Preparing sysbench data for $ENGINE"
log_info "Configuration: ${SYSBENCH_TABLES} tables, ${SYSBENCH_TABLE_SIZE} rows each"

# Create database
log_info "Creating database: $BENCHMARK_DB"
mysql --socket="$SOCKET" -e "DROP DATABASE IF EXISTS $BENCHMARK_DB;"
mysql --socket="$SOCKET" -e "CREATE DATABASE $BENCHMARK_DB;"

# Create sysbench user if it doesn't exist
log_info "Creating sysbench user..."
mysql --socket="$SOCKET" -e "CREATE USER IF NOT EXISTS 'sbtest'@'localhost' IDENTIFIED BY '';"
mysql --socket="$SOCKET" -e "GRANT ALL PRIVILEGES ON $BENCHMARK_DB.* TO 'sbtest'@'localhost';"
mysql --socket="$SOCKET" -e "FLUSH PRIVILEGES;"

# Run sysbench prepare
# Note: Storage engine is determined by MySQL's default-storage-engine setting
log_info "Running sysbench prepare (this may take a while)..."
log_info "Expected storage engine: $STORAGE_ENGINE (set via MySQL config)"

START_TIME=$(date +%s)

sysbench oltp_common \
    --mysql-socket="$SOCKET" \
    --mysql-db="$BENCHMARK_DB" \
    --tables="$SYSBENCH_TABLES" \
    --table-size="$SYSBENCH_TABLE_SIZE" \
    prepare || {
    log_error "Sysbench prepare failed"
    exit 1
}

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

log_info "Sysbench data preparation completed in $DURATION seconds"

# Show database size
log_info "Database size information:"
mysql --socket="$SOCKET" "$BENCHMARK_DB" -e "
    SELECT
        table_schema AS 'Database',
        COUNT(*) AS 'Tables',
        ROUND(SUM(data_length + index_length) / 1024 / 1024 / 1024, 2) AS 'Size (GB)',
        ROUND(SUM(data_length) / 1024 / 1024 / 1024, 2) AS 'Data (GB)',
        ROUND(SUM(index_length) / 1024 / 1024 / 1024, 2) AS 'Index (GB)'
    FROM information_schema.tables
    WHERE table_schema = '$BENCHMARK_DB'
    GROUP BY table_schema;
"

log_info "Data preparation for $ENGINE completed successfully"
