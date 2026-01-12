#!/bin/bash
# Sysbench-TPCC Data Preparation Script

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
        ;;
    percona-innodb)
        SOCKET="${MYSQL_SOCKET_PERCONA_INNODB}"
        ;;
    percona-myrocks)
        SOCKET="${MYSQL_SOCKET_PERCONA_MYROCKS}"
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

SYSBENCH_TPCC_DIR="${SCRIPT_DIR}/sysbench-tpcc"

# Setup sysbench-tpcc repository
PARENT_DIR="$(dirname "$SCRIPT_DIR")"
if [ ! -d "$SYSBENCH_TPCC_DIR" ]; then
    log_info "Initializing sysbench-tpcc submodule..."
    cd "$PARENT_DIR"
    git submodule update --init --recursive sysbench-tpcc/sysbench-tpcc
    cd - > /dev/null
else
    log_info "Updating sysbench-tpcc submodule..."
    cd "$PARENT_DIR"
    git submodule update --remote sysbench-tpcc/sysbench-tpcc
    cd - > /dev/null
fi

# Check if tpcc.lua exists
if [ ! -f "$SYSBENCH_TPCC_DIR/tpcc.lua" ]; then
    log_error "tpcc.lua not found in $SYSBENCH_TPCC_DIR"
    exit 1
fi

log_info "sysbench-tpcc setup complete"

# Check sysbench version
SYSBENCH_VERSION=$(sysbench --version 2>&1 | head -1)
log_info "Using $SYSBENCH_VERSION"

# Create database
log_info "Creating database: $BENCHMARK_DB"
mysql --socket="$SOCKET" -e "DROP DATABASE IF EXISTS $BENCHMARK_DB;"
mysql --socket="$SOCKET" -e "CREATE DATABASE $BENCHMARK_DB;"

# Create sysbench user if it doesn't exist
log_info "Creating sysbench user..."
mysql --socket="$SOCKET" -e "CREATE USER IF NOT EXISTS 'sbtest'@'localhost' IDENTIFIED BY '';"
mysql --socket="$SOCKET" -e "GRANT ALL PRIVILEGES ON $BENCHMARK_DB.* TO 'sbtest'@'localhost';"
mysql --socket="$SOCKET" -e "FLUSH PRIVILEGES;"

# Prepare TPC-C data
log_info "Preparing TPC-C data..."
log_info "Configuration:"
log_info "  Tables: $SYSBENCH_TPCC_TABLES"
log_info "  Scale (warehouses per table): $SYSBENCH_TPCC_SCALE"
log_info "  Total warehouses: $(( SYSBENCH_TPCC_TABLES * SYSBENCH_TPCC_SCALE ))"

log_info "This may take a while depending on the data size..."

sysbench "$SYSBENCH_TPCC_DIR/tpcc.lua" \
    --mysql-socket="$SOCKET" \
    --mysql-db="$BENCHMARK_DB" \
    --tables="$SYSBENCH_TPCC_TABLES" \
    --scale="$SYSBENCH_TPCC_SCALE" \
    --threads="$SYSBENCH_TPCC_TABLES" \
    --db-driver=mysql \
    prepare

if [ $? -ne 0 ]; then
    log_error "Failed to prepare TPC-C data"
    exit 1
fi

log_info "TPC-C data preparation completed successfully"

# Show database size
log_info "Database size:"
mysql --socket="$SOCKET" -e "
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

log_info "Data preparation complete. You can now run the benchmark using:"
log_info "  ./sysbench-tpcc/run.sh $ENGINE"
