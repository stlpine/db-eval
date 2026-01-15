#!/bin/bash
# TPC-C Data Preparation Script

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
        STORAGE_ENGINE="InnoDB"
        ;;
    percona-innodb)
        SOCKET="${MYSQL_SOCKET_PERCONA_INNODB}"
        STORAGE_ENGINE="InnoDB"
        ;;
    percona-myrocks)
        SOCKET="${MYSQL_SOCKET_PERCONA_MYROCKS}"
        STORAGE_ENGINE="ROCKSDB"
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

TPCC_DIR="${SCRIPT_DIR}/tpcc-mysql"

# Setup tpcc-mysql repository
PARENT_DIR="$(dirname "$SCRIPT_DIR")"
if [ ! -d "$TPCC_DIR" ]; then
    log_info "Initializing tpcc-mysql submodule..."
    cd "$PARENT_DIR"
    git submodule update --init --recursive tpcc/tpcc-mysql
    cd - > /dev/null
else
    log_info "Updating tpcc-mysql submodule..."
    cd "$PARENT_DIR"
    git submodule update --remote tpcc/tpcc-mysql
    cd - > /dev/null
fi

# Build tpcc-mysql
log_info "Building tpcc-mysql..."
cd "$TPCC_DIR/src"
make clean
make

if [ ! -f "../tpcc_load" ] || [ ! -f "../tpcc_start" ]; then
    log_error "Failed to build tpcc-mysql"
    exit 1
fi

log_info "tpcc-mysql built successfully"

# Create database
log_info "Creating TPC-C database: $BENCHMARK_DB"
mysql --socket="$SOCKET" -e "DROP DATABASE IF EXISTS $BENCHMARK_DB;"
mysql --socket="$SOCKET" -e "CREATE DATABASE $BENCHMARK_DB;"

# Create tables
log_info "Creating TPC-C tables for $STORAGE_ENGINE..."
cd "$TPCC_DIR"

# Modify create_table.sql to use specified storage engine
sed "s/ENGINE=InnoDB/ENGINE=$STORAGE_ENGINE/g" create_table.sql > /tmp/create_table_${ENGINE}.sql
sed -i "s/ENGINE=INNODB/ENGINE=$STORAGE_ENGINE/g" /tmp/create_table_${ENGINE}.sql

mysql --socket="$SOCKET" "$BENCHMARK_DB" < /tmp/create_table_${ENGINE}.sql
mysql --socket="$SOCKET" "$BENCHMARK_DB" < "${SCRIPT_DIR}/add_idx_only.sql"

log_info "TPC-C tables created successfully"

# Load data in parallel
log_info "Loading TPC-C data with $TPCC_WAREHOUSES warehouses (parallel loading)..."
log_info "This may take a while depending on warehouse count..."

START_TIME=$(date +%s)

cd "$TPCC_DIR"

# Set library path for MySQL libraries
MYSQL_LIB_PATH=$(mysql_config --variable=pkglibdir 2>/dev/null)
if [ -n "$MYSQL_LIB_PATH" ]; then
    export LD_LIBRARY_PATH="${MYSQL_LIB_PATH}:${LD_LIBRARY_PATH}"
fi

# Create log directory for parallel load outputs
LOAD_LOG_DIR="${SCRIPT_DIR}/load_logs_${ENGINE}"
rm -rf "$LOAD_LOG_DIR"
mkdir -p "$LOAD_LOG_DIR"

# Parallel loading configuration
STEP=100  # Number of warehouses per batch

# Load item table first (only needs to run once for all warehouses)
log_info "Loading item table..."
MYSQL_UNIX_PORT="$SOCKET" ./tpcc_load \
    -h localhost \
    -d "$BENCHMARK_DB" \
    -u root \
    -p "" \
    -w "$TPCC_WAREHOUSES" \
    -l 1 -m 1 -n "$TPCC_WAREHOUSES" \
    > "$LOAD_LOG_DIR/load_item.log" 2>&1 &

# Load warehouse data in parallel batches
# -l 2: warehouse, district, customer
# -l 3: orders
# -l 4: stock
log_info "Loading warehouse data in parallel (batch size: $STEP)..."

x=1
while [ $x -le $TPCC_WAREHOUSES ]; do
    END_WH=$(( x + STEP - 1 ))
    if [ $END_WH -gt $TPCC_WAREHOUSES ]; then
        END_WH=$TPCC_WAREHOUSES
    fi

    log_info "  Starting loaders for warehouses $x to $END_WH..."

    # Load warehouse/district/customer
    MYSQL_UNIX_PORT="$SOCKET" ./tpcc_load \
        -h localhost \
        -d "$BENCHMARK_DB" \
        -u root \
        -p "" \
        -w "$TPCC_WAREHOUSES" \
        -l 2 -m $x -n $END_WH \
        > "$LOAD_LOG_DIR/load_wdc_${x}.log" 2>&1 &

    # Load orders
    MYSQL_UNIX_PORT="$SOCKET" ./tpcc_load \
        -h localhost \
        -d "$BENCHMARK_DB" \
        -u root \
        -p "" \
        -w "$TPCC_WAREHOUSES" \
        -l 3 -m $x -n $END_WH \
        > "$LOAD_LOG_DIR/load_orders_${x}.log" 2>&1 &

    # Load stock
    MYSQL_UNIX_PORT="$SOCKET" ./tpcc_load \
        -h localhost \
        -d "$BENCHMARK_DB" \
        -u root \
        -p "" \
        -w "$TPCC_WAREHOUSES" \
        -l 4 -m $x -n $END_WH \
        > "$LOAD_LOG_DIR/load_stock_${x}.log" 2>&1 &

    x=$(( x + STEP ))
done

# Wait for all background loading processes to complete
log_info "Waiting for all parallel loaders to complete..."
wait

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

# Check for errors in load logs
ERRORS=$(grep -l -i "error" "$LOAD_LOG_DIR"/*.log 2>/dev/null | wc -l)
if [ "$ERRORS" -gt 0 ]; then
    log_error "Errors detected in loading. Check logs in $LOAD_LOG_DIR"
fi

log_info "TPC-C data loading completed in $DURATION seconds ($((DURATION / 60)) minutes)"

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

log_info "TPC-C data preparation for $ENGINE completed successfully"
