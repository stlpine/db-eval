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

# Setup tpcc-mysql repository (only if not already present)
PARENT_DIR="$(dirname "$SCRIPT_DIR")"
if [ ! -f "$TPCC_DIR/src/load.c" ]; then
    log_info "Initializing tpcc-mysql submodule..."
    cd "$PARENT_DIR"
    git submodule update --init --recursive tpcc/tpcc-mysql
    cd - > /dev/null
else
    log_info "tpcc-mysql submodule already exists, skipping update"
fi

# Build tpcc-mysql
log_info "Building tpcc-mysql..."
cd "$TPCC_DIR/src"

# Remove old binaries to ensure fresh build check
rm -f ../tpcc_load ../tpcc_start

make clean
if ! make; then
    log_error "Failed to build tpcc-mysql"
    log_error "Make sure MySQL development libraries are installed:"
    log_error "  Ubuntu/Debian: sudo apt install libmysqlclient-dev"
    log_error "  RHEL/CentOS: sudo yum install mysql-devel"
    exit 1
fi

if [ ! -f "../tpcc_load" ] || [ ! -f "../tpcc_start" ]; then
    log_error "Failed to build tpcc-mysql - binaries not found"
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

START_TIME=$(date +%s)

# Set library path for MySQL libraries
MYSQL_LIB_PATH=$(mysql_config --variable=pkglibdir 2>/dev/null)
if [ -n "$MYSQL_LIB_PATH" ]; then
    export LD_LIBRARY_PATH="${MYSQL_LIB_PATH}:${LD_LIBRARY_PATH}"
fi

export MYSQL_UNIX_PORT="$SOCKET"

# Create log directory
LOG_DIR="${SCRIPT_DIR}/load_logs_${ENGINE}"
rm -rf "$LOG_DIR"
mkdir -p "$LOG_DIR"

# Parallel loading configuration
# -l 1: ITEM table (only needs to run once, not per-warehouse)
# -l 2: WAREHOUSE, STOCK, DISTRICT
# -l 3: CUSTOMER, HISTORY
# -l 4: ORDERS, NEW_ORDER, ORDER_LINE
STEP=500  # Warehouses per chunk (4 chunks × 3 table groups = 12 processes + 1 for ITEM = 13 total)

log_info "Loading ITEM table (table group 1)..."
"$TPCC_DIR/tpcc_load" \
    -h localhost \
    -d "$BENCHMARK_DB" \
    -u root \
    -p "" \
    -w "$TPCC_WAREHOUSES" \
    -l 1 \
    -m 1 \
    -n "$TPCC_WAREHOUSES" \
    >> "$LOG_DIR/load_1.log" 2>&1 &
PIDS=($!)

log_info "Loading warehouse data in parallel (table groups 2, 3, 4)..."
log_info "  Chunk size: $STEP warehouses"
log_info "  Total warehouses: $TPCC_WAREHOUSES"

# Spawn parallel processes for each warehouse range and table group
x=1
while [ $x -le "$TPCC_WAREHOUSES" ]; do
    END_WH=$((x + STEP - 1))
    if [ $END_WH -gt "$TPCC_WAREHOUSES" ]; then
        END_WH=$TPCC_WAREHOUSES
    fi

    log_info "  Spawning loaders for warehouses $x to $END_WH..."

    # Table group 2: WAREHOUSE, STOCK, DISTRICT
    "$TPCC_DIR/tpcc_load" \
        -h localhost \
        -d "$BENCHMARK_DB" \
        -u root \
        -p "" \
        -w "$TPCC_WAREHOUSES" \
        -l 2 \
        -m $x \
        -n $END_WH \
        >> "$LOG_DIR/load_2_${x}.log" 2>&1 &
    PIDS+=($!)

    # Table group 3: CUSTOMER, HISTORY
    "$TPCC_DIR/tpcc_load" \
        -h localhost \
        -d "$BENCHMARK_DB" \
        -u root \
        -p "" \
        -w "$TPCC_WAREHOUSES" \
        -l 3 \
        -m $x \
        -n $END_WH \
        >> "$LOG_DIR/load_3_${x}.log" 2>&1 &
    PIDS+=($!)

    # Table group 4: ORDERS, NEW_ORDER, ORDER_LINE
    "$TPCC_DIR/tpcc_load" \
        -h localhost \
        -d "$BENCHMARK_DB" \
        -u root \
        -p "" \
        -w "$TPCC_WAREHOUSES" \
        -l 4 \
        -m $x \
        -n $END_WH \
        >> "$LOG_DIR/load_4_${x}.log" 2>&1 &
    PIDS+=($!)

    x=$((x + STEP))
done

log_info "Spawned ${#PIDS[@]} parallel loader processes"
log_info "Waiting for all loaders to complete..."

# Wait for all processes and track failures
FAILED=0
for pid in "${PIDS[@]}"; do
    if ! wait "$pid"; then
        FAILED=$((FAILED + 1))
    fi
done

if [ $FAILED -gt 0 ]; then
    log_error "$FAILED loader processes failed. Check logs in $LOG_DIR"
    exit 1
fi

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

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
