#!/bin/bash
# TPC-H OLAP Data Preparation Script
# Builds dbgen, generates data, creates tables, and loads data

set -e
set -o pipefail

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

TPCH_KIT_DIR="${SCRIPT_DIR}/tpch-kit"
DBGEN_DIR="${TPCH_KIT_DIR}/dbgen"

log_info "Preparing TPC-H data for $ENGINE"
log_info "Scale Factor: $TPCH_SCALE_FACTOR (SF${TPCH_SCALE_FACTOR} = ~${TPCH_SCALE_FACTOR}GB)"
log_info "Data directory: $TPCH_DATA_DIR"

# Initialize tpch-kit submodule if needed
PARENT_DIR="$(dirname "$SCRIPT_DIR")"
if [ ! -f "$DBGEN_DIR/Makefile" ]; then
    log_info "Initializing tpch-kit submodule..."
    cd "$PARENT_DIR"
    git submodule update --init --recursive tpch-olap/tpch-kit
    cd - > /dev/null
else
    log_info "tpch-kit submodule already exists"
fi

# Build dbgen
log_info "Building dbgen..."
cd "$DBGEN_DIR"

# Patch the Makefile for our configuration
# Note: Using POSTGRESQL for DATABASE as it generates MySQL-compatible date formats
# The .tbl data files are database-agnostic (pipe-delimited text)
# MySQL-specific queries are in tpch-olap/queries/mysql/
if grep -q "^DATABASE = POSTGRESQL" Makefile; then
    log_info "Makefile already configured"
else
    log_info "Configuring Makefile for Linux/PostgreSQL (MySQL-compatible)..."
    sed -i 's/^DATABASE\s*=.*/DATABASE = POSTGRESQL/' Makefile
    sed -i 's/^MACHINE\s*=.*/MACHINE = LINUX/' Makefile
fi

make clean
if ! make; then
    log_error "Failed to build dbgen"
    exit 1
fi

if [ ! -f "dbgen" ]; then
    log_error "dbgen binary not found after build"
    exit 1
fi

log_info "dbgen built successfully"

# Create data directory
mkdir -p "$TPCH_DATA_DIR"

# Generate data
log_info "Generating TPC-H data (SF${TPCH_SCALE_FACTOR})..."
log_info "This may take a while for large scale factors..."

cd "$TPCH_DATA_DIR"

START_TIME=$(date +%s)

# Generate data files
# -s: scale factor
# -f: force overwrite
# -v: verbose
if ! "$DBGEN_DIR/dbgen" -s "$TPCH_SCALE_FACTOR" -f -v; then
    log_error "Failed to generate TPC-H data"
    exit 1
fi

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
log_info "Data generation completed in $DURATION seconds ($((DURATION / 60)) minutes)"

# List generated files
log_info "Generated data files:"
ls -lh "$TPCH_DATA_DIR"/*.tbl 2>/dev/null || log_error "No .tbl files found"

# Create database
log_info "Creating database: $BENCHMARK_DB"
mysql --socket="$SOCKET" -e "DROP DATABASE IF EXISTS $BENCHMARK_DB;"
mysql --socket="$SOCKET" -e "CREATE DATABASE $BENCHMARK_DB;"

# Create tables with appropriate storage engine
log_info "Creating TPC-H tables with $STORAGE_ENGINE engine..."
sed "s/_ENGINE_/$STORAGE_ENGINE/g" "${SCRIPT_DIR}/schema/create_tables.sql" > /tmp/tpch_tables_${ENGINE}.sql
mysql --socket="$SOCKET" "$BENCHMARK_DB" < /tmp/tpch_tables_${ENGINE}.sql

log_info "Tables created successfully"

# Load data
log_info "Loading TPC-H data into tables..."

START_TIME=$(date +%s)

# Load each table
# Order matters for foreign key constraints (if any)
TABLES="region nation supplier part partsupp customer orders lineitem"

for table in $TABLES; do
    tbl_file="${TPCH_DATA_DIR}/${table}.tbl"
    if [ ! -f "$tbl_file" ]; then
        log_error "Data file not found: $tbl_file"
        exit 1
    fi

    file_size=$(du -h "$tbl_file" | cut -f1)
    log_info "Loading $table ($file_size)..."

    # TPC-H .tbl files use | as delimiter and have trailing |
    # Use LOAD DATA LOCAL INFILE for bulk loading
    mysql --socket="$SOCKET" --local-infile=1 "$BENCHMARK_DB" -e "
SET unique_checks = 0;
SET foreign_key_checks = 0;
SET sql_log_bin = 0;

LOAD DATA LOCAL INFILE '$tbl_file'
INTO TABLE $table
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '|\n';

SET unique_checks = 1;
SET foreign_key_checks = 1;
SET sql_log_bin = 1;
" || {
        log_error "Failed to load $table"
        exit 1
    }
done

END_TIME=$(date +%s)
LOAD_DURATION=$((END_TIME - START_TIME))

log_info "Data loading completed in $LOAD_DURATION seconds ($((LOAD_DURATION / 60)) minutes)"

# Verify row counts
log_info "Verifying data load..."
TOTAL_ROWS=0
for table in $TABLES; do
    count=$(mysql --socket="$SOCKET" -N -e "SELECT COUNT(*) FROM ${BENCHMARK_DB}.${table};")
    log_info "  $table: $count rows"
    TOTAL_ROWS=$((TOTAL_ROWS + count))
done
log_info "Total rows: $TOTAL_ROWS"

# Show database size
log_info "Database size information:"
SIZE_INFO=$(mysql --socket="$SOCKET" -N -e "
    SELECT
        ROUND(SUM(data_length) / 1024 / 1024 / 1024, 4),
        ROUND(SUM(index_length) / 1024 / 1024 / 1024, 4),
        ROUND((SUM(data_length) + SUM(index_length)) / 1024 / 1024 / 1024, 4)
    FROM information_schema.tables
    WHERE table_schema = '$BENCHMARK_DB';
")
DATA_GB=$(echo "$SIZE_INFO" | awk '{print $1}')
INDEX_GB=$(echo "$SIZE_INFO" | awk '{print $2}')
TOTAL_GB=$(echo "$SIZE_INFO" | awk '{print $3}')

log_info "  Data: ${DATA_GB} GB, Index: ${INDEX_GB} GB, Total: ${TOTAL_GB} GB"

# Save load metrics to CSV for later comparison
LOAD_METRICS_FILE="${TPCH_DATA_DIR}/load_metrics_${ENGINE}.csv"
{
    echo "engine,scale_factor,load_time_seconds,row_count,data_gb,index_gb,total_gb"
    echo "${ENGINE},${TPCH_SCALE_FACTOR},${LOAD_DURATION},${TOTAL_ROWS},${DATA_GB},${INDEX_GB},${TOTAL_GB}"
} > "$LOAD_METRICS_FILE"
log_info "Load metrics saved to: $LOAD_METRICS_FILE"

log_info "TPC-H data preparation for $ENGINE completed successfully"
