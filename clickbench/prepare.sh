#!/bin/bash
# ClickBench Data Preparation Script
# Downloads data, creates table, and bulk loads

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

log_info "Preparing ClickBench data for $ENGINE"
log_info "Data directory: $CLICKBENCH_DATA_DIR"

# Create data directory
mkdir -p "$CLICKBENCH_DATA_DIR"

# Download data if not present
DATA_FILE="${CLICKBENCH_DATA_DIR}/hits.tsv"
COMPRESSED_FILE="${CLICKBENCH_DATA_DIR}/hits.tsv.gz"

if [ ! -f "$DATA_FILE" ]; then
    if [ ! -f "$COMPRESSED_FILE" ]; then
        log_info "Downloading ClickBench data (~20GB compressed)..."
        log_info "URL: $CLICKBENCH_DATA_URL"

        # Try wget first, fall back to curl
        if command -v wget &>/dev/null; then
            if ! wget -c -O "$COMPRESSED_FILE" "$CLICKBENCH_DATA_URL"; then
                log_error "Failed to download ClickBench data"
                rm -f "$COMPRESSED_FILE"
                exit 1
            fi
        elif command -v curl &>/dev/null; then
            if ! curl -L -C - -o "$COMPRESSED_FILE" "$CLICKBENCH_DATA_URL"; then
                log_error "Failed to download ClickBench data"
                rm -f "$COMPRESSED_FILE"
                exit 1
            fi
        else
            log_error "Neither wget nor curl found. Please install one of them."
            exit 1
        fi

        log_info "Download complete"
    fi

    log_info "Decompressing data (~75GB uncompressed)..."
    if ! gunzip -k "$COMPRESSED_FILE"; then
        log_error "Failed to decompress data"
        exit 1
    fi
    log_info "Decompression complete"
else
    log_info "Data file already exists: $DATA_FILE"
fi

# Create database
log_info "Creating database: $BENCHMARK_DB"
mysql --socket="$SOCKET" -e "DROP DATABASE IF EXISTS $BENCHMARK_DB;"
mysql --socket="$SOCKET" -e "CREATE DATABASE $BENCHMARK_DB;"

# Create table with appropriate storage engine
log_info "Creating hits table with $STORAGE_ENGINE engine..."
sed "s/_ENGINE_/$STORAGE_ENGINE/g" "${SCRIPT_DIR}/schema/hits_table.sql" > /tmp/hits_table_${ENGINE}.sql
mysql --socket="$SOCKET" "$BENCHMARK_DB" < /tmp/hits_table_${ENGINE}.sql

log_info "Table created successfully"

# Load data
log_info "Loading data into hits table (this will take a while)..."
log_info "Data file: $DATA_FILE"
log_info "File size: $(du -h "$DATA_FILE" | cut -f1)"

START_TIME=$(date +%s)

# Use LOAD DATA LOCAL INFILE for bulk loading
# Disable unique checks and foreign key checks for faster loading
mysql --socket="$SOCKET" --local-infile=1 "$BENCHMARK_DB" -e "
SET unique_checks = 0;
SET foreign_key_checks = 0;
SET sql_log_bin = 0;

LOAD DATA LOCAL INFILE '$DATA_FILE'
INTO TABLE hits
FIELDS TERMINATED BY '\t';

SET unique_checks = 1;
SET foreign_key_checks = 1;
SET sql_log_bin = 1;
" || {
    log_error "Failed to load data"
    exit 1
}

END_TIME=$(date +%s)
LOAD_DURATION=$((END_TIME - START_TIME))

log_info "Data loading completed in $LOAD_DURATION seconds ($((LOAD_DURATION / 60)) minutes)"

# Show row count and table size
log_info "Verifying data load..."
ROW_COUNT=$(mysql --socket="$SOCKET" -N -e "SELECT COUNT(*) FROM ${BENCHMARK_DB}.hits;")
log_info "Row count: $ROW_COUNT"

log_info "Database size information:"
SIZE_INFO=$(mysql --socket="$SOCKET" -N -e "
    SELECT
        ROUND(data_length / 1024 / 1024 / 1024, 4),
        ROUND(index_length / 1024 / 1024 / 1024, 4),
        ROUND((data_length + index_length) / 1024 / 1024 / 1024, 4)
    FROM information_schema.tables
    WHERE table_schema = '$BENCHMARK_DB' AND table_name = 'hits';
")
DATA_GB=$(echo "$SIZE_INFO" | awk '{print $1}')
INDEX_GB=$(echo "$SIZE_INFO" | awk '{print $2}')
TOTAL_GB=$(echo "$SIZE_INFO" | awk '{print $3}')

log_info "  Data: ${DATA_GB} GB, Index: ${INDEX_GB} GB, Total: ${TOTAL_GB} GB"

# Save load metrics to CSV for later comparison
LOAD_METRICS_FILE="${CLICKBENCH_DATA_DIR}/load_metrics_${ENGINE}.csv"
{
    echo "engine,load_time_seconds,row_count,data_gb,index_gb,total_gb"
    echo "${ENGINE},${LOAD_DURATION},${ROW_COUNT},${DATA_GB},${INDEX_GB},${TOTAL_GB}"
} > "$LOAD_METRICS_FILE"
log_info "Load metrics saved to: $LOAD_METRICS_FILE"

log_info "ClickBench data preparation for $ENGINE completed successfully"
