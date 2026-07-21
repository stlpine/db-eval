#!/bin/bash
# Sysbench HTAP Data Preparation Script
#
# Creates 12 tables x 100k rows (AIDE VLDB'23 §6.4 configuration) and drops
# the secondary k index so join4.sql forces non-indexed full scans.
#
# WARNING: Creates sbtest1..sbtest12 — same names as standard sysbench tables.
#   Cannot co-exist with standard sysbench data. prepare-data.sh BENCH_COUNT
#   guard prevents simultaneous loading.
#
# Usage: ./sysbench-htap/prepare.sh <engine>
#   engine: percona-innodb | percona-myrocks | percona-myrocks-csd

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"

usage() {
    echo "Usage: $0 <engine>"
    echo "Engines: percona-innodb, percona-myrocks, percona-myrocks-csd"
    exit 1
}

if [ $# -ne 1 ]; then
    usage
fi

ENGINE=$1

case $ENGINE in
    percona-innodb)
        SOCKET="${MYSQL_SOCKET_PERCONA_INNODB}"
        STORAGE_ENGINE="innodb"
        ;;
    percona-myrocks)
        SOCKET="${MYSQL_SOCKET_PERCONA_MYROCKS}"
        STORAGE_ENGINE="rocksdb"
        ;;
    percona-myrocks-csd)
        SOCKET="${MYSQL_SOCKET_PERCONA_MYROCKS_CSD}"
        STORAGE_ENGINE="rocksdb"
        ;;
    percona-myrocks-nvmevirt)
        SOCKET="${MYSQL_SOCKET_PERCONA_MYROCKS_NVMEVIRT}"
        STORAGE_ENGINE="rocksdb"
        ;;
    *)
        log_error "Unknown engine: $ENGINE"
        usage
        ;;
esac

if ! mysqladmin --socket="$SOCKET" ping &>/dev/null; then
    log_error "MySQL is not running. Start it first: ./scripts/mysql-control.sh $ENGINE start"
    exit 1
fi

log_info "Preparing sysbench-htap data for $ENGINE"
log_info "Configuration: ${HTAP_TABLES} tables, ${HTAP_TABLE_SIZE} rows each"
log_info "NOTE: sbtest1..sbtest${HTAP_TABLES} share names with standard sysbench tables"

# Create database
log_info "Creating database: $BENCHMARK_DB"
mysql --socket="$SOCKET" -e "DROP DATABASE IF EXISTS $BENCHMARK_DB;"
mysql --socket="$SOCKET" -e "CREATE DATABASE $BENCHMARK_DB;"

# Create sysbench user
log_info "Creating sysbench user..."
mysql --socket="$SOCKET" -e "CREATE USER IF NOT EXISTS 'sbtest'@'localhost' IDENTIFIED BY '';"
mysql --socket="$SOCKET" -e "GRANT ALL PRIVILEGES ON $BENCHMARK_DB.* TO 'sbtest'@'localhost';"
mysql --socket="$SOCKET" -e "FLUSH PRIVILEGES;"

# Run sysbench prepare
log_info "Running sysbench prepare (${HTAP_TABLES} tables x ${HTAP_TABLE_SIZE} rows)..."
START_TIME=$(date +%s)

sysbench oltp_read_write \
    --mysql-socket="$SOCKET" \
    --mysql-db="$BENCHMARK_DB" \
    --mysql-storage-engine="$STORAGE_ENGINE" \
    --tables="$HTAP_TABLES" \
    --table-size="$HTAP_TABLE_SIZE" \
    prepare || {
    log_error "Sysbench prepare failed"
    exit 1
}

# Drop the k index on all tables so join4.sql forces non-indexed full scans.
# This matches the AIDE paper's MyRocks configuration (non-indexed join columns).
# Index name: k_N where N is the table number (sysbench naming convention).
log_info "Dropping k index on all ${HTAP_TABLES} tables (non-indexed join columns per AIDE paper)..."
for N in $(seq 1 "$HTAP_TABLES"); do
    mysql --socket="$SOCKET" "$BENCHMARK_DB" \
        -e "ALTER TABLE sbtest${N} DROP INDEX k_${N};" 2>/dev/null || \
        log_error "  WARNING: Could not drop index k_${N} on sbtest${N} (may not exist)"
done
log_info "k indexes dropped"

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
log_info "Sysbench-htap preparation completed in $DURATION seconds"

# Show database size
log_info "Database size:"
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

# For CSD sim / FLAX NVMeVirt: move OLAP tables (sbtest1–4, the join query targets)
# to the device's offload-covered column family. sbtest5–12 stay in 'default' CF
# and are used only by the OLTP background load — the offload filter does not
# need to cover them.
#
# MyRocks CF assignment is controlled by the KEY-level COMMENT, not the table-level
# COMMENT. The correct approach is to drop and recreate the PRIMARY KEY with
# COMMENT 'cfname=...'. ALTER TABLE ... ENGINE=ROCKSDB COMMENT='cfname=...'
# sets the table comment only (ignored by MyRocks for CF assignment) and does NOT
# move the table to a different CF.
CF_NAME=""
[ "$ENGINE" = "percona-myrocks-csd" ] && CF_NAME="csd_olap"
[ "$ENGINE" = "percona-myrocks-nvmevirt" ] && CF_NAME="nvmevirt_olap"
if [ -n "$CF_NAME" ]; then
    log_info "Migrating OLAP tables (sbtest1–4) to ${CF_NAME} CF for offload coverage..."
    for N in 1 2 3 4; do
        log_info "  Rebuilding PRIMARY KEY for sbtest${N} with cfname=${CF_NAME} ..."
        mysql --socket="$SOCKET" "$BENCHMARK_DB" \
            -e "ALTER TABLE sbtest${N} DROP PRIMARY KEY,
                    ADD PRIMARY KEY (id) COMMENT 'cfname=${CF_NAME}';" || \
            log_error "  WARNING: CF migration failed for sbtest${N} (non-fatal)"
    done
    log_info "CF migration complete — sbtest1–4 are now in ${CF_NAME} CF"
    # Verify via RocksDB DDL
    mysql --socket="$SOCKET" \
        -e "SELECT TABLE_NAME, INDEX_NAME, CF
            FROM information_schema.ROCKSDB_DDL
            WHERE TABLE_SCHEMA='${BENCHMARK_DB}' AND TABLE_NAME IN ('sbtest1','sbtest2','sbtest3','sbtest4');" \
        2>/dev/null | head -20 || true
fi

log_info "Data preparation for $ENGINE completed successfully"
