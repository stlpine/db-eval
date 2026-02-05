#!/bin/bash
# ClickBench Cleanup Script

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

log_info "Cleaning up ClickBench data for $ENGINE"

# Drop database
log_info "Dropping database: $BENCHMARK_DB"
mysql --socket="$SOCKET" -e "DROP DATABASE IF EXISTS $BENCHMARK_DB;"

# Optionally clean up data files
if [ -d "$CLICKBENCH_DATA_DIR" ]; then
    log_info "Note: Raw data files still exist at $CLICKBENCH_DATA_DIR"
    log_info "To remove them: rm -rf $CLICKBENCH_DATA_DIR"
fi

log_info "Cleanup completed for $ENGINE"
