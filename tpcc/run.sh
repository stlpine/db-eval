#!/bin/bash
# TPC-C Benchmark Execution Script

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

TPCC_DIR="${SCRIPT_DIR}/tpcc-mysql"

if [ ! -d "$TPCC_DIR" ]; then
    log_error "tpcc-mysql not found. Please run prepare.sh first"
    exit 1
fi

# Create results directory
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_DIR="${RESULTS_DIR}/tpcc/${ENGINE}/${TIMESTAMP}"
mkdir -p "$RESULT_DIR"

log_info "Starting TPC-C benchmark for $ENGINE"
log_info "Results will be saved to: $RESULT_DIR"

# Log all configuration options
CONFIG_LOG="${RESULT_DIR}/benchmark_config.log"
log_info "Logging configuration to: $CONFIG_LOG"
{
    echo "============================================================"
    echo "BENCHMARK CONFIGURATION LOG"
    echo "Generated: $(date)"
    echo "Engine: $ENGINE"
    echo "============================================================"
    echo ""

    echo "============================================================"
    echo "SYSTEM INFORMATION"
    echo "============================================================"
    echo "Hostname: $(hostname)"
    echo "Kernel: $(uname -r)"
    echo "OS: $(cat /etc/os-release 2>/dev/null | grep PRETTY_NAME | cut -d= -f2 | tr -d '"')"
    echo ""
    echo "CPU Info:"
    lscpu 2>/dev/null | grep -E "^(Model name|Socket|Core|Thread|CPU\(s\)|CPU MHz)" || cat /proc/cpuinfo | grep -E "^(model name|cpu cores|siblings)" | head -4
    echo ""
    echo "Memory Info:"
    free -h 2>/dev/null || cat /proc/meminfo | head -3
    echo ""
    echo "Disk Info:"
    df -h "$SSD_MOUNT" 2>/dev/null
    echo ""

    echo "============================================================"
    echo "BENCHMARK PARAMETERS (env.sh)"
    echo "============================================================"
    echo "BENCHMARK_DB: $BENCHMARK_DB"
    echo "BENCHMARK_THREADS: $BENCHMARK_THREADS"
    echo "BENCHMARK_DURATION: $BENCHMARK_DURATION"
    echo "TPCC_WAREHOUSES: $TPCC_WAREHOUSES"
    echo "TPCC_DURATION: $TPCC_DURATION"
    echo ""

    echo "============================================================"
    echo "MYSQL SERVER VARIABLES"
    echo "============================================================"
    mysql --socket="$SOCKET" -e "SHOW VARIABLES;" 2>/dev/null
    echo ""

    echo "============================================================"
    echo "MYSQL GLOBAL STATUS (before benchmark)"
    echo "============================================================"
    mysql --socket="$SOCKET" -e "SHOW GLOBAL STATUS;" 2>/dev/null
    echo ""

    echo "============================================================"
    echo "STORAGE ENGINE STATUS"
    echo "============================================================"
    if [ "$ENGINE" = "percona-myrocks" ]; then
        mysql --socket="$SOCKET" -e "SHOW ENGINE ROCKSDB STATUS\G" 2>/dev/null
    else
        mysql --socket="$SOCKET" -e "SHOW ENGINE INNODB STATUS\G" 2>/dev/null
    fi
    echo ""

} > "$CONFIG_LOG" 2>&1

# Function to run a single benchmark
run_benchmark() {
    local threads=$1

    log_info "Running TPC-C with $threads threads"

    local result_file="${RESULT_DIR}/tpcc_threads${threads}.txt"
    local stats_file="${RESULT_DIR}/tpcc_threads${threads}_stats.csv"

    # Start monitoring CPU and I/O
    pidstat -u -r -d 1 > "${RESULT_DIR}/tpcc_threads${threads}_pidstat.txt" 2>&1 &
    local pidstat_pid=$!

    iostat -x 1 > "${RESULT_DIR}/tpcc_threads${threads}_iostat.txt" 2>&1 &
    local iostat_pid=$!

    # Run TPC-C
    {
        echo "==================================================================="
        echo "Benchmark: TPC-C"
        echo "Threads: $threads"
        echo "Warehouses: $TPCC_WAREHOUSES"
        echo "Duration: $TPCC_DURATION seconds"
        echo "Engine: $ENGINE"
        echo "Date: $(date)"
        echo "==================================================================="
        echo ""

        cd "$TPCC_DIR/src"

        # Set library path for MySQL libraries
        MYSQL_LIB_PATH=$(mysql_config --variable=pkglibdir 2>/dev/null)
        if [ -n "$MYSQL_LIB_PATH" ]; then
            export LD_LIBRARY_PATH="${MYSQL_LIB_PATH}:${LD_LIBRARY_PATH}"
        fi

        ../tpcc_start \
            -h localhost \
            -S "$SOCKET" \
            -d "$BENCHMARK_DB" \
            -u root \
            -p "" \
            -w "$TPCC_WAREHOUSES" \
            -c "$threads" \
            -r 10 \
            -l "$TPCC_DURATION" \
            2> "${RESULT_DIR}/tpcc_threads${threads}_errors.log"

    } > "$result_file"

    # Stop monitoring
    kill $pidstat_pid $iostat_pid 2>/dev/null

    # Extract key metrics
    # TPC-C output format:
    #   <TpmC>
    #                    50031.699 TpmC
    {
        echo "engine,threads,warehouses,duration,tpmC,tpmTotal"
        awk -v engine="$ENGINE" -v threads="$threads" -v warehouses="$TPCC_WAREHOUSES" -v duration="$TPCC_DURATION" '
        /TpmC$/ { tpmC = $1 }
        END {
            if (tpmC == "") tpmC = 0
            printf "%s,%s,%s,%s,%.2f,%.2f\n", engine, threads, warehouses, duration, tpmC, tpmC
        }
        ' "$result_file"
    } >> "$stats_file"

    log_info "Completed TPC-C with $threads threads"
}

# Run benchmarks for different thread counts
for threads in $BENCHMARK_THREADS; do
    # Wait for SSD to cool down before each test
    wait_for_ssd_cooldown || log_info "Skipping cooldown (temperature check unavailable)"

    run_benchmark "$threads"
done

# Consolidate results
log_info "Consolidating results..."
CONSOLIDATED_CSV="${RESULT_DIR}/consolidated_results.csv"
echo "engine,threads,warehouses,duration,tpmC,tpmTotal" > "$CONSOLIDATED_CSV"

for stats_file in "${RESULT_DIR}"/tpcc_threads*_stats.csv; do
    if [ -f "$stats_file" ]; then
        tail -n +2 "$stats_file" >> "$CONSOLIDATED_CSV"
    fi
done

log_info "TPC-C benchmark completed successfully!"
log_info "Results saved to: $RESULT_DIR"
log_info "Consolidated results: $CONSOLIDATED_CSV"
