#!/bin/bash
# TPC-C Benchmark Execution Script
# Runs a single thread count; called by run-benchmark.sh for each thread count.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"
source "${SCRIPT_DIR}/../scripts/monitor.sh"

usage() {
    echo "Usage: $0 <engine> <threads> <result_dir>"
    echo "Engines: vanilla-innodb, percona-innodb, percona-myrocks"
    exit 1
}

if [ $# -ne 3 ]; then
    usage
fi

ENGINE=$1
THREADS=$2
RESULT_DIR=$3

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

log_info "Running TPC-C: engine=$ENGINE, threads=$THREADS, result_dir=$RESULT_DIR"

trap cleanup_monitors EXIT

# Function to run a single benchmark
run_benchmark() {
    local threads=$1

    log_info "Running TPC-C with $threads threads"

    local result_file="${RESULT_DIR}/tpcc_threads${threads}.txt"
    local stats_file="${RESULT_DIR}/tpcc_threads${threads}_stats.csv"

    # Start monitoring (pidstat, iostat, mpstat, vmstat)
    start_monitors "$RESULT_DIR" "tpcc_threads${threads}"

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

    # Stop monitoring and generate resource utilization summary
    stop_monitors
    generate_resource_summary "$RESULT_DIR" "tpcc_threads${threads}"

    # Extract key metrics
    # TPC-C output format:
    #   <TpmC>
    #                    50031.699 TpmC
    # Latency from periodic output: "  10, trx: 7140, 95%: 14.752, 99%: 18.311, ..."
    # Latency from Raw Results: "[0] sc:63200 lt:318400  rt:0  fl:0 avg_rt: 10.8 (5)"
    {
        echo "engine,threads,warehouses,duration,tpmC,tpmTotal,latency_avg,latency_95"
        awk -v engine="$ENGINE" -v threads="$threads" -v warehouses="$TPCC_WAREHOUSES" -v duration="$TPCC_DURATION" '
        /TpmC$/ { tpmC = $1 }
        # Extract 95th percentile from periodic output lines (format: "  10, trx: 7140, 95%: 14.752, ...")
        /^[[:space:]]*[0-9]+, trx:/ {
            # Find 95%: value
            for (i = 1; i <= NF; i++) {
                if ($i ~ /^95%:/) {
                    val = $(i+1)
                    gsub(/,/, "", val)
                    sum_95 += val
                    count_95++
                }
            }
        }
        # Extract avg_rt for New-Order [0] from Raw Results
        /^[[:space:]]*\[0\] sc:.*avg_rt:/ {
            for (i = 1; i <= NF; i++) {
                if ($i == "avg_rt:") {
                    avg_rt = $(i+1)
                    gsub(/,/, "", avg_rt)
                }
            }
        }
        END {
            if (tpmC == "") tpmC = 0
            if (avg_rt == "") avg_rt = 0
            if (count_95 > 0) {
                lat_95 = sum_95 / count_95
            } else {
                lat_95 = 0
            }
            printf "%s,%s,%s,%s,%.2f,%.2f,%.2f,%.2f\n", engine, threads, warehouses, duration, tpmC, tpmC, avg_rt, lat_95
        }
        ' "$result_file"
    } >> "$stats_file"

    log_info "Completed TPC-C with $threads threads"
}

wait_for_ssd_cooldown || log_info "Skipping cooldown (temperature check unavailable)"
run_benchmark "$THREADS"
