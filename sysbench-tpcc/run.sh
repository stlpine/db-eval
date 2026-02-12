#!/bin/bash
# Sysbench-TPCC Benchmark Execution Script
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

# Check if sysbench-tpcc exists
SYSBENCH_TPCC_DIR="${SCRIPT_DIR}/sysbench-tpcc"
if [ ! -f "$SYSBENCH_TPCC_DIR/tpcc.lua" ]; then
    log_error "sysbench-tpcc not found. Please run prepare.sh first."
    exit 1
fi

log_info "Running sysbench-tpcc: engine=$ENGINE, threads=$THREADS, result_dir=$RESULT_DIR"

trap cleanup_monitors EXIT

# Function to run a single benchmark
run_benchmark() {
    local threads=$1
    local result_file="${RESULT_DIR}/tpcc_threads${threads}.txt"
    local stats_file="${RESULT_DIR}/tpcc_threads${threads}_stats.csv"

    log_info "Running sysbench-tpcc with $threads threads..."

    # Start monitoring (pidstat, iostat, mpstat, vmstat)
    start_monitors "$RESULT_DIR" "tpcc_threads${threads}"

    # Run benchmark (cd to sysbench-tpcc dir so Lua can find tpcc_common.lua)
    (cd "$SYSBENCH_TPCC_DIR" && sysbench ./tpcc.lua \
        --mysql-socket="$SOCKET" \
        --mysql-db="$BENCHMARK_DB" \
        --threads="$threads" \
        --tables="$SYSBENCH_TPCC_TABLES" \
        --scale="$SYSBENCH_TPCC_SCALE" \
        --time="$SYSBENCH_TPCC_DURATION" \
        --report-interval="$SYSBENCH_TPCC_REPORT_INTERVAL" \
        --trx_level=RC \
        --db-driver=mysql \
        run) > "$result_file" 2>&1

    local bench_exit_code=$?

    # Stop monitoring and generate resource utilization summary
    stop_monitors
    generate_resource_summary "$RESULT_DIR" "tpcc_threads${threads}"

    if [ $bench_exit_code -ne 0 ]; then
        log_error "Benchmark failed for $threads threads"
        log_error "Check $result_file for details"
        return 1
    fi

    # Extract metrics from result file
    log_info "Extracting metrics..."

    # Parse sysbench output
    # Format:
    #   transactions:                        928191 (1546.92 per sec.)
    #   queries:                             26404260 (44005.32 per sec.)
    #   avg:                                   10.34
    #   95th percentile:                       36.24
    {
        echo "engine,threads,tables,scale,warehouses,duration,tps,qps,latency_avg,latency_95p,latency_99p,tpmC,tpmTotal"
        awk -v engine="$ENGINE" -v threads="$threads" -v tables="$SYSBENCH_TPCC_TABLES" \
            -v scale="$SYSBENCH_TPCC_SCALE" -v duration="$SYSBENCH_TPCC_DURATION" '
        /transactions:.*per sec/ {
            # "transactions:  928191 (1546.92 per sec.)"
            gsub(/.*\(/, ""); gsub(/ per sec.*/, "")
            tps = $0 + 0
        }
        /queries:.*per sec/ {
            gsub(/.*\(/, ""); gsub(/ per sec.*/, "")
            qps = $0 + 0
        }
        /^[[:space:]]+avg:/ {
            lat_avg = $2 + 0
        }
        /95th percentile:/ {
            lat_95 = $3 + 0
        }
        /99th percentile:/ {
            lat_99 = $3 + 0
        }
        END {
            warehouses = tables * scale
            tpmC = tps * 60
            tpmTotal = tpmC
            printf "%s,%s,%s,%s,%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f\n", \
                engine, threads, tables, scale, warehouses, duration, \
                tps, qps, lat_avg, lat_95, lat_99, tpmC, tpmTotal
        }
        ' "$result_file"
    } > "$stats_file"

    log_info "Completed benchmark with $threads threads"

    # Show quick summary (skip header line)
    if [ -s "$stats_file" ]; then
        local tps=$(awk -F',' 'NR==2 {print $7}' "$stats_file")
        local tpmC=$(awk -F',' 'NR==2 {print $12}' "$stats_file")
        local lat_avg=$(awk -F',' 'NR==2 {print $9}' "$stats_file")
        log_info "  TPS: $tps, TpmC: $tpmC, Avg Latency: ${lat_avg}ms"
    fi
}

wait_for_ssd_cooldown || log_info "Skipping cooldown (temperature check unavailable)"
run_benchmark "$THREADS"
if [ $? -ne 0 ]; then
    log_error "Benchmark failed for $THREADS threads"
    exit 1
fi
