#!/bin/bash
# Sysbench Benchmark Execution Script
# Runs all workloads for a single thread count; called by run-benchmark.sh for each thread count.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"

usage() {
    echo "Usage: $0 <engine> <workload> <threads> <result_dir>"
    echo "Engines: vanilla-innodb, percona-innodb, percona-myrocks"
    echo "Workload: oltp_read_write, oltp_read_only, etc. or 'all'"
    exit 1
}

if [ $# -ne 4 ]; then
    usage
fi

ENGINE=$1
WORKLOAD=$2
THREADS=$3
RESULT_DIR=$4

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

# Set workloads to run
if [ "$WORKLOAD" = "all" ]; then
    WORKLOADS="${SYSBENCH_WORKLOADS}"
else
    WORKLOADS="$WORKLOAD"
fi

# Check if MySQL is running
if ! mysqladmin --socket="$SOCKET" ping &>/dev/null; then
    log_error "MySQL is not running. Please start MySQL first using: ./scripts/mysql-control.sh $ENGINE start"
    exit 1
fi

log_info "Running sysbench: engine=$ENGINE, threads=$THREADS, result_dir=$RESULT_DIR"

# Track background monitoring PIDs for cleanup
MONITOR_PIDS=""

cleanup_monitors() {
    if [ -n "$MONITOR_PIDS" ]; then
        kill $MONITOR_PIDS 2>/dev/null
        wait $MONITOR_PIDS 2>/dev/null
    fi
}

trap cleanup_monitors EXIT

# Function to run a single benchmark
run_benchmark() {
    local workload=$1
    local threads=$2

    log_info "Running $workload with $threads threads"

    local result_file="${RESULT_DIR}/${workload}_threads${threads}.txt"
    local stats_file="${RESULT_DIR}/${workload}_threads${threads}_stats.csv"

    # Start monitoring CPU and I/O
    pidstat -u -r -d 1 > "${RESULT_DIR}/${workload}_threads${threads}_pidstat.txt" 2>&1 &
    local pidstat_pid=$!

    iostat -x 1 > "${RESULT_DIR}/${workload}_threads${threads}_iostat.txt" 2>&1 &
    local iostat_pid=$!

    # Track PIDs for cleanup on exit
    MONITOR_PIDS="$pidstat_pid $iostat_pid"

    # Run sysbench
    {
        echo "==================================================================="
        echo "Workload: $workload"
        echo "Threads: $threads"
        echo "Engine: $ENGINE"
        echo "Date: $(date)"
        echo "==================================================================="
        echo ""

        sysbench "$workload" \
            --mysql-socket="$SOCKET" \
            --mysql-db="$BENCHMARK_DB" \
            --tables="$SYSBENCH_TABLES" \
            --table-size="$SYSBENCH_TABLE_SIZE" \
            --threads="$threads" \
            --time="$BENCHMARK_DURATION" \
            --report-interval=10 \
            --db-ps-mode=disable \
            run

    } > "$result_file" 2>&1

    # Stop monitoring
    kill $pidstat_pid $iostat_pid 2>/dev/null
    wait $pidstat_pid $iostat_pid 2>/dev/null
    MONITOR_PIDS=""

    # Extract key metrics
    # Format:
    #   transactions:                        126695 (422.31 per sec.)
    #   queries:                             2027120 (6757.01 per sec.)
    #   avg:                                    2.37
    #   95th percentile:                        3.07
    {
        echo "engine,workload,threads,tps,qps,latency_avg,latency_95p,latency_99p"
        awk -v engine="$ENGINE" -v workload="$workload" -v threads="$threads" '
        /transactions:.*per sec/ {
            gsub(/.*\(/, ""); gsub(/ per sec.*/, "")
            tps = $0 + 0
        }
        /queries:.*per sec/ {
            gsub(/.*\(/, ""); gsub(/ per sec.*/, "")
            qps = $0 + 0
        }
        /^[[:space:]]+avg:/ { lat_avg = $2 + 0 }
        /95th percentile:/ { lat_95 = $3 + 0 }
        /99th percentile:/ { lat_99 = $3 + 0 }
        END { printf "%s,%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f\n", engine, workload, threads, tps, qps, lat_avg, lat_95, lat_99 }
        ' "$result_file"
    } >> "$stats_file"

    log_info "Completed $workload with $threads threads"
}

for workload in $WORKLOADS; do
    wait_for_ssd_cooldown || log_info "Skipping cooldown (temperature check unavailable)"
    run_benchmark "$workload" "$THREADS"
done
