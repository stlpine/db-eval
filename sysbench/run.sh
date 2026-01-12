#!/bin/bash
# Sysbench Benchmark Execution Script

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"

usage() {
    echo "Usage: $0 <engine> [workload]"
    echo ""
    echo "Engines: vanilla-innodb, percona-innodb, percona-myrocks"
    echo "Workload: oltp_read_write, oltp_read_only, etc. (default: all)"
    exit 1
}

if [ $# -lt 1 ]; then
    usage
fi

ENGINE=$1
WORKLOAD=${2:-all}

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

# Create results directory
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_DIR="${RESULTS_DIR}/sysbench/${ENGINE}/${TIMESTAMP}"
mkdir -p "$RESULT_DIR"

log_info "Starting sysbench benchmark for $ENGINE"
log_info "Results will be saved to: $RESULT_DIR"

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

# Run benchmarks
for workload in $WORKLOADS; do
    log_info "Starting workload: $workload"

    for threads in $BENCHMARK_THREADS; do
        # Wait for SSD to cool down before each test
        wait_for_ssd_cooldown || log_info "Skipping cooldown (temperature check unavailable)"

        run_benchmark "$workload" "$threads"
    done

    log_info "Completed workload: $workload"
done

# Consolidate results
log_info "Consolidating results..."
CONSOLIDATED_CSV="${RESULT_DIR}/consolidated_results.csv"
echo "engine,workload,threads,tps,qps,latency_avg,latency_95p,latency_99p" > "$CONSOLIDATED_CSV"

for stats_file in "${RESULT_DIR}"/*_stats.csv; do
    if [ -f "$stats_file" ]; then
        tail -n +2 "$stats_file" >> "$CONSOLIDATED_CSV"
    fi
done

log_info "Benchmark completed successfully!"
log_info "Results saved to: $RESULT_DIR"
log_info "Consolidated results: $CONSOLIDATED_CSV"
