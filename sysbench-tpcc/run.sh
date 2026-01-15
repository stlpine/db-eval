#!/bin/bash
# Sysbench-TPCC Benchmark Execution Script

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

# Check if sysbench-tpcc exists
SYSBENCH_TPCC_DIR="${SCRIPT_DIR}/sysbench-tpcc"
if [ ! -f "$SYSBENCH_TPCC_DIR/tpcc.lua" ]; then
    log_error "sysbench-tpcc not found. Please run prepare.sh first."
    exit 1
fi

# Create results directory
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_DIR="${RESULTS_DIR}/sysbench-tpcc/${ENGINE}/${TIMESTAMP}"
mkdir -p "$RESULT_DIR"

log_info "Starting sysbench-tpcc benchmark for $ENGINE"
log_info "Results will be saved to: $RESULT_DIR"

# Function to run a single benchmark
run_benchmark() {
    local threads=$1
    local result_file="${RESULT_DIR}/tpcc_threads${threads}.txt"
    local stats_file="${RESULT_DIR}/tpcc_threads${threads}_stats.csv"
    local pidstat_file="${RESULT_DIR}/tpcc_threads${threads}_pidstat.txt"
    local iostat_file="${RESULT_DIR}/tpcc_threads${threads}_iostat.txt"

    log_info "Running sysbench-tpcc with $threads threads..."

    # Start monitoring
    pidstat -u -r -d 1 > "$pidstat_file" 2>&1 &
    local pidstat_pid=$!

    iostat -x 1 > "$iostat_file" 2>&1 &
    local iostat_pid=$!

    # Run benchmark (cd to sysbench-tpcc dir so Lua can find tpcc_common.lua)
    (cd "$SYSBENCH_TPCC_DIR" && sysbench ./tpcc.lua \
        --mysql-socket="$SOCKET" \
        --mysql-db="$BENCHMARK_DB" \
        --threads="$threads" \
        --tables="$SYSBENCH_TPCC_TABLES" \
        --scale="$SYSBENCH_TPCC_SCALE" \
        --time="$SYSBENCH_TPCC_DURATION" \
        --report-interval="$SYSBENCH_TPCC_REPORT_INTERVAL" \
        --db-driver=mysql \
        run) > "$result_file" 2>&1

    local bench_exit_code=$?

    # Stop monitoring
    kill $pidstat_pid $iostat_pid 2>/dev/null
    wait $pidstat_pid $iostat_pid 2>/dev/null

    if [ $bench_exit_code -ne 0 ]; then
        log_error "Benchmark failed for $threads threads"
        log_error "Check $result_file for details"
        return 1
    fi

    # Extract metrics from result file
    # Hybrid format: engine,threads,tables,scale,warehouses,duration,tps,qps,latency_avg,latency_95p,latency_99p,tpmC,tpmTotal

    log_info "Extracting metrics..."

    # Parse sysbench output
    # Format:
    #   transactions:                        928191 (1546.92 per sec.)
    #   queries:                             26404260 (44005.32 per sec.)
    #   avg:                                   10.34
    #   95th percentile:                       36.24
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
    ' "$result_file" > "$stats_file"

    log_info "Completed benchmark with $threads threads"

    # Show quick summary
    if [ -s "$stats_file" ]; then
        local tps=$(awk -F',' '{print $7}' "$stats_file")
        local tpmC=$(awk -F',' '{print $12}' "$stats_file")
        local lat_avg=$(awk -F',' '{print $9}' "$stats_file")
        log_info "  TPS: $tps, TpmC: $tpmC, Avg Latency: ${lat_avg}ms"
    fi
}

# Run benchmark for each thread count
for threads in $SYSBENCH_TPCC_THREADS; do
    # Wait for SSD to cool down before each test
    wait_for_ssd_cooldown || log_info "Skipping cooldown (temperature check unavailable)"

    run_benchmark "$threads"
    if [ $? -ne 0 ]; then
        log_error "Benchmark failed, stopping execution"
        exit 1
    fi
done

# Consolidate results
log_info "Consolidating results..."

CONSOLIDATED_CSV="${RESULT_DIR}/consolidated_results.csv"

# Write header
echo "engine,threads,tables,scale,warehouses,duration,tps,qps,latency_avg,latency_95p,latency_99p,tpmC,tpmTotal" > "$CONSOLIDATED_CSV"

# Append all stats
for stats_file in "${RESULT_DIR}"/tpcc_threads*_stats.csv; do
    if [ -f "$stats_file" ]; then
        cat "$stats_file" >> "$CONSOLIDATED_CSV"
    fi
done

log_info "Benchmark completed successfully"
log_info "Results saved to: $RESULT_DIR"
log_info "Consolidated results: $CONSOLIDATED_CSV"

# Show summary
log_info "Summary:"
column -t -s',' "$CONSOLIDATED_CSV" | head -20

# Output result directory for scripting
echo "$RESULT_DIR"
