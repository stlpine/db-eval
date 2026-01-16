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
    echo "Cgroup Memory Limits:"
    if [ -f /sys/fs/cgroup/memory.max ]; then
        # cgroup v2 (process's own cgroup)
        echo "  memory.max: $(cat /sys/fs/cgroup/memory.max 2>/dev/null)"
        echo "  memory.current: $(cat /sys/fs/cgroup/memory.current 2>/dev/null)"
    fi
    if [ -f /sys/fs/cgroup/limited_memory_group/memory.max ]; then
        # Named cgroup group
        echo "  limited_memory_group/memory.max: $(cat /sys/fs/cgroup/limited_memory_group/memory.max 2>/dev/null)"
        echo "  limited_memory_group/memory.current: $(cat /sys/fs/cgroup/limited_memory_group/memory.current 2>/dev/null)"
    fi
    # Check process's own cgroup
    if [ -f /proc/self/cgroup ]; then
        CGROUP_PATH=$(cat /proc/self/cgroup | grep -E "^0::" | cut -d: -f3)
        if [ -n "$CGROUP_PATH" ] && [ "$CGROUP_PATH" != "/" ]; then
            echo "  Process cgroup: $CGROUP_PATH"
            if [ -f "/sys/fs/cgroup${CGROUP_PATH}/memory.max" ]; then
                echo "  Process memory.max: $(cat /sys/fs/cgroup${CGROUP_PATH}/memory.max 2>/dev/null)"
                echo "  Process memory.current: $(cat /sys/fs/cgroup${CGROUP_PATH}/memory.current 2>/dev/null)"
            fi
        fi
    fi
    echo ""
    echo "Disk Info:"
    df -h "$SSD_MOUNT" 2>/dev/null
    echo ""

    echo "============================================================"
    echo "BENCHMARK PARAMETERS (env.sh)"
    echo "============================================================"
    echo "--- General ---"
    echo "BENCHMARK_DB: $BENCHMARK_DB"
    echo "BENCHMARK_THREADS: $BENCHMARK_THREADS"
    echo "BENCHMARK_DURATION: $BENCHMARK_DURATION"
    echo ""
    echo "--- TPC-C (tpcc-mysql) ---"
    echo "TPCC_WAREHOUSES: $TPCC_WAREHOUSES"
    echo "TPCC_DURATION: $TPCC_DURATION"
    echo ""
    echo "--- Sysbench ---"
    echo "SYSBENCH_TABLE_SIZE: $SYSBENCH_TABLE_SIZE"
    echo "SYSBENCH_TABLES: $SYSBENCH_TABLES"
    echo "SYSBENCH_WORKLOADS: $SYSBENCH_WORKLOADS"
    echo ""
    echo "--- Sysbench-TPCC ---"
    echo "SYSBENCH_TPCC_TABLES: $SYSBENCH_TPCC_TABLES"
    echo "SYSBENCH_TPCC_SCALE: $SYSBENCH_TPCC_SCALE"
    echo "SYSBENCH_TPCC_THREADS: $SYSBENCH_TPCC_THREADS"
    echo "SYSBENCH_TPCC_DURATION: $SYSBENCH_TPCC_DURATION"
    echo "SYSBENCH_TPCC_WARMUP: $SYSBENCH_TPCC_WARMUP"
    echo "SYSBENCH_TPCC_REPORT_INTERVAL: $SYSBENCH_TPCC_REPORT_INTERVAL"
    echo "SYSBENCH_TPCC_USE_FK: $SYSBENCH_TPCC_USE_FK"
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

# Log engine status after benchmark
log_info "Logging post-benchmark engine status..."
{
    echo ""
    echo "============================================================"
    echo "MYSQL GLOBAL STATUS (after benchmark)"
    echo "============================================================"
    mysql --socket="$SOCKET" -e "SHOW GLOBAL STATUS;" 2>/dev/null
    echo ""

    echo "============================================================"
    echo "STORAGE ENGINE STATUS (after benchmark)"
    echo "============================================================"
    if [ "$ENGINE" = "percona-myrocks" ]; then
        mysql --socket="$SOCKET" -e "SHOW ENGINE ROCKSDB STATUS\G" 2>/dev/null
    else
        mysql --socket="$SOCKET" -e "SHOW ENGINE INNODB STATUS\G" 2>/dev/null
    fi
    echo ""
} >> "$CONFIG_LOG" 2>&1

log_info "Benchmark completed successfully"
log_info "Results saved to: $RESULT_DIR"
log_info "Consolidated results: $CONSOLIDATED_CSV"

# Show summary
log_info "Summary:"
column -t -s',' "$CONSOLIDATED_CSV" | head -20

# Output result directory for scripting
echo "$RESULT_DIR"
