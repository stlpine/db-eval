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
    echo "BENCHMARK_DB: $BENCHMARK_DB"
    echo "BENCHMARK_THREADS: $BENCHMARK_THREADS"
    echo "BENCHMARK_DURATION: $BENCHMARK_DURATION"
    echo "SYSBENCH_TABLE_SIZE: $SYSBENCH_TABLE_SIZE"
    echo "SYSBENCH_TABLES: $SYSBENCH_TABLES"
    echo "SYSBENCH_WORKLOADS: $SYSBENCH_WORKLOADS"
    echo "WORKLOADS (selected): $WORKLOADS"
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
