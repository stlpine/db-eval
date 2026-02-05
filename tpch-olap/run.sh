#!/bin/bash
# TPC-H OLAP Benchmark Execution Script
# Executes 22 queries with cold and warm runs, records per-query times

set -e
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"

usage() {
    echo "Usage: $0 <engine> <result_dir>"
    echo "Engines: vanilla-innodb, percona-innodb, percona-myrocks"
    exit 1
}

if [ $# -ne 2 ]; then
    usage
fi

ENGINE=$1
RESULT_DIR=$2

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

QUERIES_DIR="${SCRIPT_DIR}/queries/mysql"
NUM_QUERIES=22

if [ ! -d "$QUERIES_DIR" ]; then
    log_error "Queries directory not found: $QUERIES_DIR"
    exit 1
fi

log_info "Running TPC-H: engine=$ENGINE, queries=$NUM_QUERIES, runs=$TPCH_QUERY_RUNS"
log_info "Scale Factor: $TPCH_SCALE_FACTOR"
log_info "Results directory: $RESULT_DIR"

mkdir -p "$RESULT_DIR"

# Capture database size before running queries
log_info "Capturing database size..."
SIZE_INFO=$(mysql --socket="$SOCKET" -N -e "
    SELECT
        ROUND(SUM(data_length) / 1024 / 1024 / 1024, 4),
        ROUND(SUM(index_length) / 1024 / 1024 / 1024, 4),
        ROUND((SUM(data_length) + SUM(index_length)) / 1024 / 1024 / 1024, 4)
    FROM information_schema.tables
    WHERE table_schema = '$BENCHMARK_DB';
")
DATA_GB=$(echo "$SIZE_INFO" | awk '{print $1}')
INDEX_GB=$(echo "$SIZE_INFO" | awk '{print $2}')
TOTAL_GB=$(echo "$SIZE_INFO" | awk '{print $3}')
ROW_COUNT=$(mysql --socket="$SOCKET" -N -e "
    SELECT SUM(TABLE_ROWS) FROM information_schema.tables WHERE table_schema = '$BENCHMARK_DB';
")

# Save size metrics
SIZE_METRICS_FILE="${RESULT_DIR}/tpch_size_metrics.csv"
{
    echo "engine,scale_factor,row_count,data_gb,index_gb,total_gb"
    echo "${ENGINE},${TPCH_SCALE_FACTOR},${ROW_COUNT},${DATA_GB},${INDEX_GB},${TOTAL_GB}"
} > "$SIZE_METRICS_FILE"
log_info "Database size: ${TOTAL_GB} GB (Data: ${DATA_GB} GB, Index: ${INDEX_GB} GB)"

# Track background monitoring PIDs for cleanup
MONITOR_PIDS=""

cleanup_monitors() {
    if [ -n "$MONITOR_PIDS" ]; then
        kill $MONITOR_PIDS 2>/dev/null
        wait $MONITOR_PIDS 2>/dev/null
    fi
}

trap cleanup_monitors EXIT

# Start monitoring
pidstat -u -r -d 1 > "${RESULT_DIR}/tpch_pidstat.txt" 2>&1 &
PIDSTAT_PID=$!

iostat -x 1 > "${RESULT_DIR}/tpch_iostat.txt" 2>&1 &
IOSTAT_PID=$!

MONITOR_PIDS="$PIDSTAT_PID $IOSTAT_PID"

# Initialize results files
QUERY_TIMES_FILE="${RESULT_DIR}/tpch_query_times.csv"
SUMMARY_FILE="${RESULT_DIR}/tpch_summary.csv"

echo "query_num,run,time_seconds,status" > "$QUERY_TIMES_FILE"
echo "query_num,cold,warm1,warm2,min_time,status" > "$SUMMARY_FILE"

# Function to run a single query with timeout
run_query() {
    local query_num=$1
    local run_num=$2
    local query_file=$3

    local start_time=$(date +%s.%N)

    # Run query with timeout, suppress output
    local result
    result=$(timeout "$TPCH_QUERY_TIMEOUT" mysql --socket="$SOCKET" "$BENCHMARK_DB" < "$query_file" 2>&1 >/dev/null)
    local exit_code=$?

    local end_time=$(date +%s.%N)
    local elapsed=$(echo "$end_time - $start_time" | bc)

    local status="OK"
    if [ $exit_code -eq 124 ]; then
        status="TIMEOUT"
        elapsed="$TPCH_QUERY_TIMEOUT"
    elif [ $exit_code -ne 0 ]; then
        status="ERROR"
        # Log error for debugging
        echo "Query $query_num error: $result" >> "${RESULT_DIR}/tpch_errors.log"
    fi

    echo "$query_num,$run_num,$elapsed,$status" >> "$QUERY_TIMES_FILE"
    echo "$elapsed:$status"
}

# Run queries
for ((q=1; q<=NUM_QUERIES; q++)); do
    query_file="${QUERIES_DIR}/${q}.sql"

    if [ ! -f "$query_file" ]; then
        log_error "Query file not found: $query_file"
        continue
    fi

    # Get first line of query for display (skip comments)
    query_display=$(grep -v '^--' "$query_file" | head -1 | cut -c1-50)
    log_info "Query $q/$NUM_QUERIES: ${query_display}..."

    declare -a run_times
    declare -a run_statuses

    for ((run=1; run<=TPCH_QUERY_RUNS; run++)); do
        run_name="run$run"
        if [ $run -eq 1 ]; then
            run_name="cold"
        elif [ $run -eq 2 ]; then
            run_name="warm1"
        elif [ $run -eq 3 ]; then
            run_name="warm2"
        fi

        result=$(run_query "$q" "$run_name" "$query_file")
        time_val=$(echo "$result" | cut -d: -f1)
        status_val=$(echo "$result" | cut -d: -f2)

        run_times[$run]="$time_val"
        run_statuses[$run]="$status_val"

        printf "  %s: %.3fs (%s)\n" "$run_name" "$time_val" "$status_val"
    done

    # Calculate min time (for successful runs only)
    min_time="N/A"
    overall_status="OK"
    for ((run=1; run<=TPCH_QUERY_RUNS; run++)); do
        if [ "${run_statuses[$run]}" != "OK" ]; then
            overall_status="${run_statuses[$run]}"
        fi
    done

    if [ "$overall_status" = "OK" ]; then
        min_time=$(printf '%s\n' "${run_times[@]}" | sort -n | head -1)
    fi

    # Write summary (handle variable number of runs)
    cold_time="${run_times[1]:-N/A}"
    warm1_time="${run_times[2]:-N/A}"
    warm2_time="${run_times[3]:-N/A}"

    echo "$q,$cold_time,$warm1_time,$warm2_time,$min_time,$overall_status" >> "$SUMMARY_FILE"
done

# Stop monitoring (ignore exit status of monitoring processes)
kill $PIDSTAT_PID $IOSTAT_PID 2>/dev/null || true
wait $PIDSTAT_PID $IOSTAT_PID 2>/dev/null || true
MONITOR_PIDS=""

# Generate overall statistics
log_info "Generating summary statistics..."

STATS_FILE="${RESULT_DIR}/tpch_stats.txt"
{
    echo "============================================================"
    echo "TPC-H Results Summary"
    echo "Generated: $(date)"
    echo "Engine: $ENGINE"
    echo "Scale Factor: $TPCH_SCALE_FACTOR"
    echo "Queries: $NUM_QUERIES"
    echo "Runs per query: $TPCH_QUERY_RUNS"
    echo "Timeout: ${TPCH_QUERY_TIMEOUT}s"
    echo "============================================================"
    echo ""

    # Count successes and failures
    ok_count=$(grep -c ",OK$" "$SUMMARY_FILE" 2>/dev/null || echo "0")
    timeout_count=$(grep -c ",TIMEOUT$" "$SUMMARY_FILE" 2>/dev/null || echo "0")
    error_count=$(grep -c ",ERROR$" "$SUMMARY_FILE" 2>/dev/null || echo "0")

    echo "Results:"
    echo "  Successful queries: $ok_count"
    echo "  Timed out queries: $timeout_count"
    echo "  Failed queries: $error_count"
    echo ""

    # Calculate total time for cold run (excluding header)
    if [ "$ok_count" -gt 0 ]; then
        cold_total=$(tail -n +2 "$SUMMARY_FILE" | cut -d, -f2 | grep -v "N/A" | awk '{sum+=$1} END {print sum}')
        echo "Total cold run time: ${cold_total}s"

        # Best times sum (geometric mean is standard for TPC-H but sum is simpler)
        min_total=$(tail -n +2 "$SUMMARY_FILE" | cut -d, -f5 | grep -v "N/A" | awk '{sum+=$1} END {print sum}')
        echo "Total best time: ${min_total}s"

        # Calculate geometric mean of best times (TPC-H standard)
        geo_mean=$(tail -n +2 "$SUMMARY_FILE" | cut -d, -f5 | grep -v "N/A" | awk '
            BEGIN { log_sum = 0; n = 0 }
            { if ($1 > 0) { log_sum += log($1); n++ } }
            END { if (n > 0) printf "%.3f", exp(log_sum/n); else print "N/A" }
        ')
        echo "Geometric mean (best times): ${geo_mean}s"
    fi

    echo ""
    echo "Database Size:"
    echo "  Scale Factor: ${TPCH_SCALE_FACTOR}"
    echo "  Data: ${DATA_GB} GB"
    echo "  Index: ${INDEX_GB} GB"
    echo "  Total: ${TOTAL_GB} GB"

} > "$STATS_FILE"

cat "$STATS_FILE"

log_info "TPC-H benchmark completed"
log_info "Results saved to: $RESULT_DIR"
log_info "  Query times: $QUERY_TIMES_FILE"
log_info "  Summary: $SUMMARY_FILE"
