#!/bin/bash
# Compare benchmark results between InnoDB and MyRocks

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"

usage() {
    cat << EOF
Usage: $0 <benchmark> <innodb_result_dir> <myrocks_result_dir>

Benchmark:
    sysbench        - Compare sysbench results
    tpcc            - Compare TPC-C results
    sysbench-tpcc   - Compare sysbench-tpcc results

Arguments:
    innodb_result_dir   - Path to InnoDB results directory
    myrocks_result_dir  - Path to MyRocks results directory

Example:
    $0 sysbench results/sysbench/innodb/20260108_120000 results/sysbench/myrocks/20260108_130000
    $0 tpcc results/tpcc/innodb/20260108_140000 results/tpcc/myrocks/20260108_150000
    $0 sysbench-tpcc results/sysbench-tpcc/vanilla-innodb/20260108_160000 results/sysbench-tpcc/percona-myrocks/20260108_170000
EOF
    exit 1
}

if [ $# -ne 3 ]; then
    usage
fi

BENCHMARK=$1
INNODB_DIR=$2
MYROCKS_DIR=$3

if [ ! -d "$INNODB_DIR" ]; then
    log_error "InnoDB results directory not found: $INNODB_DIR"
    exit 1
fi

if [ ! -d "$MYROCKS_DIR" ]; then
    log_error "MyRocks results directory not found: $MYROCKS_DIR"
    exit 1
fi

COMPARISON_DIR="${RESULTS_DIR}/comparison/${BENCHMARK}/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$COMPARISON_DIR"

log_info "Comparing $BENCHMARK results"
log_info "InnoDB: $INNODB_DIR"
log_info "MyRocks: $MYROCKS_DIR"
log_info "Output: $COMPARISON_DIR"

compare_sysbench() {
    local innodb_csv="${INNODB_DIR}/consolidated_results.csv"
    local myrocks_csv="${MYROCKS_DIR}/consolidated_results.csv"

    if [ ! -f "$innodb_csv" ] || [ ! -f "$myrocks_csv" ]; then
        log_error "Consolidated CSV files not found"
        exit 1
    fi

    # Merge results
    {
        head -n 1 "$innodb_csv"
        tail -n +2 "$innodb_csv"
        tail -n +2 "$myrocks_csv"
    } > "${COMPARISON_DIR}/merged_results.csv"

    # Create comparison report
    {
        echo "=========================================="
        echo "Sysbench Performance Comparison"
        echo "InnoDB vs MyRocks"
        echo "Date: $(date)"
        echo "=========================================="
        echo ""
        echo "InnoDB Results: $INNODB_DIR"
        echo "MyRocks Results: $MYROCKS_DIR"
        echo ""
        echo "==================== Throughput (TPS) Comparison ===================="
        echo ""

        # Compare TPS for each workload and thread count
        awk -F',' 'NR>1 {
            key=$2"_"$3
            if ($1 ~ /innodb/) {
                innodb[key] = $4
            } else {
                myrocks[key] = $4
            }
        }
        END {
            printf "%-30s %15s %15s %15s\n", "Workload_Threads", "InnoDB TPS", "MyRocks TPS", "Speedup"
            print "--------------------------------------------------------------------------"
            for (key in innodb) {
                if (key in myrocks) {
                    speedup = myrocks[key] / innodb[key]
                    printf "%-30s %15.2f %15.2f %15.2fx\n", key, innodb[key], myrocks[key], speedup
                }
            }
        }' "${COMPARISON_DIR}/merged_results.csv"

        echo ""
        echo "==================== Latency (ms) Comparison ===================="
        echo ""

        # Compare latency
        awk -F',' 'NR>1 {
            key=$2"_"$3
            if ($1 ~ /innodb/) {
                innodb_lat[key] = $6
            } else {
                myrocks_lat[key] = $6
            }
        }
        END {
            printf "%-30s %15s %15s %15s\n", "Workload_Threads", "InnoDB Lat", "MyRocks Lat", "Reduction"
            print "--------------------------------------------------------------------------"
            for (key in innodb_lat) {
                if (key in myrocks_lat) {
                    reduction = (innodb_lat[key] - myrocks_lat[key]) / innodb_lat[key] * 100
                    printf "%-30s %15.2f %15.2f %14.1f%%\n", key, innodb_lat[key], myrocks_lat[key], reduction
                }
            }
        }' "${COMPARISON_DIR}/merged_results.csv"

    } | tee "${COMPARISON_DIR}/comparison_report.txt"
}

compare_tpcc() {
    local innodb_csv="${INNODB_DIR}/consolidated_results.csv"
    local myrocks_csv="${MYROCKS_DIR}/consolidated_results.csv"

    if [ ! -f "$innodb_csv" ] || [ ! -f "$myrocks_csv" ]; then
        log_error "Consolidated CSV files not found"
        exit 1
    fi

    # Merge results
    {
        head -n 1 "$innodb_csv"
        tail -n +2 "$innodb_csv"
        tail -n +2 "$myrocks_csv"
    } > "${COMPARISON_DIR}/merged_results.csv"

    # Create comparison report
    {
        echo "=========================================="
        echo "TPC-C Performance Comparison"
        echo "InnoDB vs MyRocks"
        echo "Date: $(date)"
        echo "=========================================="
        echo ""
        echo "InnoDB Results: $INNODB_DIR"
        echo "MyRocks Results: $MYROCKS_DIR"
        echo ""
        echo "==================== TpmC Comparison ===================="
        echo ""

        awk -F',' 'NR>1 {
            key=$2
            if ($1 ~ /innodb/) {
                innodb[key] = $5
            } else {
                myrocks[key] = $5
            }
        }
        END {
            printf "%-15s %15s %15s %15s\n", "Threads", "InnoDB TpmC", "MyRocks TpmC", "Speedup"
            print "-------------------------------------------------------------"
            for (key in innodb) {
                if (key in myrocks) {
                    speedup = myrocks[key] / innodb[key]
                    printf "%-15s %15.2f %15.2f %15.2fx\n", key, innodb[key], myrocks[key], speedup
                }
            }
        }' "${COMPARISON_DIR}/merged_results.csv"

    } | tee "${COMPARISON_DIR}/comparison_report.txt"
}

compare_sysbench_tpcc() {
    local innodb_csv="${INNODB_DIR}/consolidated_results.csv"
    local myrocks_csv="${MYROCKS_DIR}/consolidated_results.csv"

    if [ ! -f "$innodb_csv" ] || [ ! -f "$myrocks_csv" ]; then
        log_error "Consolidated CSV files not found"
        exit 1
    fi

    # Merge results
    {
        head -n 1 "$innodb_csv"
        tail -n +2 "$innodb_csv"
        tail -n +2 "$myrocks_csv"
    } > "${COMPARISON_DIR}/merged_results.csv"

    # Create comparison report
    {
        echo "=========================================="
        echo "Sysbench-TPCC Performance Comparison"
        echo "InnoDB vs MyRocks"
        echo "Date: $(date)"
        echo "=========================================="
        echo ""
        echo "InnoDB Results: $INNODB_DIR"
        echo "MyRocks Results: $MYROCKS_DIR"
        echo ""
        echo "==================== TpmC Comparison ===================="
        echo ""

        # Compare TpmC (TPC-C metric)
        awk -F',' 'NR>1 {
            key=$2
            if ($1 ~ /innodb/) {
                innodb[key] = $12
            } else {
                myrocks[key] = $12
            }
        }
        END {
            printf "%-15s %15s %15s %15s\n", "Threads", "InnoDB TpmC", "MyRocks TpmC", "Speedup"
            print "-------------------------------------------------------------"
            for (key in innodb) {
                if (key in myrocks) {
                    speedup = myrocks[key] / innodb[key]
                    printf "%-15s %15.2f %15.2f %15.2fx\n", key, innodb[key], myrocks[key], speedup
                }
            }
        }' "${COMPARISON_DIR}/merged_results.csv"

        echo ""
        echo "==================== TPS Comparison ===================="
        echo ""

        # Compare TPS (sysbench metric)
        awk -F',' 'NR>1 {
            key=$2
            if ($1 ~ /innodb/) {
                innodb[key] = $7
            } else {
                myrocks[key] = $7
            }
        }
        END {
            printf "%-15s %15s %15s %15s\n", "Threads", "InnoDB TPS", "MyRocks TPS", "Speedup"
            print "-------------------------------------------------------------"
            for (key in innodb) {
                if (key in myrocks) {
                    speedup = myrocks[key] / innodb[key]
                    printf "%-15s %15.2f %15.2f %15.2fx\n", key, innodb[key], myrocks[key], speedup
                }
            }
        }' "${COMPARISON_DIR}/merged_results.csv"

        echo ""
        echo "==================== Latency (ms) Comparison ===================="
        echo ""

        # Compare average latency
        awk -F',' 'NR>1 {
            key=$2
            if ($1 ~ /innodb/) {
                innodb_lat[key] = $9
            } else {
                myrocks_lat[key] = $9
            }
        }
        END {
            printf "%-15s %15s %15s %15s\n", "Threads", "InnoDB Lat", "MyRocks Lat", "Reduction"
            print "-------------------------------------------------------------"
            for (key in innodb_lat) {
                if (key in myrocks_lat) {
                    reduction = (innodb_lat[key] - myrocks_lat[key]) / innodb_lat[key] * 100
                    printf "%-15s %15.2f %15.2f %14.1f%%\n", key, innodb_lat[key], myrocks_lat[key], reduction
                }
            }
        }' "${COMPARISON_DIR}/merged_results.csv"

    } | tee "${COMPARISON_DIR}/comparison_report.txt"
}

# Run comparison based on benchmark type
case $BENCHMARK in
    sysbench)
        compare_sysbench
        ;;
    tpcc)
        compare_tpcc
        ;;
    sysbench-tpcc)
        compare_sysbench_tpcc
        ;;
    *)
        log_error "Unknown benchmark: $BENCHMARK"
        usage
        ;;
esac

log_info "Comparison completed successfully!"
log_info "Results saved to: $COMPARISON_DIR"
