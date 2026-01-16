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

    # Extract engine names from CSV files
    local engine1=$(awk -F',' 'NR==2 {print $1}' "$innodb_csv")
    local engine2=$(awk -F',' 'NR==2 {print $1}' "$myrocks_csv")

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
        echo "$engine1 vs $engine2"
        echo "Date: $(date)"
        echo "=========================================="
        echo ""
        echo "[1] $engine1: $INNODB_DIR"
        echo "[2] $engine2: $MYROCKS_DIR"
        echo ""
        echo "==================== Throughput (TPS) Comparison ===================="
        echo ""

        # Compare TPS for each workload and thread count
        tps_data=$(awk -F',' -v eng1="$engine1" -v eng2="$engine2" 'NR>1 {
            key=$2"_"$3
            if ($1 == eng1) {
                engine1[key] = $4
            } else if ($1 == eng2) {
                engine2[key] = $4
            }
        }
        END {
            for (key in engine1) {
                if (key in engine2 && engine1[key] > 0) {
                    speedup = engine2[key] / engine1[key]
                    printf "%-30s %12.2f %12.2f %9.2fx\n", key, engine1[key], engine2[key], speedup
                }
            }
        }' "${COMPARISON_DIR}/merged_results.csv" | sort -t_ -k1,1 -k2,2n)
        printf "%-30s %12s %12s %10s\n" "Workload_Threads" "TPS [1]" "TPS [2]" "Speedup"
        echo "-------------------------------------------------------------------"
        echo "$tps_data"

        echo ""
        echo "==================== Latency (ms) Comparison ===================="
        echo ""

        # Compare latency
        latency_data=$(awk -F',' -v eng1="$engine1" -v eng2="$engine2" 'NR>1 {
            key=$2"_"$3
            if ($1 == eng1) {
                engine1_lat[key] = $6
            } else if ($1 == eng2) {
                engine2_lat[key] = $6
            }
        }
        END {
            for (key in engine1_lat) {
                if (key in engine2_lat && engine1_lat[key] > 0) {
                    reduction = (engine1_lat[key] - engine2_lat[key]) / engine1_lat[key] * 100
                    printf "%-30s %12.2f %12.2f %11.1f%%\n", key, engine1_lat[key], engine2_lat[key], reduction
                }
            }
        }' "${COMPARISON_DIR}/merged_results.csv" | sort -t_ -k1,1 -k2,2n)
        printf "%-30s %12s %12s %12s\n" "Workload_Threads" "Lat [1]" "Lat [2]" "Reduction"
        echo "---------------------------------------------------------------------"
        echo "$latency_data"

    } | tee "${COMPARISON_DIR}/comparison_report.txt"
}

compare_tpcc() {
    local innodb_csv="${INNODB_DIR}/consolidated_results.csv"
    local myrocks_csv="${MYROCKS_DIR}/consolidated_results.csv"

    if [ ! -f "$innodb_csv" ] || [ ! -f "$myrocks_csv" ]; then
        log_error "Consolidated CSV files not found"
        exit 1
    fi

    # Extract engine names from CSV files
    local engine1=$(awk -F',' 'NR==2 {print $1}' "$innodb_csv")
    local engine2=$(awk -F',' 'NR==2 {print $1}' "$myrocks_csv")

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
        echo "$engine1 vs $engine2"
        echo "Date: $(date)"
        echo "=========================================="
        echo ""
        echo "[1] $engine1: $INNODB_DIR"
        echo "[2] $engine2: $MYROCKS_DIR"
        echo ""
        echo "==================== TpmC Comparison ===================="
        echo ""

        tpmc_data=$(awk -F',' -v eng1="$engine1" -v eng2="$engine2" 'NR>1 {
            key=$2
            if ($1 == eng1) {
                engine1[key] = $5
            } else if ($1 == eng2) {
                engine2[key] = $5
            }
        }
        END {
            for (key in engine1) {
                if (key in engine2 && engine1[key] > 0) {
                    speedup = engine2[key] / engine1[key]
                    printf "%-10s %20.2f %20.2f %9.2fx\n", key, engine1[key], engine2[key], speedup
                }
            }
        }' "${COMPARISON_DIR}/merged_results.csv" | sort -n)
        printf "%-10s %20s %20s %10s\n" "Threads" "$engine1" "$engine2" "Speedup"
        echo "---------------------------------------------------------------"
        echo "$tpmc_data"

        echo ""
        echo "==================== Latency (ms) Comparison ===================="
        echo ""

        # Check if latency columns exist (columns 7 and 8)
        if head -1 "${COMPARISON_DIR}/merged_results.csv" | grep -q "latency_avg"; then
            # Compare average latency
            latency_data=$(awk -F',' -v eng1="$engine1" -v eng2="$engine2" 'NR>1 {
                key=$2
                if ($1 == eng1) {
                    engine1_lat[key] = $7
                    engine1_95[key] = $8
                } else if ($1 == eng2) {
                    engine2_lat[key] = $7
                    engine2_95[key] = $8
                }
            }
            END {
                for (key in engine1_lat) {
                    if (key in engine2_lat && engine1_lat[key] > 0) {
                        reduction = (engine1_lat[key] - engine2_lat[key]) / engine1_lat[key] * 100
                        printf "%-10s %12.2f %12.2f %12.2f %12.2f %11.1f%%\n", key, engine1_lat[key], engine2_lat[key], engine1_95[key], engine2_95[key], reduction
                    }
                }
            }' "${COMPARISON_DIR}/merged_results.csv" | sort -n)
            printf "%-10s %12s %12s %12s %12s %12s\n" "Threads" "Avg [1]" "Avg [2]" "95% [1]" "95% [2]" "Reduction"
            echo "---------------------------------------------------------------------------"
            echo "$latency_data"
        else
            echo "(Latency data not available - run benchmark again to collect latency)"
        fi

    } | tee "${COMPARISON_DIR}/comparison_report.txt"
}

compare_sysbench_tpcc() {
    local innodb_csv="${INNODB_DIR}/consolidated_results.csv"
    local myrocks_csv="${MYROCKS_DIR}/consolidated_results.csv"

    if [ ! -f "$innodb_csv" ] || [ ! -f "$myrocks_csv" ]; then
        log_error "Consolidated CSV files not found"
        exit 1
    fi

    # Extract engine names from CSV files
    local engine1=$(awk -F',' 'NR==2 {print $1}' "$innodb_csv")
    local engine2=$(awk -F',' 'NR==2 {print $1}' "$myrocks_csv")

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
        echo "$engine1 vs $engine2"
        echo "Date: $(date)"
        echo "=========================================="
        echo ""
        echo "[1] $engine1: $INNODB_DIR"
        echo "[2] $engine2: $MYROCKS_DIR"
        echo ""
        echo "==================== TpmC Comparison ===================="
        echo ""

        # Compare TpmC (TPC-C metric)
        tpmc_data=$(awk -F',' -v eng1="$engine1" -v eng2="$engine2" 'NR>1 {
            key=$2
            if ($1 == eng1) {
                engine1[key] = $12
            } else if ($1 == eng2) {
                engine2[key] = $12
            }
        }
        END {
            for (key in engine1) {
                if (key in engine2 && engine1[key] > 0) {
                    speedup = engine2[key] / engine1[key]
                    printf "%-10s %15.2f %15.2f %9.2fx\n", key, engine1[key], engine2[key], speedup
                }
            }
        }' "${COMPARISON_DIR}/merged_results.csv" | sort -n)
        printf "%-10s %15s %15s %10s\n" "Threads" "TpmC [1]" "TpmC [2]" "Speedup"
        echo "-----------------------------------------------------"
        echo "$tpmc_data"

        echo ""
        echo "==================== TPS Comparison ===================="
        echo ""

        # Compare TPS (sysbench metric)
        tps_data=$(awk -F',' -v eng1="$engine1" -v eng2="$engine2" 'NR>1 {
            key=$2
            if ($1 == eng1) {
                engine1[key] = $7
            } else if ($1 == eng2) {
                engine2[key] = $7
            }
        }
        END {
            for (key in engine1) {
                if (key in engine2 && engine1[key] > 0) {
                    speedup = engine2[key] / engine1[key]
                    printf "%-10s %15.2f %15.2f %9.2fx\n", key, engine1[key], engine2[key], speedup
                }
            }
        }' "${COMPARISON_DIR}/merged_results.csv" | sort -n)
        printf "%-10s %15s %15s %10s\n" "Threads" "TPS [1]" "TPS [2]" "Speedup"
        echo "-----------------------------------------------------"
        echo "$tps_data"

        echo ""
        echo "==================== Latency (ms) Comparison ===================="
        echo ""

        # Compare average latency
        latency_data=$(awk -F',' -v eng1="$engine1" -v eng2="$engine2" 'NR>1 {
            key=$2
            if ($1 == eng1) {
                engine1_lat[key] = $9
            } else if ($1 == eng2) {
                engine2_lat[key] = $9
            }
        }
        END {
            for (key in engine1_lat) {
                if (key in engine2_lat && engine1_lat[key] > 0) {
                    reduction = (engine1_lat[key] - engine2_lat[key]) / engine1_lat[key] * 100
                    printf "%-10s %12.2f %12.2f %11.1f%%\n", key, engine1_lat[key], engine2_lat[key], reduction
                }
            }
        }' "${COMPARISON_DIR}/merged_results.csv" | sort -n)
        printf "%-10s %12s %12s %12s\n" "Threads" "Lat [1]" "Lat [2]" "Reduction"
        echo "-------------------------------------------------"
        echo "$latency_data"

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
