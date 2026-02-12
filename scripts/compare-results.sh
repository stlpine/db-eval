#!/bin/bash
# Compare benchmark results between InnoDB and MyRocks

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"

usage() {
    cat << EOF
Usage: $0 <benchmark> <innodb_result_dir> <myrocks_result_dir>

Benchmark:
    OLTP benchmarks:
    sysbench        - Compare sysbench results
    tpcc            - Compare TPC-C results
    sysbench-tpcc   - Compare sysbench-tpcc results

    OLAP benchmarks:
    clickbench      - Compare ClickBench results (per-query times)
    tpch-olap       - Compare TPC-H results (per-query times)

Arguments:
    innodb_result_dir   - Path to InnoDB results directory
    myrocks_result_dir  - Path to MyRocks results directory

Example:
    $0 sysbench results/sysbench/innodb/20260108_120000 results/sysbench/myrocks/20260108_130000
    $0 tpcc results/tpcc/innodb/20260108_140000 results/tpcc/myrocks/20260108_150000
    $0 clickbench results/clickbench/vanilla-innodb/20260108_160000 results/clickbench/percona-myrocks/20260108_170000
    $0 tpch-olap results/tpch-olap/vanilla-innodb/20260108_180000 results/tpch-olap/percona-myrocks/20260108_190000
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

PROJECT_ROOT="$(dirname $(dirname $(readlink -f "${BASH_SOURCE[0]}")))"
COMPARISON_DIR="${PROJECT_ROOT}/comparison/${BENCHMARK}/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$COMPARISON_DIR"

log_info "Comparing $BENCHMARK results"
log_info "InnoDB: $INNODB_DIR"
log_info "MyRocks: $MYROCKS_DIR"
log_info "Output: $COMPARISON_DIR"

# ============================================================================
# Resource utilization comparison helpers
# ============================================================================

# Compare two resource_summary.csv files side by side
# Usage: compare_resource_summaries <file1> <file2> <label1> <label2>
compare_resource_summaries() {
    local file1="$1"
    local file2="$2"
    local label1="$3"
    local label2="$4"

    if [ ! -f "$file1" ] && [ ! -f "$file2" ]; then
        echo "    (Resource utilization data not available)"
        return
    fi
    if [ ! -f "$file1" ]; then
        echo "    (Missing: $file1)"
        return
    fi
    if [ ! -f "$file2" ]; then
        echo "    (Missing: $file2)"
        return
    fi

    awk -F, -v l1="$label1" -v l2="$label2" '
    BEGIN {
        n = 0
        # CPU metrics
        key[n]="cpu,user_pct";      lbl[n]="CPU user";          unit[n]="%";    sc[n]=0; n++
        key[n]="cpu,system_pct";    lbl[n]="CPU system";        unit[n]="%";    sc[n]=0; n++
        key[n]="cpu,iowait_pct";    lbl[n]="CPU iowait";        unit[n]="%";    sc[n]=0; n++
        key[n]="cpu,idle_pct";      lbl[n]="CPU idle";          unit[n]="%";    sc[n]=0; n++
        # IO metrics
        key[n]="io,read_iops";      lbl[n]="Read IOPS";        unit[n]="";     sc[n]=0; n++
        key[n]="io,write_iops";     lbl[n]="Write IOPS";       unit[n]="";     sc[n]=0; n++
        key[n]="io,read_kbps";      lbl[n]="Read throughput";   unit[n]="MB/s"; sc[n]=1024; n++
        key[n]="io,write_kbps";     lbl[n]="Write throughput";  unit[n]="MB/s"; sc[n]=1024; n++
        key[n]="io,read_await_ms";  lbl[n]="Read latency";     unit[n]="ms";   sc[n]=0; n++
        key[n]="io,write_await_ms"; lbl[n]="Write latency";    unit[n]="ms";   sc[n]=0; n++
        key[n]="io,queue_depth";    lbl[n]="Queue depth";      unit[n]="";     sc[n]=0; n++
        key[n]="io,util_pct";       lbl[n]="Disk utilization";  unit[n]="%";    sc[n]=0; n++
        # Memory metrics
        key[n]="mem,free_kb";       lbl[n]="Free memory";      unit[n]="MB";   sc[n]=1024; n++
        key[n]="mem,cache_kb";      lbl[n]="Page cache";       unit[n]="MB";   sc[n]=1024; n++
        key[n]="mem,swap_in_ps";    lbl[n]="Swap in";          unit[n]="/s";   sc[n]=0; n++
        key[n]="mem,swap_out_ps";   lbl[n]="Swap out";         unit[n]="/s";   sc[n]=0; n++
        # System metrics
        key[n]="sys,runqueue_avg";  lbl[n]="Run queue";        unit[n]="";     sc[n]=0; n++
        key[n]="sys,blocked_avg";   lbl[n]="Blocked procs";    unit[n]="";     sc[n]=0; n++
        total = n
    }
    FNR == 1 { fnum++; next }
    fnum == 1 { d1[$1","$2] = $3 }
    fnum == 2 { d2[$1","$2] = $3 }
    END {
        fmt = "    %-22s %14s %14s  %s\n"
        printf fmt, "Metric", l1, l2, "Unit"
        printf fmt, "----------------------", "--------------", "--------------", "----"

        last_cat = ""
        for (i = 0; i < total; i++) {
            k = key[i]
            split(k, p, ",")
            if (p[1] != last_cat && last_cat != "") printf "\n"
            last_cat = p[1]

            v1 = (k in d1) ? d1[k]+0 : -1
            v2 = (k in d2) ? d2[k]+0 : -1
            if (v1 < 0 && v2 < 0) continue

            if (sc[i] > 0) {
                if (v1 >= 0) v1 = v1 / sc[i]
                if (v2 >= 0) v2 = v2 / sc[i]
            }

            s1 = (v1 >= 0) ? sprintf("%14.1f", v1) : sprintf("%14s", "N/A")
            s2 = (v2 >= 0) ? sprintf("%14.1f", v2) : sprintf("%14s", "N/A")
            printf "    %-22s %s %s  %s\n", lbl[i], s1, s2, unit[i]
        }
    }
    ' "$file1" "$file2"
}

# Discover and compare all resource summary files between two result directories
# Usage: compare_all_resource_summaries <dir1> <dir2> <label1> <label2>
compare_all_resource_summaries() {
    local dir1="$1"
    local dir2="$2"
    local label1="$3"
    local label2="$4"

    echo "==================== Resource Utilization Comparison ===================="
    echo ""

    local found=0

    # Iterate over resource summary files from dir1
    for f1 in "${dir1}"/*_resource_summary.csv; do
        [ -f "$f1" ] || continue
        local base
        base=$(basename "$f1")
        local f2="${dir2}/${base}"
        local prefix="${base%_resource_summary.csv}"

        echo "--- ${prefix} ---"
        echo ""
        compare_resource_summaries "$f1" "$f2" "$label1" "$label2"
        echo ""
        found=1
    done

    # Check dir2 for files not in dir1
    for f2 in "${dir2}"/*_resource_summary.csv; do
        [ -f "$f2" ] || continue
        local base
        base=$(basename "$f2")
        local f1="${dir1}/${base}"
        [ -f "$f1" ] && continue  # already handled
        local prefix="${base%_resource_summary.csv}"

        echo "--- ${prefix} ---"
        echo ""
        compare_resource_summaries "$f1" "$f2" "$label1" "$label2"
        echo ""
        found=1
    done

    if [ $found -eq 0 ]; then
        echo "(No resource utilization data found. Re-run benchmarks to collect.)"
        echo ""
    fi
}

# ============================================================================

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

        echo ""
        compare_all_resource_summaries "$INNODB_DIR" "$MYROCKS_DIR" "$engine1" "$engine2"

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

        echo ""
        compare_all_resource_summaries "$INNODB_DIR" "$MYROCKS_DIR" "$engine1" "$engine2"

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

        echo ""
        compare_all_resource_summaries "$INNODB_DIR" "$MYROCKS_DIR" "$engine1" "$engine2"

    } | tee "${COMPARISON_DIR}/comparison_report.txt"
}

compare_clickbench() {
    local innodb_csv="${INNODB_DIR}/clickbench_summary.csv"
    local myrocks_csv="${MYROCKS_DIR}/clickbench_summary.csv"
    local innodb_size="${INNODB_DIR}/clickbench_size_metrics.csv"
    local myrocks_size="${MYROCKS_DIR}/clickbench_size_metrics.csv"

    if [ ! -f "$innodb_csv" ] || [ ! -f "$myrocks_csv" ]; then
        log_error "ClickBench summary CSV files not found"
        exit 1
    fi

    # Merge results
    {
        echo "query_num,innodb_cold,innodb_warm1,innodb_warm2,innodb_min,innodb_status,myrocks_cold,myrocks_warm1,myrocks_warm2,myrocks_min,myrocks_status"
        paste -d',' <(tail -n +2 "$innodb_csv") <(tail -n +2 "$myrocks_csv" | cut -d',' -f2-)
    } > "${COMPARISON_DIR}/merged_results.csv"

    # Create comparison report
    {
        echo "=========================================="
        echo "ClickBench Performance Comparison"
        echo "InnoDB vs MyRocks"
        echo "Date: $(date)"
        echo "=========================================="
        echo ""
        echo "[1] InnoDB: $INNODB_DIR"
        echo "[2] MyRocks: $MYROCKS_DIR"
        echo ""

        # Database size comparison
        echo "==================== Database Size Comparison ===================="
        echo ""
        if [ -f "$innodb_size" ] && [ -f "$myrocks_size" ]; then
            innodb_total_gb=$(awk -F',' 'NR==2 {print $5}' "$innodb_size")
            myrocks_total_gb=$(awk -F',' 'NR==2 {print $5}' "$myrocks_size")
            innodb_data_gb=$(awk -F',' 'NR==2 {print $3}' "$innodb_size")
            myrocks_data_gb=$(awk -F',' 'NR==2 {print $3}' "$myrocks_size")

            printf "%-20s %12s %12s %12s\n" "Metric" "InnoDB" "MyRocks" "Ratio"
            echo "------------------------------------------------------------"
            if [ -n "$innodb_data_gb" ] && [ -n "$myrocks_data_gb" ]; then
                ratio=$(awk "BEGIN {printf \"%.2f\", $innodb_data_gb / $myrocks_data_gb}")
                printf "%-20s %10.2f GB %10.2f GB %10.2fx\n" "Data Size" "$innodb_data_gb" "$myrocks_data_gb" "$ratio"
            fi
            if [ -n "$innodb_total_gb" ] && [ -n "$myrocks_total_gb" ]; then
                ratio=$(awk "BEGIN {printf \"%.2f\", $innodb_total_gb / $myrocks_total_gb}")
                printf "%-20s %10.2f GB %10.2f GB %10.2fx\n" "Total Size" "$innodb_total_gb" "$myrocks_total_gb" "$ratio"
            fi
        else
            echo "(Size metrics not available - run benchmark again to collect)"
        fi
        echo ""

        echo "==================== Per-Query Best Time Comparison ===================="
        echo ""
        printf "%-10s %12s %12s %10s\n" "Query" "InnoDB (s)" "MyRocks (s)" "Speedup"
        echo "-----------------------------------------------"

        # Compare min times for each query
        awk -F',' 'NR>1 {
            q = $1
            innodb_min = $5
            myrocks_min = $10
            if (innodb_min != "N/A" && myrocks_min != "N/A" && innodb_min > 0) {
                speedup = innodb_min / myrocks_min
                printf "%-10s %12.3f %12.3f %9.2fx\n", q, innodb_min, myrocks_min, speedup
            } else {
                printf "%-10s %12s %12s %10s\n", q, innodb_min, myrocks_min, "N/A"
            }
        }' "${COMPARISON_DIR}/merged_results.csv"

        echo ""
        echo "==================== Performance Summary ===================="
        echo ""

        # Calculate totals and geometric mean (only over queries both engines completed)
        awk -F',' 'NR>1 {
            innodb_ok = ($5 != "N/A" && $5 > 0)
            myrocks_ok = ($10 != "N/A" && $10 > 0)

            if (innodb_ok) innodb_total += $5
            if (myrocks_ok) myrocks_total += $10

            # Geometric mean: only count queries completed by BOTH engines
            if (innodb_ok && myrocks_ok) {
                both_innodb_log += log($5)
                both_myrocks_log += log($10)
                both_count++
            }
        }
        END {
            printf "Total best time (InnoDB):  %.2f seconds\n", innodb_total
            printf "Total best time (MyRocks): %.2f seconds\n", myrocks_total
            if (both_count > 0) {
                innodb_geomean = exp(both_innodb_log / both_count)
                myrocks_geomean = exp(both_myrocks_log / both_count)
                printf "Geometric mean (InnoDB):   %.3f seconds (%d queries)\n", innodb_geomean, both_count
                printf "Geometric mean (MyRocks):  %.3f seconds (%d queries)\n", myrocks_geomean, both_count
                printf "Overall speedup (geomean): %.2fx\n", innodb_geomean / myrocks_geomean
            }
        }' "${COMPARISON_DIR}/merged_results.csv"

        echo ""
        compare_all_resource_summaries "$INNODB_DIR" "$MYROCKS_DIR" "InnoDB" "MyRocks"

    } | tee "${COMPARISON_DIR}/comparison_report.txt"
}

compare_tpch_olap() {
    local innodb_csv="${INNODB_DIR}/tpch_summary.csv"
    local myrocks_csv="${MYROCKS_DIR}/tpch_summary.csv"
    local innodb_size="${INNODB_DIR}/tpch_size_metrics.csv"
    local myrocks_size="${MYROCKS_DIR}/tpch_size_metrics.csv"

    if [ ! -f "$innodb_csv" ] || [ ! -f "$myrocks_csv" ]; then
        log_error "TPC-H summary CSV files not found"
        exit 1
    fi

    # Merge results
    {
        echo "query_num,innodb_cold,innodb_warm1,innodb_warm2,innodb_min,innodb_status,myrocks_cold,myrocks_warm1,myrocks_warm2,myrocks_min,myrocks_status"
        paste -d',' <(tail -n +2 "$innodb_csv") <(tail -n +2 "$myrocks_csv" | cut -d',' -f2-)
    } > "${COMPARISON_DIR}/merged_results.csv"

    # Create comparison report
    {
        echo "=========================================="
        echo "TPC-H Performance Comparison"
        echo "InnoDB vs MyRocks"
        echo "Date: $(date)"
        echo "=========================================="
        echo ""
        echo "[1] InnoDB: $INNODB_DIR"
        echo "[2] MyRocks: $MYROCKS_DIR"
        echo ""

        # Database size comparison
        echo "==================== Database Size Comparison ===================="
        echo ""
        if [ -f "$innodb_size" ] && [ -f "$myrocks_size" ]; then
            innodb_sf=$(awk -F',' 'NR==2 {print $2}' "$innodb_size")
            innodb_total_gb=$(awk -F',' 'NR==2 {print $6}' "$innodb_size")
            myrocks_total_gb=$(awk -F',' 'NR==2 {print $6}' "$myrocks_size")
            innodb_data_gb=$(awk -F',' 'NR==2 {print $4}' "$innodb_size")
            myrocks_data_gb=$(awk -F',' 'NR==2 {print $4}' "$myrocks_size")

            echo "Scale Factor: ${innodb_sf}"
            echo ""
            printf "%-20s %12s %12s %12s\n" "Metric" "InnoDB" "MyRocks" "Ratio"
            echo "------------------------------------------------------------"
            if [ -n "$innodb_data_gb" ] && [ -n "$myrocks_data_gb" ]; then
                ratio=$(awk "BEGIN {printf \"%.2f\", $innodb_data_gb / $myrocks_data_gb}")
                printf "%-20s %10.2f GB %10.2f GB %10.2fx\n" "Data Size" "$innodb_data_gb" "$myrocks_data_gb" "$ratio"
            fi
            if [ -n "$innodb_total_gb" ] && [ -n "$myrocks_total_gb" ]; then
                ratio=$(awk "BEGIN {printf \"%.2f\", $innodb_total_gb / $myrocks_total_gb}")
                printf "%-20s %10.2f GB %10.2f GB %10.2fx\n" "Total Size" "$innodb_total_gb" "$myrocks_total_gb" "$ratio"
                compression=$(awk "BEGIN {printf \"%.2f\", $innodb_total_gb / $myrocks_total_gb}")
                echo ""
                echo "MyRocks compression ratio: ${compression}x smaller than InnoDB"
            fi
        else
            echo "(Size metrics not available - run benchmark again to collect)"
        fi
        echo ""

        echo "==================== Per-Query Best Time Comparison ===================="
        echo ""
        printf "%-10s %12s %12s %10s\n" "Query" "InnoDB (s)" "MyRocks (s)" "Speedup"
        echo "-----------------------------------------------"

        # Compare min times for each query
        awk -F',' 'NR>1 {
            q = $1
            innodb_min = $5
            myrocks_min = $10
            if (innodb_min != "N/A" && myrocks_min != "N/A" && innodb_min > 0) {
                speedup = innodb_min / myrocks_min
                printf "%-10s %12.3f %12.3f %9.2fx\n", q, innodb_min, myrocks_min, speedup
            } else {
                printf "%-10s %12s %12s %10s\n", q, innodb_min, myrocks_min, "N/A"
            }
        }' "${COMPARISON_DIR}/merged_results.csv"

        echo ""
        echo "==================== Performance Summary ===================="
        echo ""

        # Calculate totals and geometric mean (only over queries both engines completed)
        awk -F',' 'NR>1 {
            innodb_ok = ($5 != "N/A" && $5 > 0)
            myrocks_ok = ($10 != "N/A" && $10 > 0)

            if (innodb_ok) innodb_total += $5
            if (myrocks_ok) myrocks_total += $10

            # Geometric mean: only count queries completed by BOTH engines
            if (innodb_ok && myrocks_ok) {
                both_innodb_log += log($5)
                both_myrocks_log += log($10)
                both_count++
            }
        }
        END {
            printf "Total best time (InnoDB):  %.2f seconds\n", innodb_total
            printf "Total best time (MyRocks): %.2f seconds\n", myrocks_total
            if (both_count > 0) {
                innodb_geomean = exp(both_innodb_log / both_count)
                myrocks_geomean = exp(both_myrocks_log / both_count)
                printf "Geometric mean (InnoDB):   %.3f seconds (%d queries)\n", innodb_geomean, both_count
                printf "Geometric mean (MyRocks):  %.3f seconds (%d queries)\n", myrocks_geomean, both_count
                printf "Overall speedup (geomean): %.2fx\n", innodb_geomean / myrocks_geomean
            }
        }' "${COMPARISON_DIR}/merged_results.csv"

        echo ""
        compare_all_resource_summaries "$INNODB_DIR" "$MYROCKS_DIR" "InnoDB" "MyRocks"

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
    clickbench)
        compare_clickbench
        ;;
    tpch-olap)
        compare_tpch_olap
        ;;
    *)
        log_error "Unknown benchmark: $BENCHMARK"
        usage
        ;;
esac

log_info "Comparison completed successfully!"
log_info "Results saved to: $COMPARISON_DIR"
