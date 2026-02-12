#!/bin/bash
# Common monitoring functions for benchmark scripts
# Provides start/stop for CPU, I/O, and memory monitoring
# and post-run summary generation into a structured CSV.
#
# Monitoring tools:
#   pidstat -u -r -d 1  — per-process CPU, memory, disk I/O (1s interval)
#   iostat -x 1         — extended device I/O stats (1s interval)
#   mpstat -P ALL 1     — per-CPU core utilization (1s interval)
#   vmstat 1            — system-wide memory, swap, I/O, CPU (1s interval)
#
# Target: Ubuntu 22.04 x86_64 with sysstat package

# Global variable to track monitor PIDs
MONITOR_PIDS=""

# Get SSD device basename for iostat filtering
_get_ssd_dev_name() {
    if [ -n "$SSD_DEVICE" ]; then
        basename "$SSD_DEVICE"
    else
        echo ""
    fi
}

# Start all monitoring processes
# Usage: start_monitors <output_dir> <prefix>
# Creates: <prefix>_{pidstat,iostat,mpstat,vmstat}.txt
start_monitors() {
    local output_dir="$1"
    local prefix="$2"
    MONITOR_PIDS=""

    # pidstat: per-process CPU, memory, disk I/O every 1 second
    if command -v pidstat &>/dev/null; then
        pidstat -u -r -d 1 > "${output_dir}/${prefix}_pidstat.txt" 2>&1 &
        MONITOR_PIDS="$MONITOR_PIDS $!"
    fi

    # iostat: extended device I/O stats every 1 second
    if command -v iostat &>/dev/null; then
        iostat -x 1 > "${output_dir}/${prefix}_iostat.txt" 2>&1 &
        MONITOR_PIDS="$MONITOR_PIDS $!"
    fi

    # mpstat: per-CPU core utilization every 1 second
    if command -v mpstat &>/dev/null; then
        mpstat -P ALL 1 > "${output_dir}/${prefix}_mpstat.txt" 2>&1 &
        MONITOR_PIDS="$MONITOR_PIDS $!"
    fi

    # vmstat: system-wide memory, swap, I/O, CPU every 1 second
    if command -v vmstat &>/dev/null; then
        vmstat 1 > "${output_dir}/${prefix}_vmstat.txt" 2>&1 &
        MONITOR_PIDS="$MONITOR_PIDS $!"
    fi

    MONITOR_PIDS=$(echo "$MONITOR_PIDS" | xargs)  # trim whitespace
}

# Stop all monitoring processes
stop_monitors() {
    if [ -n "$MONITOR_PIDS" ]; then
        kill $MONITOR_PIDS 2>/dev/null || true
        wait $MONITOR_PIDS 2>/dev/null || true
        MONITOR_PIDS=""
    fi
}

# Cleanup handler for trap
cleanup_monitors() {
    stop_monitors
}

# Generate resource utilization summary CSV from monitoring output files
# Usage: generate_resource_summary <output_dir> <prefix>
# Creates: <prefix>_resource_summary.csv
generate_resource_summary() {
    local output_dir="$1"
    local prefix="$2"
    local summary_file="${output_dir}/${prefix}_resource_summary.csv"
    local ssd_dev
    ssd_dev=$(_get_ssd_dev_name)

    {
        echo "category,metric,avg,min,max,stddev"

        # === Parse mpstat for overall CPU utilization ===
        # Ubuntu 22.04 sysstat mpstat -P ALL format (24h time, no AM/PM):
        #   HH:MM:SS  CPU  %usr  %nice  %sys  %iowait  %irq  %soft  %steal  %guest  %gnice  %idle
        local mpstat_file="${output_dir}/${prefix}_mpstat.txt"
        if [ -f "$mpstat_file" ]; then
            awk '
            / all / && !/Average/ && !/CPU/ && !/^Linux/ {
                all_count++
                if (all_count <= 1) next  # skip first interval (since-boot average)
                for (i = 1; i <= NF; i++) {
                    if ($i == "all") { pos = i; break }
                }
                n++
                usr = $(pos+1)+0; sys = $(pos+3)+0; iowait = $(pos+4)+0; idle = $NF+0

                sum_usr += usr; sum_sys += sys; sum_iow += iowait; sum_idle += idle
                sumsq_usr += usr*usr; sumsq_sys += sys*sys
                sumsq_iow += iowait*iowait; sumsq_idle += idle*idle

                if (n==1 || usr < min_usr) min_usr = usr
                if (n==1 || usr > max_usr) max_usr = usr
                if (n==1 || sys < min_sys) min_sys = sys
                if (n==1 || sys > max_sys) max_sys = sys
                if (n==1 || iowait < min_iow) min_iow = iowait
                if (n==1 || iowait > max_iow) max_iow = iowait
                if (n==1 || idle < min_idle) min_idle = idle
                if (n==1 || idle > max_idle) max_idle = idle
            }
            function sd(sumsq, sum, n) {
                if (n < 2) return 0
                v = sumsq/n - (sum/n)^2
                return (v > 0) ? sqrt(v) : 0
            }
            END {
                if (n > 0) {
                    printf "cpu,user_pct,%.2f,%.2f,%.2f,%.2f\n", sum_usr/n, min_usr, max_usr, sd(sumsq_usr,sum_usr,n)
                    printf "cpu,system_pct,%.2f,%.2f,%.2f,%.2f\n", sum_sys/n, min_sys, max_sys, sd(sumsq_sys,sum_sys,n)
                    printf "cpu,iowait_pct,%.2f,%.2f,%.2f,%.2f\n", sum_iow/n, min_iow, max_iow, sd(sumsq_iow,sum_iow,n)
                    printf "cpu,idle_pct,%.2f,%.2f,%.2f,%.2f\n", sum_idle/n, min_idle, max_idle, sd(sumsq_idle,sum_idle,n)
                }
            }
            ' "$mpstat_file"
        fi

        # === Parse iostat for SSD I/O utilization ===
        # Ubuntu 22.04 sysstat iostat -x format:
        #   Device  r/s  rkB/s  rrqm/s  %rrqm  r_await  rareq-sz  w/s  wkB/s  ...  aqu-sz  %util
        local iostat_file="${output_dir}/${prefix}_iostat.txt"
        if [ -f "$iostat_file" ] && [ -n "$ssd_dev" ]; then
            awk -v dev="$ssd_dev" '
            /^Device/ {
                interval++
                for (i = 1; i <= NF; i++) {
                    if ($i == "r/s") col_rs = i
                    else if ($i == "rkB/s") col_rkb = i
                    else if ($i == "r_await") col_rawait = i
                    else if ($i == "w/s") col_ws = i
                    else if ($i == "wkB/s") col_wkb = i
                    else if ($i == "w_await") col_wawait = i
                    else if ($i == "aqu-sz") col_aqu = i
                    else if ($i == "%util") col_util = i
                }
                next
            }
            $1 == dev && interval > 1 && col_rs > 0 {
                n++
                rs = $col_rs+0; rkb = $col_rkb+0; rawait = $col_rawait+0
                ws = $col_ws+0; wkb = $col_wkb+0; wawait = $col_wawait+0
                aqu = $col_aqu+0; util = $col_util+0

                sum_rs += rs; sum_rkb += rkb; sum_rawait += rawait
                sum_ws += ws; sum_wkb += wkb; sum_wawait += wawait
                sum_aqu += aqu; sum_util += util

                sumsq_rs += rs*rs; sumsq_rkb += rkb*rkb; sumsq_rawait += rawait*rawait
                sumsq_ws += ws*ws; sumsq_wkb += wkb*wkb; sumsq_wawait += wawait*wawait
                sumsq_aqu += aqu*aqu; sumsq_util += util*util

                if (n==1||rs<min_rs) min_rs=rs; if (n==1||rs>max_rs) max_rs=rs
                if (n==1||rkb<min_rkb) min_rkb=rkb; if (n==1||rkb>max_rkb) max_rkb=rkb
                if (n==1||rawait<min_rawait) min_rawait=rawait; if (n==1||rawait>max_rawait) max_rawait=rawait
                if (n==1||ws<min_ws) min_ws=ws; if (n==1||ws>max_ws) max_ws=ws
                if (n==1||wkb<min_wkb) min_wkb=wkb; if (n==1||wkb>max_wkb) max_wkb=wkb
                if (n==1||wawait<min_wawait) min_wawait=wawait; if (n==1||wawait>max_wawait) max_wawait=wawait
                if (n==1||aqu<min_aqu) min_aqu=aqu; if (n==1||aqu>max_aqu) max_aqu=aqu
                if (n==1||util<min_util) min_util=util; if (n==1||util>max_util) max_util=util
            }
            function sd(sq, s, n) {
                if (n < 2) return 0
                v = sq/n - (s/n)^2
                return (v > 0) ? sqrt(v) : 0
            }
            END {
                if (n > 0) {
                    printf "io,read_iops,%.2f,%.2f,%.2f,%.2f\n", sum_rs/n, min_rs, max_rs, sd(sumsq_rs,sum_rs,n)
                    printf "io,write_iops,%.2f,%.2f,%.2f,%.2f\n", sum_ws/n, min_ws, max_ws, sd(sumsq_ws,sum_ws,n)
                    printf "io,read_kbps,%.2f,%.2f,%.2f,%.2f\n", sum_rkb/n, min_rkb, max_rkb, sd(sumsq_rkb,sum_rkb,n)
                    printf "io,write_kbps,%.2f,%.2f,%.2f,%.2f\n", sum_wkb/n, min_wkb, max_wkb, sd(sumsq_wkb,sum_wkb,n)
                    printf "io,read_await_ms,%.2f,%.2f,%.2f,%.2f\n", sum_rawait/n, min_rawait, max_rawait, sd(sumsq_rawait,sum_rawait,n)
                    printf "io,write_await_ms,%.2f,%.2f,%.2f,%.2f\n", sum_wawait/n, min_wawait, max_wawait, sd(sumsq_wawait,sum_wawait,n)
                    printf "io,queue_depth,%.2f,%.2f,%.2f,%.2f\n", sum_aqu/n, min_aqu, max_aqu, sd(sumsq_aqu,sum_aqu,n)
                    printf "io,util_pct,%.2f,%.2f,%.2f,%.2f\n", sum_util/n, min_util, max_util, sd(sumsq_util,sum_util,n)
                }
            }
            ' "$iostat_file"
        fi

        # === Parse vmstat for memory/swap/system ===
        # Ubuntu 22.04 vmstat format:
        #  r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
        local vmstat_file="${output_dir}/${prefix}_vmstat.txt"
        if [ -f "$vmstat_file" ]; then
            awk '
            /^ *[0-9]/ {
                lines++
                if (lines <= 1) next  # skip first line (since-boot average)
                n++
                r = $1+0; b = $2+0
                free = $4+0; buff = $5+0; cache = $6+0
                si = $7+0; so = $8+0; bi = $9+0; bo = $10+0

                sum_r += r; sum_b += b
                sum_free += free; sum_buff += buff; sum_cache += cache
                sum_si += si; sum_so += so; sum_bi += bi; sum_bo += bo

                sumsq_r += r*r; sumsq_b += b*b

                if (n==1||r<min_r) min_r=r; if (n==1||r>max_r) max_r=r
                if (n==1||b<min_b) min_b=b; if (n==1||b>max_b) max_b=b
                if (n==1||free<min_free) min_free=free; if (n==1||free>max_free) max_free=free
                if (n==1||buff<min_buff) min_buff=buff; if (n==1||buff>max_buff) max_buff=buff
                if (n==1||cache<min_cache) min_cache=cache; if (n==1||cache>max_cache) max_cache=cache
                if (n==1||si<min_si) min_si=si; if (n==1||si>max_si) max_si=si
                if (n==1||so<min_so) min_so=so; if (n==1||so>max_so) max_so=so
                if (n==1||bi<min_bi) min_bi=bi; if (n==1||bi>max_bi) max_bi=bi
                if (n==1||bo<min_bo) min_bo=bo; if (n==1||bo>max_bo) max_bo=bo
            }
            function sd(sq, s, n) {
                if (n < 2) return 0
                v = sq/n - (s/n)^2
                return (v > 0) ? sqrt(v) : 0
            }
            END {
                if (n > 0) {
                    printf "sys,runqueue_avg,%.2f,%.2f,%.2f,%.2f\n", sum_r/n, min_r, max_r, sd(sumsq_r,sum_r,n)
                    printf "sys,blocked_avg,%.2f,%.2f,%.2f,%.2f\n", sum_b/n, min_b, max_b, sd(sumsq_b,sum_b,n)
                    printf "mem,free_kb,%.0f,%.0f,%.0f,0\n", sum_free/n, min_free, max_free
                    printf "mem,buffer_kb,%.0f,%.0f,%.0f,0\n", sum_buff/n, min_buff, max_buff
                    printf "mem,cache_kb,%.0f,%.0f,%.0f,0\n", sum_cache/n, min_cache, max_cache
                    printf "mem,swap_in_ps,%.2f,%.0f,%.0f,0\n", sum_si/n, min_si, max_si
                    printf "mem,swap_out_ps,%.2f,%.0f,%.0f,0\n", sum_so/n, min_so, max_so
                    printf "io,vmstat_bi_ps,%.2f,%.0f,%.0f,0\n", sum_bi/n, min_bi, max_bi
                    printf "io,vmstat_bo_ps,%.2f,%.0f,%.0f,0\n", sum_bo/n, min_bo, max_bo
                }
            }
            ' "$vmstat_file"
        fi

    } > "$summary_file"

    log_info "Resource summary: $summary_file"
}
