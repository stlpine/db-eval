#!/bin/bash
# Kill all running benchmark processes

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"

usage() {
    cat << EOF
Usage: $0 [options]

Options:
    -a, --all         Kill all benchmark processes and MySQL instances
    -b, --benchmark   Kill only benchmark processes (sysbench, tpcc, dbgen)
    -m, --mysql       Kill only MySQL instances
    -n, --monitoring  Kill only monitoring processes (pidstat, iostat, mpstat, vmstat)
    -h, --help        Show this help message

Without options, kills all benchmark processes and MySQL instances (same as --all).
EOF
    exit 1
}

# Default: kill everything
KILL_BENCHMARK=true
KILL_MYSQL=true
KILL_MONITORING=true

if [ $# -gt 0 ]; then
    KILL_BENCHMARK=false
    KILL_MYSQL=false
    KILL_MONITORING=false

    while [[ $# -gt 0 ]]; do
        case $1 in
            -a|--all)
                KILL_BENCHMARK=true
                KILL_MYSQL=true
                KILL_MONITORING=true
                shift
                ;;
            -b|--benchmark)
                KILL_BENCHMARK=true
                shift
                ;;
            -m|--mysql)
                KILL_MYSQL=true
                shift
                ;;
            -n|--monitoring)
                KILL_MONITORING=true
                shift
                ;;
            -h|--help)
                usage
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                ;;
        esac
    done
fi

kill_processes() {
    local pattern=$1
    local name=$2

    pids=$(pgrep -f "$pattern" 2>/dev/null)
    if [ -n "$pids" ]; then
        log_info "Killing $name processes: $pids"
        echo "$pids" | xargs kill -9 2>/dev/null || true
        return 0
    else
        log_info "No $name processes found"
        return 1
    fi
}

log_info "=========================================="
log_info "Benchmark Process Killer"
log_info "=========================================="

# Kill benchmark processes
if [ "$KILL_BENCHMARK" = true ]; then
    log_info ""
    log_info "Stopping benchmark processes..."

    # Sysbench (OLTP)
    kill_processes "sysbench" "sysbench"

    # TPC-C (tpcc_start and tpcc_load)
    kill_processes "tpcc_start" "tpcc_start"
    kill_processes "tpcc_load" "tpcc_load"

    # TPC-H dbgen (data generation)
    kill_processes "dbgen" "dbgen"
fi

# Kill monitoring processes
if [ "$KILL_MONITORING" = true ]; then
    log_info ""
    log_info "Stopping monitoring processes..."

    kill_processes "pidstat" "pidstat"
    kill_processes "iostat" "iostat"
    kill_processes "mpstat" "mpstat"
    kill_processes "vmstat" "vmstat"
fi

# Kill MySQL instances
if [ "$KILL_MYSQL" = true ]; then
    log_info ""
    log_info "Stopping MySQL instances..."

    # Try graceful shutdown first via mysql-control.sh
    for engine in vanilla-innodb percona-innodb percona-myrocks; do
        case $engine in
            vanilla-innodb)
                PID_FILE="${MYSQL_PID_VANILLA_INNODB}"
                ;;
            percona-innodb)
                PID_FILE="${MYSQL_PID_PERCONA_INNODB}"
                ;;
            percona-myrocks)
                PID_FILE="${MYSQL_PID_PERCONA_MYROCKS}"
                ;;
        esac

        if [ -f "$PID_FILE" ]; then
            pid=$(cat "$PID_FILE" 2>/dev/null)
            if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
                log_info "Stopping MySQL ($engine) with PID $pid..."
                kill "$pid" 2>/dev/null || true
                sleep 2
                # Force kill if still running
                if kill -0 "$pid" 2>/dev/null; then
                    log_info "Force killing MySQL ($engine)..."
                    kill -9 "$pid" 2>/dev/null || true
                fi
            fi
            rm -f "$PID_FILE"
        fi
    done

    # Kill any remaining mysqld processes
    kill_processes "mysqld" "mysqld (remaining)"
fi

log_info ""
log_info "=========================================="
log_info "Cleanup complete"
log_info "=========================================="

# Show remaining processes if any
remaining=$(pgrep -f "sysbench|tpcc_|dbgen|mysqld|pidstat|iostat|mpstat|vmstat" 2>/dev/null | wc -l)
if [ "$remaining" -gt 0 ]; then
    log_info ""
    log_info "Warning: Some processes may still be running:"
    pgrep -af "sysbench|tpcc_|dbgen|mysqld|pidstat|iostat|mpstat|vmstat" 2>/dev/null || true
fi
