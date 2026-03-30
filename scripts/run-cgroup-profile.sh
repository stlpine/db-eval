#!/bin/bash
# Run MyRocks profiling with cgroup memory limit
#
# Mirrors run-cgroup.sh structure exactly:
#   Phase 1 (optional): prepare-data.sh WITHOUT cgroup  (SSD reset + data load)
#   Phase 2:            profile-<type>.sh  WITH cgroup   (memory-limited profiling)
#
# Engine is always percona-myrocks (profiling is MyRocks-specific).
#
# Usage: $0 [options]
#
# Options:
#   -t, --type <type>       Profiling type: oltp | olap | sysbench | clickbench  (required)
#   -q, --queries <list>    olap: TPC-H query numbers (default: "1 6 12 19")
#                           clickbench: query numbers (default: "3 8 14 17")
#   --threads <n>           oltp/sysbench: thread count (default: 32)
#   --workload <name>       sysbench only: workload name (default: oltp_read_only)
#                           Options: oltp_read_only, oltp_read_write, oltp_write_only
#   --skip-prepare          Skip data preparation (data must already exist on SSD)
#   --full                  Force full data preparation (ignore backup)
#   -h, --help
#
# Examples:
#   $0 -t olap
#   $0 -t olap -q "1 6"
#   $0 -t oltp --threads 16
#   $0 -t sysbench
#   $0 -t sysbench --workload oltp_read_write --threads 16
#   $0 -t clickbench
#   $0 -t clickbench -q "3 8 14 17 34"
#   $0 -t olap --skip-prepare
#   $0 -t oltp --full

set -e
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"

CGROUP_PATH="/sys/fs/cgroup/${CGROUP_NAME}"
ENGINE="percona-myrocks"

usage() {
    cat << EOF
Usage: $0 [options]

Options:
    -t, --type <type>       Profiling type (required): oltp | olap | sysbench | clickbench
    -q, --queries <list>    olap: TPC-H query numbers, space-separated (default: "${PROFILING_OLAP_QUERIES}")
                            clickbench: query numbers, space-separated (default: "3 8 14 17")
    --threads <n>           oltp/sysbench: thread count (default: 32)
    --workload <name>       sysbench: workload name (default: oltp_read_only)
                            Options: oltp_read_only, oltp_read_write, oltp_write_only
    --skip-prepare          Skip data preparation (data must already exist)
    --full                  Force full data preparation (ignore backup)
    -h, --help

Examples:
    $0 -t olap
    $0 -t olap -q "1 6"
    $0 -t oltp --threads 16
    $0 -t sysbench
    $0 -t sysbench --workload oltp_read_write --threads 16
    $0 -t clickbench
    $0 -t clickbench -q "3 8 14 17 34"
    $0 -t olap --skip-prepare
EOF
    exit 1
}

# ── Argument parsing ──────────────────────────────────────────────────────────

TYPE=""
QUERIES=""
THREADS="32"
WORKLOAD="oltp_read_only"
SKIP_PREPARE=false
FORCE_FULL=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--type)
            TYPE="$2"
            shift 2
            ;;
        -q|--queries)
            QUERIES="$2"
            shift 2
            ;;
        --threads)
            THREADS="$2"
            shift 2
            ;;
        --workload)
            WORKLOAD="$2"
            shift 2
            ;;
        --skip-prepare)
            SKIP_PREPARE=true
            shift
            ;;
        --full)
            FORCE_FULL=true
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

if [ -z "$TYPE" ]; then
    log_error "Profiling type is required (-t oltp|olap|sysbench|clickbench)"
    usage
fi

case $TYPE in
    oltp)
        PREPARE_BENCHMARK="tpcc"
        ;;
    olap)
        PREPARE_BENCHMARK="tpch-olap"
        [ -z "$QUERIES" ] && QUERIES="${PROFILING_OLAP_QUERIES}"
        ;;
    sysbench)
        PREPARE_BENCHMARK="sysbench"
        ;;
    clickbench)
        PREPARE_BENCHMARK="clickbench"
        [ -z "$QUERIES" ] && QUERIES="3 8 14 17"
        ;;
    *)
        log_error "Invalid type: $TYPE (must be oltp, olap, sysbench, or clickbench)"
        usage
        ;;
esac

# ── Cgroup check (same as run-cgroup.sh) ─────────────────────────────────────

if [ ! -d "$CGROUP_PATH" ]; then
    log_error "Cgroup '${CGROUP_NAME}' does not exist"
    log_error "Run './scripts/setup-cgroup.sh' first"
    exit 1
fi

MEMORY_LIMIT=$(cat "${CGROUP_PATH}/memory.max" 2>/dev/null \
    | awk '{printf "%.2f GB", $1/1024/1024/1024}')

log_info "=========================================="
log_info "MyRocks Profiling with Cgroup Memory Limit"
log_info "=========================================="
log_info "Engine        : ${ENGINE}"
log_info "Type          : ${TYPE}"
case $TYPE in
    olap|clickbench)
        log_info "Queries       : ${QUERIES}"
        ;;
    sysbench)
        log_info "Workload      : ${WORKLOAD}"
        log_info "Threads       : ${THREADS}"
        ;;
    oltp)
        log_info "Threads       : ${THREADS}"
        ;;
esac
log_info "Cgroup        : ${CGROUP_NAME}"
log_info "Memory limit  : ${MEMORY_LIMIT}"
log_info "Warmup        : ${PROFILING_WARMUP_DURATION}s"
log_info "Record window : ${PROFILING_RECORD_DURATION}s"
log_info "Skip prepare  : ${SKIP_PREPARE}"
log_info "Force full    : ${FORCE_FULL}"
log_info "=========================================="
echo ""

START_TIME=$(date +%s)

# ── Phase 1: Data preparation WITHOUT cgroup (same as run-cgroup.sh) ─────────

if [ "$SKIP_PREPARE" = false ]; then
    log_info "Phase 1: Data Preparation for ${PREPARE_BENCHMARK} (no cgroup limit)"
    log_info "  - SSD will be reset for a clean state"
    echo ""

    PREPARE_ARGS=("-e" "$ENGINE" "-b" "$PREPARE_BENCHMARK")
    [ "$FORCE_FULL" = true ] && PREPARE_ARGS+=("--full")

    "${SCRIPT_DIR}/prepare-data.sh" "${PREPARE_ARGS[@]}"

    echo ""
    log_info "Data preparation completed"
    echo ""
else
    log_info "Phase 1: Skipping data preparation (--skip-prepare)"
    echo ""
fi

# ── Phase 2: Run profiling WITH cgroup (same pattern as run-cgroup.sh) ────────

log_info "Phase 2: Run ${TYPE} profiling (cgroup limit: ${MEMORY_LIMIT})"
echo ""

case $TYPE in
    olap)
        sudo -E cgexec -g memory:"${CGROUP_NAME}" \
            bash "${SCRIPT_DIR}/../profiling/profile-olap.sh" \
            "$QUERIES"
        ;;
    oltp)
        sudo -E cgexec -g memory:"${CGROUP_NAME}" \
            bash "${SCRIPT_DIR}/../profiling/profile-oltp.sh" \
            "$THREADS"
        ;;
    sysbench)
        sudo -E cgexec -g memory:"${CGROUP_NAME}" \
            bash "${SCRIPT_DIR}/../profiling/profile-sysbench.sh" \
            "$WORKLOAD" "$THREADS"
        ;;
    clickbench)
        sudo -E cgexec -g memory:"${CGROUP_NAME}" \
            bash "${SCRIPT_DIR}/../profiling/profile-clickbench.sh" \
            "$QUERIES"
        ;;
esac

echo ""

END_TIME=$(date +%s)
TOTAL_DURATION=$(( END_TIME - START_TIME ))

log_info "=========================================="
log_info "Profiling complete!"
log_info "Type     : ${TYPE}"
log_info "Duration : ${TOTAL_DURATION}s ($((TOTAL_DURATION / 60))m)"
log_info "Results  : ${RESULTS_DIR}/profiling/${TYPE}/"
log_info "=========================================="
