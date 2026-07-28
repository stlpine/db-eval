#!/bin/bash
# HTAP profiling runner for percona-myrocks-nvmevirt on the real bare-metal
# FLAX/NVMeVirt host (s1) -- full AIDE-paper scale, real nvmev.ko-backed
# device, no QEMU nesting. See run-flax-sandbox-htap.sh for the QEMU-guest
# equivalent (scaled-down first pass this bare-metal run supersedes).
#
# Usage:
#   bash run-flax-baremetal-htap.sh [--skip-prepare] [--cutoff <n>]
#
# Examples:
#   bash run-flax-baremetal-htap.sh                  # prepare data + run profiling
#   bash run-flax-baremetal-htap.sh --skip-prepare   # skip data prep (data already loaded)
#   bash run-flax-baremetal-htap.sh --cutoff 90000

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Point env.sh to bare-metal overrides — exported so all subprocess scripts inherit it
export FLAX_BAREMETAL_ENV="${SCRIPT_DIR}/../common/config/env-flax-baremetal.sh"
source "${SCRIPT_DIR}/../common/config/env.sh"

SKIP_PREPARE=false
CUTOFF="${HTAP_JOIN_CUTOFF}"

while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-prepare) SKIP_PREPARE=true; shift ;;
        --cutoff) CUTOFF="$2"; shift 2 ;;
        *) log_error "Unknown option: $1"; exit 1 ;;
    esac
done

log_info "=========================================="
log_info "HTAP Profiling — FLAX bare metal (percona-myrocks-nvmevirt)"
log_info "=========================================="
log_info "Socket       : ${MYSQL_SOCKET_PERCONA_MYROCKS_NVMEVIRT}"
log_info "Datadir      : ${MYSQL_DATADIR_PERCONA_MYROCKS_NVMEVIRT}"
log_info "FlameGraph   : ${FLAMEGRAPH_DIR}"
log_info "Results      : ${RESULTS_DIR}"
log_info "Cutoff       : ${CUTOFF}"
log_info "Table size   : ${HTAP_TABLE_SIZE} (full AIDE-paper scale)"
log_info "OLTP threads : ${HTAP_OLTP_THREADS}"
log_info "LLT count    : ${HTAP_LLT_COUNT}"
log_info "Query timeout: ${HTAP_QUERY_TIMEOUT}s"
log_info "Skip prepare : ${SKIP_PREPARE}"
log_info "=========================================="

mkdir -p "${RESULTS_DIR}"

# Real safety check this time (env-flax-baremetal.sh's check_ssd_device/
# check_ssd_mount actually verify /mnt/nvme is backed by the NVMeVirt
# device specifically, not a blind no-op like the sandbox's -- see that
# file's own comment for why this matters on a host with multiple physical
# NVMe devices whose naming has already been observed reshuffling).
if ! check_ssd_device || ! check_ssd_mount; then
    log_error "NVMeVirt device/mount check failed -- see above. Refusing to continue."
    exit 1
fi

# Phase 1: Data preparation
if [ "$SKIP_PREPARE" = false ]; then
    log_info "Phase 1: Preparing sysbench-htap data..."
    if [ ! -d "${MYSQL_DATADIR_PERCONA_MYROCKS_NVMEVIRT}" ]; then
        log_info "Datadir missing — initializing..."
        bash "${SCRIPT_DIR}/mysql-control.sh" percona-myrocks-nvmevirt init || { log_error "Failed to init datadir"; exit 1; }
    fi
    bash "${SCRIPT_DIR}/mysql-control.sh" percona-myrocks-nvmevirt start || { log_error "Failed to start mysqld for data prep"; exit 1; }
    # prepare.sh's own internal `mysql` calls have no explicit -u, so they
    # default to the MySQL client's OS-username-as-MySQL-username fallback.
    # --initialize-insecure only creates a passwordless root@localhost --
    # running as the plain invoking user (not root) fails with "Access
    # denied" before ever reaching the sysbench prepare step (confirmed
    # 2026-07-28). sudo the whole script so it inherits root, matching how
    # mysqld itself and every other FLAX-path command in this harness
    # already runs as root.
    # -E: sudo resets the environment by default, which would drop the
    # SOCKET/BENCHMARK_DB/HTAP_*/etc. variables env.sh already exported in
    # this shell -- prepare.sh needs those, not just root's identity.
    sudo -E bash "${SCRIPT_DIR}/../sysbench-htap/prepare.sh" percona-myrocks-nvmevirt || { bash "${SCRIPT_DIR}/mysql-control.sh" percona-myrocks-nvmevirt stop; exit 1; }
    bash "${SCRIPT_DIR}/mysql-control.sh" percona-myrocks-nvmevirt stop
    log_info "Data preparation complete."
else
    log_info "Phase 1: Skipping data preparation (--skip-prepare)"
fi

# Phase 2: HTAP profiling
log_info "Phase 2: Running HTAP profiling..."
bash "${SCRIPT_DIR}/../profiling/profile-htap.sh" "$CUTOFF" "" percona-myrocks-nvmevirt

log_info "=========================================="
log_info "FLAX bare-metal HTAP profiling complete"
log_info "Results : ${RESULTS_DIR}/profiling/htap/percona-myrocks-nvmevirt/"
log_info "=========================================="
