#!/bin/bash
# HTAP profiling runner for percona-myrocks-nvmevirt inside the FLAX/NVMeVirt
# QEMU sandbox guest. Scaled-down first pass (see env-flax-sandbox.sh) --
# full scale is reserved for the eventual bare-metal s1 run.
#
# Usage:
#   bash run-flax-sandbox-htap.sh [--skip-prepare] [--cutoff <n>]
#
# Examples:
#   bash run-flax-sandbox-htap.sh                  # prepare data + run profiling
#   bash run-flax-sandbox-htap.sh --skip-prepare   # skip data prep (data already loaded)
#   bash run-flax-sandbox-htap.sh --cutoff 3000

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Point env.sh to sandbox overrides — exported so all subprocess scripts inherit it
export FLAX_SANDBOX_ENV="${SCRIPT_DIR}/../common/config/env-flax-sandbox.sh"
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
log_info "HTAP Profiling — FLAX sandbox (percona-myrocks-nvmevirt)"
log_info "=========================================="
log_info "Socket       : ${MYSQL_SOCKET_PERCONA_MYROCKS_NVMEVIRT}"
log_info "Datadir      : ${MYSQL_DATADIR_PERCONA_MYROCKS_NVMEVIRT}"
log_info "FlameGraph   : ${FLAMEGRAPH_DIR}"
log_info "Results      : ${RESULTS_DIR}"
log_info "Cutoff       : ${CUTOFF}"
log_info "Table size   : ${HTAP_TABLE_SIZE} (scaled down for sandbox)"
log_info "Skip prepare : ${SKIP_PREPARE}"
log_info "=========================================="

mkdir -p "${RESULTS_DIR}"

# NVMeVirt-backed mount sanity check. Unlike CEMU's FDMFS pool (safe to
# recreate every run via touch/fallocate on existing files), remounting
# NVMeVirt means guest-phase4-mvcc-filter.sh's full rmmod/reload +
# `mkfs.ext4 -F` chain, which unconditionally wipes /mnt/nvme -- this script
# never does that automatically. Fail loudly instead of guessing.
if ! mountpoint -q /mnt/nvme; then
    log_error "/mnt/nvme is not mounted. Run guest-phase4-mvcc-filter.sh first"
    log_error "(that script reformats the device -- do not run it if you have data there you want to keep)."
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
    bash "${SCRIPT_DIR}/../sysbench-htap/prepare.sh" percona-myrocks-nvmevirt || { bash "${SCRIPT_DIR}/mysql-control.sh" percona-myrocks-nvmevirt stop; exit 1; }
    bash "${SCRIPT_DIR}/mysql-control.sh" percona-myrocks-nvmevirt stop
    log_info "Data preparation complete."
else
    log_info "Phase 1: Skipping data preparation (--skip-prepare)"
fi

# Phase 2: HTAP profiling (no cgexec — sandbox has no cgroup setup, and the
# workload is already scaled down so memory pressure isn't a concern)
log_info "Phase 2: Running HTAP profiling..."
bash "${SCRIPT_DIR}/../profiling/profile-htap.sh" "$CUTOFF" "" percona-myrocks-nvmevirt

log_info "=========================================="
log_info "FLAX sandbox HTAP profiling complete"
log_info "Results : ${RESULTS_DIR}/profiling/htap/percona-myrocks-nvmevirt/"
log_info "=========================================="
