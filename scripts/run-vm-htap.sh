#!/bin/bash
# HTAP profiling runner for percona-myrocks-csd inside the QEMU VM.
# Replaces run-cgroup-profile.sh for VM use:
#   - No cgroup needed (VM itself is memory-limited to 16GB via -m 16G)
#   - No SSD setup (data lives on rootfs at /root/mysql-data)
#   - Sets CEMU_VM_ENV so all sourced scripts pick up VM path overrides
#
# Usage:
#   bash run-vm-htap.sh [--skip-prepare] [--cutoff <n>]
#
# Examples:
#   bash run-vm-htap.sh                        # prepare data + run profiling
#   bash run-vm-htap.sh --skip-prepare         # skip data prep (data already loaded)
#   bash run-vm-htap.sh --cutoff 10000

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Point env.sh to VM overrides — exported so all subprocess scripts inherit it
export CEMU_VM_ENV="${SCRIPT_DIR}/../common/config/env-vm-csd.sh"
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
log_info "HTAP Profiling — VM mode (percona-myrocks-csd)"
log_info "=========================================="
log_info "Socket       : ${MYSQL_SOCKET_PERCONA_MYROCKS_CSD}"
log_info "FlameGraph   : ${FLAMEGRAPH_DIR}"
log_info "Results      : ${RESULTS_DIR}"
log_info "Cutoff       : ${CUTOFF}"
log_info "Skip prepare : ${SKIP_PREPARE}"
log_info "=========================================="

mkdir -p "${RESULTS_DIR}"

# FDMFS pool setup — device memory is volatile; systemd auto-mounts FDMFS on boot
# but skips file creation, so pool files must be (re)created after every reboot.
if [[ ! -e /mnt/fdm0/0 ]]; then
    log_info "FDMFS pool missing — creating pool files..."
    for j in $(seq 0 31); do touch /mnt/fdm0/$j && fallocate -l 32m /mnt/fdm0/$j; done
    for j in $(seq 0 31); do touch /mnt/fdm1/$j && fallocate -l 32m /mnt/fdm1/$j; done
    log_info "FDMFS pool ready."
fi

# Phase 1: Data preparation
if [ "$SKIP_PREPARE" = false ]; then
    log_info "Phase 1: Preparing sysbench-htap data..."
    bash "${SCRIPT_DIR}/mysql-control.sh" percona-myrocks-csd start || { log_error "Failed to start mysqld for data prep"; exit 1; }
    bash "${SCRIPT_DIR}/../sysbench-htap/prepare.sh" percona-myrocks-csd || { bash "${SCRIPT_DIR}/mysql-control.sh" percona-myrocks-csd stop; exit 1; }
    bash "${SCRIPT_DIR}/mysql-control.sh" percona-myrocks-csd stop
    log_info "Data preparation complete."
else
    log_info "Phase 1: Skipping data preparation (--skip-prepare)"
fi

# Phase 2: HTAP profiling (no cgexec — VM provides memory isolation)
log_info "Phase 2: Running HTAP profiling..."
bash "${SCRIPT_DIR}/../profiling/profile-htap.sh" "$CUTOFF" "" percona-myrocks-csd

log_info "=========================================="
log_info "VM HTAP profiling complete"
log_info "Results : ${RESULTS_DIR}/profiling/htap/percona-myrocks-csd/"
log_info "=========================================="
