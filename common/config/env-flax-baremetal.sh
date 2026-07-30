#!/bin/bash
# Bare-metal overrides for percona-myrocks-nvmevirt HTAP profiling.
# Do not source directly -- set FLAX_BAREMETAL_ENV to this file's absolute
# path before running any db-eval script; env.sh sources it automatically
# at the end.
#
# Unlike env-flax-sandbox.sh, this does NOT scale down HTAP_TABLE_SIZE/
# HTAP_OLTP_THREADS/etc. -- env.sh's own defaults already ARE the full
# AIDE-paper-scale values (HTAP_TABLE_SIZE=100000, HTAP_OLTP_THREADS=24,
# HTAP_LLT_COUNT=4, HTAP_QUERY_TIMEOUT=7200), which is what this bare-metal
# pass is for. This file only overrides paths/binaries specific to this
# deployment and the real (non-emulated-in-QEMU) NVMeVirt device.
#
# Updated for a migration to a new bare-metal host (deliberate migration,
# not a same-host update -- an older CPU generation plus a permanent
# whole-NUMA-node core reservation for NVMeVirt's own dispatcher/IO/compute
# threads, present in both offload-ON and offload-OFF configs, left mysqld
# starved of cores on the original host relative to what a comparison host
# had). See CPU/NUMA topology notes in FLAX/src/init_nvmev.sh and perf.sh.

# --- Percona Server build tree (raw build dir, not an installed prefix) ---
export FLAX_PS_BUILD_DIR="$HOME/flax-scratch/repos/percona-server/build"
export PATH="${FLAX_PS_BUILD_DIR}/runtime_output_directory:${PATH}"

# Socket/pid/datadir for this profiling run.
export MYSQL_SOCKET_PERCONA_MYROCKS_NVMEVIRT="/tmp/mysql_nvmevirt_htap.sock"
export MYSQL_PID_PERCONA_MYROCKS_NVMEVIRT="/tmp/mysql_nvmevirt_htap.pid"
# Must live on the NVMeVirt-backed filesystem -- csdvirt_load_files derives
# physical LBAs via FIEMAP on the SST file; an SST on the wrong filesystem
# yields a bogus LBA into NVMeVirt's address space. /mnt/nvme confirmed as
# the real emulated device's mount point on this host too.
export MYSQL_DATADIR_PERCONA_MYROCKS_NVMEVIRT="/mnt/nvme/mysql-nvmevirt-htap-data"

# mysqld runs as root because csdvirt_init_dev() needs root for device-node
# access.
export MYSQL_DAEMON_USER="root"
export BENCH_SUDO="sudo"

# Fully generic already (no hardcoded paths/usernames, just __PLACEHOLDER__
# tokens substituted by mysql-control.sh at runtime) -- no per-host fork
# needed.
export MYSQL_NVMEVIRT_CONFIG_OVERRIDE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/my-percona-myrocks-nvmevirt-baremetal.cnf"

# FlameGraph and db-eval itself must be present on this host -- verify
# before running (not auto-cloned by this file):
#   git clone https://github.com/brendangregg/FlameGraph ~/FlameGraph
#   git clone <db-eval remote> ~/db-eval   (or wherever RESULTS_DIR below expects)
export FLAMEGRAPH_DIR="$HOME/FlameGraph"
export RESULTS_DIR="$HOME/db-eval/results"

# Real safety check, unlike the sandbox's blind no-op -- resolve the
# emulated device by its stable by-id alias (same one mount.sh itself uses,
# and the device always reports this same identity regardless of host) and
# verify /mnt/nvme is actually mounted FROM that device, not just mounted
# from *something*. Device numbering (/dev/nvmeXn1) is not stable across
# reboots -- confirmed on more than one host in this project now
# (project_flax_nvme_naming_instability memory).
check_ssd_device() {
    SSD_DEVICE="$(realpath /dev/disk/by-id/nvme-CSL_Virt_MN_01_CSL_Virt_SN_01 2>/dev/null)"
    if [ -z "$SSD_DEVICE" ]; then
        log_error "NVMeVirt emulated device (CSL_Virt_SN_01) not found -- is nvmev.ko loaded? (cd ~/flax-scratch/repos/FLAX/src && sudo ./init_nvmev.sh)"
        return 1
    fi
    export SSD_DEVICE
    return 0
}
check_ssd_mount() {
    local actual
    actual="$(findmnt -n -o SOURCE /mnt/nvme 2>/dev/null)"
    if [ -z "$actual" ]; then
        log_error "/mnt/nvme is not mounted at all"
        return 1
    fi
    # Self-sufficient: don't assume check_ssd_device already ran in this
    # process and populated $SSD_DEVICE -- some call sites (profile-htap.sh)
    # call check_ssd_mount alone, without check_ssd_device first (confirmed
    # 2026-07-29: SSD_DEVICE came back empty there, making every mount look
    # like the wrong one even when it wasn't). Resolve it here too if unset.
    if [ -z "${SSD_DEVICE:-}" ]; then
        check_ssd_device || return 1
    fi
    if [ "$actual" != "$SSD_DEVICE" ]; then
        log_error "/mnt/nvme is mounted from '$actual', but the NVMeVirt device resolved to '$SSD_DEVICE' -- wrong disk mounted, refusing to continue"
        return 1
    fi
    return 0
}
wait_for_mount_settle() { return 0; }
export SSD_MOUNT="/mnt/nvme"

# perf built from FLAX/linux/linux-6.0.10/tools/perf, installed to
# /usr/local/bin/perf -- confirmed real DWARF unwind support
# (libdw-dwarf-unwind: on) on this host. Bare metal, no QEMU nesting
# overhead, so dwarf is used directly (matches the CEMU thread's own
# bare-metal choice). Deliberately NOT fp here: fp silently truncates the
# stack the moment a sample lands in code not built with
# -fno-omit-frame-pointer (e.g. glibc/libstdc++ internals like
# std::mutex/std::lock_guard), which would specifically hide the mutex-wait
# call stacks this profiling round cares about (g_nvmevirt_exec_mutex
# contention). dwarf unwinds through that correctly, and this host's faster
# CPU means postprocessing time (the original reason fp was ever needed, in
# the resource-constrained QEMU sandbox) is not a concern here either.
export PERF_CALL_GRAPH="dwarf"

# This host's CPU is a server-class Xeon (no hybrid P/E-core PMU, which is
# specific to newer client Intel chips like the CEMU thread's i7-13700K) --
# same "Cannot find PMU 'cpu_core'" issue already hit and fixed once in this
# project, expected to apply here too for the same reason.
export PERF_EVENT="cycles"

# No disk-safety scale-down override here -- this host's emulated device has
# ~206GB usable capacity (confirmed via `df -h /mnt/nvme`), comfortably
# covering the calibrated growth estimate for a full-scale
# two-configuration profiling pass (~70-80GB, per the original bare-metal
# host's own disk-full incident calibration: ~0.13GB/hour/OLTP-thread).
# Full AIDE-paper-scale defaults from env.sh (HTAP_QUERY_TIMEOUT=7200,
# HTAP_OLAP_RUNS=5) apply directly -- removing this constraint was the
# whole point of migrating to this host.
