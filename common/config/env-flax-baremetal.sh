#!/bin/bash
# Bare-metal (s1) overrides for percona-myrocks-nvmevirt HTAP profiling.
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

# --- Percona Server build tree (raw build dir, not an installed prefix --
#     same pattern as the sandbox, just built directly on s1 instead of
#     inside the guest) ---
export FLAX_PS_BUILD_DIR="$HOME/flax-scratch/repos/percona-server/build"
export PATH="${FLAX_PS_BUILD_DIR}/runtime_output_directory:${PATH}"

# Socket/pid/datadir for this profiling run.
export MYSQL_SOCKET_PERCONA_MYROCKS_NVMEVIRT="/tmp/mysql_nvmevirt_htap.sock"
export MYSQL_PID_PERCONA_MYROCKS_NVMEVIRT="/tmp/mysql_nvmevirt_htap.pid"
# Must live on the NVMeVirt-backed filesystem -- csdvirt_load_files derives
# physical LBAs via FIEMAP on the SST file; an SST on the wrong filesystem
# yields a bogus LBA into NVMeVirt's address space. /mnt/nvme confirmed as
# the real emulated device's mount point (nvme3n1 -> CSL_Virt_SN_01,
# 2026-07-28 via init_nvmev.sh + mount.sh).
export MYSQL_DATADIR_PERCONA_MYROCKS_NVMEVIRT="/mnt/nvme/mysql-nvmevirt-htap-data"

# Same as the sandbox: mysqld runs as root because csdvirt_init_dev()
# needs root for device-node access.
export MYSQL_DAEMON_USER="root"
export BENCH_SUDO="sudo"

# Use the same .cnf content as the sandbox (it's already fully generic --
# no guest-specific paths baked in, just __PLACEHOLDER__ tokens substituted
# by mysql-control.sh at runtime) but under a bare-metal-named file, purely
# so result logs/configs are unambiguous about which environment produced
# them.
export MYSQL_NVMEVIRT_CONFIG_OVERRIDE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/my-percona-myrocks-nvmevirt-baremetal.cnf"

# FlameGraph and db-eval itself must be present on s1 -- verify before
# running (not auto-cloned by this file):
#   git clone https://github.com/brendangregg/FlameGraph ~/FlameGraph
#   git clone <db-eval remote> ~/db-eval   (or wherever RESULTS_DIR below expects)
export FLAMEGRAPH_DIR="$HOME/FlameGraph"
export RESULTS_DIR="$HOME/db-eval/results"

# Real safety check, unlike the sandbox's blind no-op -- s1 has multiple
# physical NVMe devices whose /dev/nvmeXn1 naming has already been observed
# reshuffling across reboots (project_flax_nvme_naming_instability memory;
# confirmed again 2026-07-28 during this same bare-metal bring-up, where
# nvme0n1/nvme1n1/nvme2n1 identities rotated between checks and a wrapper
# mount briefly grabbed the wrong physical disk). Resolve the emulated
# device by its stable by-id alias (same one mount.sh itself uses) and
# verify /mnt/nvme is actually mounted FROM that device, not just mounted
# from *something*.
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
# /usr/local/bin/perf (confirmed 2026-07-28: real DWARF unwind support,
# multi-frame symbolicated stacks). Unlike the sandbox, there's no
# resource-constrained-guest reason to prefer fp mode here -- bare metal
# has no QEMU nesting overhead, and dwarf is already confirmed working, so
# use it directly (matches the CEMU thread's own bare-metal choice).
export PERF_CALL_GRAPH="dwarf"

# Xeon E5-2640 v4 has no cpu_core hybrid P/E-core PMU (that's specific to
# newer client Intel chips like the CEMU thread's i7-13700K) -- same
# "Cannot find PMU 'cpu_core'" issue the sandbox already hit and fixed.
export PERF_EVENT="cycles"

# NOTE: cpufrequtils is required for FLAX's own src/init_nvmev.sh CPU
# frequency throttling step (emulates a "wimpy" ARM compute core) --
# confirmed missing on s1 as of 2026-07-28 (every cpufreq-set call failed
# with "command not found", and the CPU list perf.sh throttles doesn't
# even match this deployment's cpus=/slm_cpus=/csd_cpus= values). Install
# it and fix perf.sh's CPU list BEFORE trusting any timing numbers from a
# real profiling run -- see project_flax_integration_status memory.
