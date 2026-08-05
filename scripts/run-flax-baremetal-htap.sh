#!/bin/bash
# HTAP profiling runner for percona-myrocks-nvmevirt on the real bare-metal
# FLAX/NVMeVirt host -- full AIDE-paper scale, real nvmev.ko-backed
# device, no QEMU nesting. See run-flax-sandbox-htap.sh for the QEMU-guest
# equivalent (scaled-down first pass this bare-metal run supersedes).
#
# Usage:
#   bash run-flax-baremetal-htap.sh [--skip-prepare] [--cutoff <n>] [--with-cgroup]
#
# Examples:
#   bash run-flax-baremetal-htap.sh                  # prepare data + run profiling
#   bash run-flax-baremetal-htap.sh --skip-prepare   # skip data prep (data already loaded)
#   bash run-flax-baremetal-htap.sh --cutoff 90000
#   bash run-flax-baremetal-htap.sh --with-cgroup    # apply FLAX_CGROUP_MEMORY_LIMIT
#                                                     # (env-flax-baremetal.sh) -- requires
#                                                     # that value to be sized first from a
#                                                     # real unconstrained session's
#                                                     # memory_run<N>_*.txt files, and
#                                                     # ./scripts/setup-cgroup-flax.sh run once

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Point env.sh to bare-metal overrides — exported so all subprocess scripts inherit it
export FLAX_BAREMETAL_ENV="${SCRIPT_DIR}/../common/config/env-flax-baremetal.sh"
source "${SCRIPT_DIR}/../common/config/env.sh"

SKIP_PREPARE=false
CUTOFF="${HTAP_JOIN_CUTOFF}"
WITH_CGROUP=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-prepare) SKIP_PREPARE=true; shift ;;
        --cutoff) CUTOFF="$2"; shift 2 ;;
        --with-cgroup) WITH_CGROUP=true; shift ;;
        *) log_error "Unknown option: $1"; exit 1 ;;
    esac
done

# --with-cgroup: opt-in only, and only usable once FLAX_CGROUP_MEMORY_LIMIT
# has been sized from real calibration data (see env-flax-baremetal.sh's own
# comment) -- refuse rather than silently running unconstrained OR silently
# picking an unvalidated number. Without this flag (the default), behavior is
# unchanged from before: plain `sudo -E bash`, no cgroup, fully unconstrained.
if [ "$WITH_CGROUP" = "true" ]; then
    if [ -z "${FLAX_CGROUP_MEMORY_LIMIT}" ]; then
        log_error "--with-cgroup passed but FLAX_CGROUP_MEMORY_LIMIT is empty."
        log_error "This must be sized from real calibration data first -- run once"
        log_error "without --with-cgroup, check memory_run<N>_{before,after}.txt for"
        log_error "peak usage, then set FLAX_CGROUP_MEMORY_LIMIT in env-flax-baremetal.sh."
        exit 1
    fi
    CGROUP_CHECK_PATH="/sys/fs/cgroup/${FLAX_CGROUP_NAME}"
    if [ ! -d "$CGROUP_CHECK_PATH" ]; then
        log_error "--with-cgroup passed but cgroup '${FLAX_CGROUP_NAME}' does not exist."
        log_error "Run ./scripts/setup-cgroup-flax.sh first."
        exit 1
    fi
fi

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
if [ "$WITH_CGROUP" = "true" ]; then
    log_info "Memory limit : ${FLAX_CGROUP_MEMORY_LIMIT} (cgroup: ${FLAX_CGROUP_NAME})"
else
    log_info "Memory limit : none (unconstrained; pass --with-cgroup once sized to enable)"
fi
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

    # Full physical wipe of the NVMeVirt-backed filesystem before every
    # independent session (on top of prepare.sh's own logical DROP DATABASE
    # below). sysbench-htap/prepare.sh already resets sbtest1-12 at the table
    # level, so this isn't needed for correctness -- it's for cleanliness:
    # without it, a reused datadir directory leaves the *previous* session's
    # dropped-table SST files sitting on disk pending background compaction/
    # GC, adding untracked I/O contention early in the new session, and lets
    # host-level state (e.g. /tmp/nvmevirt_debug.log) accumulate silently
    # across sessions instead of starting clean. Stop mysqld first -- umount
    # fails on a busy device otherwise.
    #
    # Inlines FLAX's own mount.sh (umount/mkfs/mount/chown) instead of calling
    # it directly -- see FLAX/CLAUDE.md gotcha #7 for the full history. Kept in
    # db-eval rather than editing FLAX/src/mount.sh, per explicit preference to
    # keep the FLAX repo's own diff at zero -- see feedback_minimal_flax_footprint
    # memory.
    #
    # A full `dd if=/dev/zero` wipe of the raw block device, BEFORE mkfs, is
    # required here. Root cause confirmed 2026-08-04 by reading FLAX's own
    # src/main.c: NVMEV_STORAGE_INIT() explicitly memset()s the SLM region to
    # zero on module load but never does the same for storage_mapped (the
    # region this filesystem sits on) -- whatever physical DRAM content was
    # already in that memmap= reservation stays there until something
    # actually writes over it. On bare metal that's real, persistent host
    # DRAM with an uncontrolled history across this project's many kernel
    # rebuilds/module reloads; harmless in the QEMU sandbox variant only
    # because a fresh guest's own memmap= region happens to be backed by the
    # host kernel's zero-filled anonymous pages, not because of anything
    # NVMeVirt itself guarantees. The earlier attempted fix (-E
    # lazy_itable_init=0,lazy_journal_init=0 on mkfs alone, no dd) is NOT
    # reliable -- it only affects inode-table/journal zeroing, not regular
    # file data blocks, and the redo-log-decrypt crash this whole chain
    # produces recurred at the identical offset even with those flags in
    # place. A full zero-fill sidesteps the actual gap directly and is the
    # only approach confirmed reliable so far. Costs ~7 minutes for this
    # device's ~225GB at observed 500-770MB/s -- real but acceptable against
    # losing an entire multi-hour session to a crash. The proper permanent
    # fix is one missing `memset(vdev->storage_mapped, 0,
    # vdev->config.storage_size);` in FLAX's own NVMEV_STORAGE_INIT() --
    # deliberately not applied here, per the zero-FLAX-diff preference above;
    # revisit if this per-session cost ever becomes a real problem. Keeping
    # the mkfs flags too since they don't hurt and may still help some other
    # case.
    log_info "Wiping NVMeVirt-backed filesystem for a clean session..."
    bash "${SCRIPT_DIR}/mysql-control.sh" percona-myrocks-nvmevirt stop 2>/dev/null || true
    sudo umount /mnt/nvme 2>/dev/null
    log_info "Zero-filling ${SSD_DEVICE} before reformat (~7 min, avoids a known intermittent startup crash)..."
    sudo dd if=/dev/zero of="${SSD_DEVICE}" bs=1M status=progress || true
    sudo mkfs.ext4 -F -E lazy_itable_init=0,lazy_journal_init=0 "${SSD_DEVICE}" || { log_error "mkfs wipe failed"; exit 1; }
    sudo mount "${SSD_DEVICE}" /mnt/nvme || { log_error "mount failed"; exit 1; }
    sudo chown -R "$USER": /mnt/nvme

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
# sudo -E, same as Phase 1's prepare.sh call above: profile-htap.sh's own
# internal `mysql --socket="$SOCKET" -e ...` calls (rocksdb_nvmevirt_enabled
# toggle, ANALYZE TABLE, histograms, and the "is MySQL responding" check
# before each OLAP run) have no explicit -u, so they fall back to the OS
# username as MySQL username -- which doesn't exist as an account (only
# root@localhost does, from --initialize-insecure) -- and fail with
# "Access denied" when run as a non-root user. Confirmed 2026-07-31: this
# silently broke the nvmevirt_enabled toggle (the "no-offload control run"
# log line prints unconditionally, NOT gated on the SET GLOBAL actually
# succeeding), ANALYZE TABLE/histograms, and very likely caused a false
# "MySQL is not responding — aborting remaining runs" (same auth failure
# misread as the server being down, not necessarily a real crash) that
# aborted an entire run before any OLAP query executed. -E preserves the
# calling shell's exported SOCKET/HTAP_*/etc. variables, which plain sudo
# would otherwise reset along with $HOME.
if [ "$WITH_CGROUP" = "true" ]; then
    log_info "Running under cgroup memory limit: ${FLAX_CGROUP_MEMORY_LIMIT} (${FLAX_CGROUP_NAME})"
    sudo -E cgexec -g memory:"${FLAX_CGROUP_NAME}" \
        bash "${SCRIPT_DIR}/../profiling/profile-htap.sh" "$CUTOFF" "" percona-myrocks-nvmevirt
else
    sudo -E bash "${SCRIPT_DIR}/../profiling/profile-htap.sh" "$CUTOFF" "" percona-myrocks-nvmevirt
fi

log_info "=========================================="
log_info "FLAX bare-metal HTAP profiling complete"
log_info "Results : ${RESULTS_DIR}/profiling/htap/percona-myrocks-nvmevirt/"
log_info "=========================================="
