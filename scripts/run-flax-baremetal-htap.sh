#!/bin/bash
# HTAP profiling runner for percona-myrocks-nvmevirt on the real bare-metal
# FLAX/NVMeVirt host -- full AIDE-paper scale, real nvmev.ko-backed
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
    # it directly, to add -E lazy_itable_init=0,lazy_journal_init=0 to the
    # mkfs step -- see FLAX/CLAUDE.md gotcha #7. Without this, mkfs.ext4
    # defers inode-table/journal zeroing to a background ext4lazyinit kernel
    # thread that mysqld --initialize-insecure can race ahead of, leaving a
    # freshly-created file (e.g. an InnoDB redo log) pointing at not-yet-
    # zeroed blocks -- read back as garbage that InnoDB misreports as a
    # "no keyring configured" decryption failure. Confirmed 2026-08-03: a
    # plain `mkfs.ext4 -F` (what mount.sh does) hit this on 2 of 3 fresh-init
    # attempts; forcing synchronous (non-lazy) init here avoids the race
    # instead of just reformatting and hoping. Kept in db-eval rather than
    # editing FLAX/src/mount.sh, per explicit preference to keep the FLAX
    # repo's own diff at zero -- see feedback_minimal_flax_footprint memory.
    log_info "Wiping NVMeVirt-backed filesystem for a clean session..."
    bash "${SCRIPT_DIR}/mysql-control.sh" percona-myrocks-nvmevirt stop 2>/dev/null || true
    sudo umount /mnt/nvme 2>/dev/null
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
sudo -E bash "${SCRIPT_DIR}/../profiling/profile-htap.sh" "$CUTOFF" "" percona-myrocks-nvmevirt

log_info "=========================================="
log_info "FLAX bare-metal HTAP profiling complete"
log_info "Results : ${RESULTS_DIR}/profiling/htap/percona-myrocks-nvmevirt/"
log_info "=========================================="
