#!/bin/bash
# Sandbox-specific overrides for percona-myrocks-nvmevirt HTAP profiling inside
# the FLAX/NVMeVirt QEMU guest. Do not source directly -- set FLAX_SANDBOX_ENV
# to this file's absolute path before running any db-eval script; env.sh
# sources it automatically at the end.
#
# This is a SCALED-DOWN pass, deliberately: the guest is carved into two
# virtual NUMA nodes (8 vCPUs / 12GB each of 16 vCPUs / 24GB total), and
# NVMeVirt's own emulation threads (dispatcher, I/O workers, compute_work)
# run INSIDE this guest's own kernel, competing with mysqld/sysbench for the
# same vCPUs. Full-scale (db-eval's normal HTAP_TABLE_SIZE=100000 /
# HTAP_OLTP_THREADS=24) is reserved for the eventual bare-metal s1 run, once
# this harness is validated here.

# --- Percona Server build tree (NOT an installed prefix -- see
#     guest-phase6-verify.sh; mysqld runs directly out of the build dir) ---
export FLAX_PS_BUILD_DIR="$HOME/flax-build/percona-server/build"

# This is a raw build tree, not an installed prefix, so the mysql/mysqladmin
# client binaries only exist under runtime_output_directory/ -- there's no
# system-wide client to fall back to. mysql-control.sh/profile-htap.sh invoke
# both as bare commands, so they must be on PATH.
export PATH="${FLAX_PS_BUILD_DIR}/runtime_output_directory:${PATH}"

# Socket/pid/datadir for this profiling run -- distinct from guest-phase6-
# verify.sh's own correctness-test instance (different socket/port/datadir)
# so the two don't collide if run back-to-back.
export MYSQL_SOCKET_PERCONA_MYROCKS_NVMEVIRT="/tmp/mysql_nvmevirt_htap.sock"
export MYSQL_PID_PERCONA_MYROCKS_NVMEVIRT="/tmp/mysql_nvmevirt_htap.pid"
# Must live on the NVMeVirt-backed filesystem, not the guest's root disk --
# csdvirt_load_files derives physical LBAs via FIEMAP on the SST file; an SST
# on the wrong filesystem yields a bogus LBA into NVMeVirt's address space.
# See feedback_flax_datadir_device_locality memory. Sandbox's mount point is
# /mnt/nvme (confirmed in guest-phase6-verify.sh) -- NOT necessarily the path
# on bare-metal s1, which must be re-discovered there via nvme list/lsblk.
export MYSQL_DATADIR_PERCONA_MYROCKS_NVMEVIRT="/mnt/nvme/mysql-nvmevirt-htap-data"

# mysqld must run as root in this sandbox: csdvirt_init_dev()/the NVMeVirt
# device node need root, and the guest user is not root by default (needs
# sudo for everything -- confirmed via guest-phase6-verify.sh's pattern).
export MYSQL_DAEMON_USER="root"
export BENCH_SUDO="sudo"

# FlameGraph cloned at ~/FlameGraph on the guest (confirmed 2026-07-21).
export FLAMEGRAPH_DIR="$HOME/FlameGraph"

# db-eval cloned directly on the guest (confirmed 2026-07-21) -- not proxied
# through /mnt/host like the FLAX/percona-server source trees.
export RESULTS_DIR="$HOME/db-eval/results"

# /mnt/nvme is NVMeVirt-emulated, not a real bare-metal SSD, so env.sh's
# check_ssd_device/check_ssd_mount (which expect the latter) are no-ops here.
export SSD_MOUNT="/mnt/nvme"
check_ssd_device()      { return 0; }
check_ssd_mount()       { return 0; }
wait_for_mount_settle() { return 0; }

# Custom perf built from FLAX/linux/linux-6.0.10/tools/perf (installed to
# /usr/local/bin/perf) has real DWARF unwind support (libdw-dwarf-unwind: on,
# confirmed 2026-07-21 via a manual perf record/script frame-depth check).
# No cgroup/cgexec wrapper here -- guest isn't set up with cgroups, and the
# workload below is already scaled down, so memory pressure isn't a concern.
export PERF_CALL_GRAPH="dwarf"

# env.sh defaults PERF_EVENT to "cpu_core/cycles/" -- the hybrid P/E-core PMU
# name specific to the bare-metal i7-13700K. This QEMU guest's virtual CPU
# has no cpu_core PMU ("Cannot find PMU `cpu_core'"), same issue the CEMU
# thread already hit and fixed the same way in env-vm-csd.sh.
export PERF_EVENT="cycles"

# --- Scaled-down HTAP workload parameters for this sandbox pass ---
# Full scale is deferred to the eventual bare-metal s1 run, once this
# harness is confirmed working here.
export HTAP_TABLES="12"                          # keep -- join4.sql needs this exact shape
export HTAP_TABLE_SIZE="10000"                   # scaled down for the sandbox's vCPU/memory budget
export HTAP_OLTP_THREADS="8"                     # matches node0's vCPU budget (node1 is NVMeVirt's own threads)
export HTAP_LLT_COUNT="2"                        # lighter version-accumulation pressure for a first pass
export HTAP_WARMUP_DURATION="30"
export HTAP_DURATION="300"                       # informational only (not load-bearing in profile-htap.sh)
export HTAP_CTX_INTERVAL="30"
export HTAP_OLAP_RUNS="3"                        # fewer runs -- naive per-SST blocking offload untested at this concurrency
export HTAP_JOIN_CUTOFF="9000"                   # ~90% selectivity, scaled proportionally to HTAP_TABLE_SIZE
export HTAP_SELECTIVITY_CUTOFFS="100 1000 3000 6000 9000"
export HTAP_QUERY_TIMEOUT="3600"                 # generous -- naive offload's per-SST latency is unknown/possibly slow

# Control mechanism: rocksdb_nvmevirt_enabled is checked per-iterator-creation
# -- see profile-htap.sh's ROCKSDB_NVMEVIRT_ENABLED handling. Default true
# (offload ON); set ROCKSDB_NVMEVIRT_ENABLED=false to run the no-offload
# control with this same binary/data.
