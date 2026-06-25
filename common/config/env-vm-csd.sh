#!/bin/bash
# VM-specific overrides for percona-myrocks-csd HTAP profiling inside the QEMU VM.
# Do not source directly — set CEMU_VM_ENV to this file's absolute path before
# running any db-eval script; env.sh will source it automatically at the end.
#
# Example (from run-vm-htap.sh):
#   export CEMU_VM_ENV="$(dirname "$0")/../common/config/env-vm-csd.sh"

# Socket and PID match the VM's running mysqld
export MYSQL_SOCKET_PERCONA_MYROCKS_CSD="/tmp/mysql_csd.sock"
export MYSQL_PID_PERCONA_MYROCKS_CSD="/tmp/mysql_csd.pid"
export MYSQL_DATADIR_PERCONA_MYROCKS_CSD="/root/mysql-data"

# mysqld is deployed at /usr/local/percona-csd; running as root inside VM
export MYSQL_CSD_CONFIG_OVERRIDE="/root/db-eval/common/config/my-percona-myrocks-csd-vm.cnf"
export MYSQL_DAEMON_USER="root"

# FlameGraph cloned at /root/FlameGraph inside VM
export FLAMEGRAPH_DIR="/root/FlameGraph"

# Results directory inside VM
export RESULTS_DIR="/root/db-eval/results"

# No dedicated SSD in VM — rootfs holds the data
export SSD_MOUNT="/root"

# Running as root inside VM — no sudo needed
export BENCH_SUDO=""

# Percona CSD binaries (mysqladmin, mysql, etc.) are not in the default PATH
export PATH="/usr/local/percona-csd/bin:${PATH}"

# Hardware PMU (cycles) teardown triggers jump_label_update SMP patching in the CEMU kernel,
# which crashes under QEMU SMP load. task-clock is a software event with no hardware PMU
# teardown path — avoids the panic while still providing CPU-time samples.
export PERF_EVENT="task-clock"

# Custom-built perf from CEMU kernel sources lacks libdw, so --call-graph dwarf is broken.
# mysqld is rebuilt with -fno-omit-frame-pointer, so fp unwinding works instead.
# task-clock + fp: no hardware PMU teardown, no jump_label crash.
export PERF_CALL_GRAPH="fp"

# Override SSD checks to succeed immediately (no block device check needed)
check_ssd_device()      { return 0; }
check_ssd_mount()       { return 0; }
wait_for_mount_settle() { return 0; }
