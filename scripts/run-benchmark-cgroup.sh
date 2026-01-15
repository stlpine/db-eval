#!/bin/bash
# Run benchmarks with cgroup memory limit
# This is a wrapper around run-full-benchmark.sh that applies cgroup memory limits

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"

CGROUP_PATH="/sys/fs/cgroup/${CGROUP_NAME}"

# Check if cgroup exists
if [ ! -d "$CGROUP_PATH" ]; then
    log_error "Cgroup '${CGROUP_NAME}' does not exist"
    log_error "Run './scripts/setup-cgroup.sh' first"
    exit 1
fi

# Show cgroup info
log_info "Running benchmark with cgroup memory limit"
log_info "  Cgroup: ${CGROUP_NAME}"
log_info "  Memory limit: $(cat ${CGROUP_PATH}/memory.max 2>/dev/null | awk '{printf "%.2f GB", $1/1024/1024/1024}')"
echo ""

# Run benchmark with cgroup
exec sudo cgexec -g memory:${CGROUP_NAME} "${SCRIPT_DIR}/run-full-benchmark.sh" "$@"
