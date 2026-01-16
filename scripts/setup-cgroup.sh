#!/bin/bash
# Setup cgroup for memory-limited benchmark testing

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"

CGROUP_PATH="/sys/fs/cgroup/${CGROUP_NAME}"

# Remove existing cgroup if exists
if [ -d "$CGROUP_PATH" ]; then
    log_info "Removing existing cgroup: ${CGROUP_NAME}"
    sudo rmdir "$CGROUP_PATH" 2>/dev/null || true
fi

# Create cgroup
log_info "Creating cgroup: ${CGROUP_NAME}"
sudo cgcreate -g memory:/${CGROUP_NAME}

# Set memory limit
log_info "Setting memory limit to ${CGROUP_MEMORY_LIMIT}"
sudo cgset -r memory.max=${CGROUP_MEMORY_LIMIT} ${CGROUP_NAME}

# Show status
log_info "Cgroup setup complete:"
echo "  memory.max: $(sudo cat ${CGROUP_PATH}/memory.max)"
echo "  memory.current: $(sudo cat ${CGROUP_PATH}/memory.current)"

echo ""
log_info "To run benchmarks with memory limit, use:"
echo "  ./scripts/run-cgroup.sh [options]"
