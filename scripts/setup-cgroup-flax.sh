#!/bin/bash
# Setup cgroup for memory-limited FLAX bare-metal HTAP profiling.
#
# Mirrors setup-cgroup.sh's structure exactly, but targets FLAX_CGROUP_NAME/
# FLAX_CGROUP_MEMORY_LIMIT (env-flax-baremetal.sh) instead of env.sh's
# CGROUP_NAME/CGROUP_MEMORY_LIMIT -- that pair belongs to the CEMU thread's
# own comparison (percona-myrocks bare-metal / percona-myrocks-csd VM, a
# different host entirely) and was never sized for FLAX's working set. Don't
# point this at that cgroup or reuse its 16G value; see
# feedback_dont_overreference_cemu memory.
#
# Usage: bash setup-cgroup-flax.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export FLAX_BAREMETAL_ENV="${SCRIPT_DIR}/../common/config/env-flax-baremetal.sh"
source "${SCRIPT_DIR}/../common/config/env.sh"

if [ -z "${FLAX_CGROUP_MEMORY_LIMIT}" ]; then
    log_error "FLAX_CGROUP_MEMORY_LIMIT is empty -- refusing to create an unsized cgroup."
    log_error "This must be set from real calibration data, not guessed:"
    log_error "  1. Run one FLAX bare-metal HTAP session unconstrained (no --with-cgroup)."
    log_error "  2. Read peak mysqld RSS / free-memory drop from that session's"
    log_error "     memory_run<N>_{before,after}.txt files."
    log_error "  3. Set FLAX_CGROUP_MEMORY_LIMIT in env-flax-baremetal.sh with headroom"
    log_error "     above that peak, then re-run this script."
    exit 1
fi

CGROUP_PATH="/sys/fs/cgroup/${FLAX_CGROUP_NAME}"

# Remove existing cgroup if exists
if [ -d "$CGROUP_PATH" ]; then
    log_info "Removing existing cgroup: ${FLAX_CGROUP_NAME}"
    sudo rmdir "$CGROUP_PATH" 2>/dev/null || true
fi

# Create cgroup
log_info "Creating cgroup: ${FLAX_CGROUP_NAME}"
sudo cgcreate -g memory:/${FLAX_CGROUP_NAME}

# Set memory limit
log_info "Setting memory limit to ${FLAX_CGROUP_MEMORY_LIMIT}"
sudo cgset -r memory.max=${FLAX_CGROUP_MEMORY_LIMIT} ${FLAX_CGROUP_NAME}

# Show status
log_info "Cgroup setup complete:"
echo "  memory.max: $(sudo cat ${CGROUP_PATH}/memory.max)"
echo "  memory.current: $(sudo cat ${CGROUP_PATH}/memory.current)"

echo ""
log_info "To run FLAX bare-metal HTAP profiling with this memory limit, use:"
echo "  ./scripts/run-flax-baremetal-htap.sh --with-cgroup [other options]"
