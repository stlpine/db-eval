#!/bin/bash
# NVMe SSD Format Script
# WARNING: This will ERASE ALL DATA on the SSD!

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"

usage() {
    cat << EOF
Usage: $0 [options]

This script performs a low-level NVMe format on the SSD.
WARNING: This will PERMANENTLY ERASE ALL DATA!

Options:
    -f, --force     Skip confirmation prompt (dangerous!)
    -h, --help      Show this help message

Example:
    $0              # Format with confirmation
    $0 --force      # Format without confirmation (use with caution!)

Note: After formatting with NVMe format, you will need to:
1. Create a filesystem: sudo ./scripts/setup-ssd.sh format
2. Mount the SSD: sudo ./scripts/setup-ssd.sh mount
EOF
    exit 1
}

FORCE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -f|--force)
            FORCE=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            ;;
    esac
done

log_info "=========================================="
log_info "NVMe SSD Format Script"
log_info "=========================================="
log_info "WARNING: This will ERASE ALL DATA!"
log_info "=========================================="
echo ""

# Check if SSD device exists
if ! check_ssd_device; then
    log_error "SSD device not found or invalid"
    exit 1
fi

log_info "Target device: $SSD_DEVICE"
echo ""

# Get device information
log_info "Current device information:"
lsblk "$SSD_DEVICE" 2>/dev/null || true
echo ""
sudo nvme id-ctrl "$SSD_DEVICE" 2>/dev/null | grep -E "^(mn|sn|fr)" || true
echo ""

# Check if mounted
if findmnt -n -o TARGET "$SSD_DEVICE" &>/dev/null; then
    MOUNT_POINT=$(findmnt -n -o TARGET "$SSD_DEVICE")
    log_error "SSD is currently mounted at: $MOUNT_POINT"
    log_error "Unmount first using: sudo ./scripts/setup-ssd.sh umount"
    exit 1
fi

# Confirmation prompt
if [ "$FORCE" = false ]; then
    echo "=========================================="
    echo "FINAL WARNING"
    echo "=========================================="
    echo "You are about to perform a low-level format on:"
    echo "  Device: $SSD_DEVICE"
    echo ""
    echo "This will:"
    echo "  - PERMANENTLY ERASE ALL DATA"
    echo "  - Remove all partitions"
    echo "  - Reset the drive to factory state"
    echo ""
    echo "This operation CANNOT be undone!"
    echo "=========================================="
    echo ""
    read -p "Type 'ERASE ALL DATA' (exactly) to confirm: " CONFIRM

    if [ "$CONFIRM" != "ERASE ALL DATA" ]; then
        log_info "Format cancelled"
        exit 0
    fi

    echo ""
    read -p "Are you ABSOLUTELY sure? Type 'yes' to proceed: " CONFIRM2

    if [ "$CONFIRM2" != "yes" ]; then
        log_info "Format cancelled"
        exit 0
    fi
fi

echo ""
log_info "Starting NVMe format..."
log_info "This may take several minutes..."
echo ""

# Perform NVMe format
sudo nvme format -f "$SSD_DEVICE"

if [ $? -eq 0 ]; then
    echo ""
    log_info "=========================================="
    log_info "NVMe format completed successfully"
    log_info "=========================================="
    echo ""
    log_info "Next steps:"
    log_info "1. Create filesystem: sudo ./scripts/setup-ssd.sh format"
    log_info "2. Mount the SSD: sudo ./scripts/setup-ssd.sh mount"
    echo ""
else
    echo ""
    log_error "=========================================="
    log_error "NVMe format failed"
    log_error "=========================================="
    echo ""
    log_error "Possible causes:"
    log_error "- Insufficient permissions (need root/sudo)"
    log_error "- Device is busy or in use"
    log_error "- NVMe device does not support format command"
    echo ""
    exit 1
fi
