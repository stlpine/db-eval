#!/bin/bash
# SSD Setup and Verification Script

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"

usage() {
    echo "Usage: $0 <action> [--force]"
    echo ""
    echo "Actions: check, mount, umount, format, reset, nvme-format, info, temp, monitor, cooldown"
    echo ""
    echo "  --force  Skip confirmations"
    exit 1
}

ACTION=${1:-check}
FORCE=false

# Parse options (skip first argument which is ACTION)
for arg in "${@:2}"; do
    case $arg in
        --force)
            FORCE=true
            ;;
        *)
            log_error "Unknown option: $arg"
            usage
            ;;
    esac
done

check_ssd() {
    echo "SSD Check"
    echo "---------"

    if ! check_ssd_device; then
        echo "ERROR: SSD device not found. Check scripts/find_ssd.sh"
        return 1
    fi

    if findmnt -n -o TARGET "$SSD_DEVICE" &>/dev/null; then
        CURRENT_MOUNT=$(findmnt -n -o TARGET "$SSD_DEVICE")
        echo "Mounted at: $CURRENT_MOUNT"
        if [ "$CURRENT_MOUNT" != "$SSD_MOUNT" ]; then
            echo "  (config says $SSD_MOUNT - update env.sh if needed)"
        fi
    else
        echo "NOT mounted. Run: sudo $0 mount"
    fi

    FS_TYPE=$(lsblk -n -o FSTYPE "$SSD_DEVICE" 2>/dev/null)
    if [ -n "$FS_TYPE" ]; then
        echo "Filesystem: $FS_TYPE"
    else
        echo "No filesystem. Run: sudo $0 format"
    fi

    echo ""
    get_ssd_info
}

mount_ssd() {
    log_info "Mounting SSD..."

    if ! check_ssd_device; then
        exit 1
    fi

    # Check if already mounted
    if findmnt -n "$SSD_DEVICE" &>/dev/null; then
        CURRENT_MOUNT=$(findmnt -n -o TARGET "$SSD_DEVICE")
        log_error "SSD is already mounted at: $CURRENT_MOUNT"
        exit 1
    fi

    # Check filesystem
    FS_TYPE=$(lsblk -n -o FSTYPE "$SSD_DEVICE" 2>/dev/null)
    if [ -z "$FS_TYPE" ]; then
        log_error "No filesystem found on $SSD_DEVICE"
        log_error "Format the device first using: sudo $0 format"
        exit 1
    fi

    # Create mount point if it doesn't exist
    if [ ! -d "$SSD_MOUNT" ]; then
        log_info "Creating mount point: $SSD_MOUNT"
        sudo mkdir -p "$SSD_MOUNT"
    fi

    # Mount the device
    log_info "Mounting $SSD_DEVICE to $SSD_MOUNT"
    sudo mount "$SSD_DEVICE" "$SSD_MOUNT"

    if [ $? -eq 0 ]; then
        log_info "SSD mounted successfully"
        sudo chmod 777 "$SSD_MOUNT"
        df -h "$SSD_MOUNT"

        # Create timestamp marker for future reference (before wait begins)
        echo "$(date +%s)" | sudo tee "${SSD_MOUNT}/.mount_timestamp" > /dev/null 2>&1

        # Wait for filesystem to settle
        log_info "Waiting ${SSD_MOUNT_WAIT} seconds for filesystem to settle..."
        log_info "(This ensures proper initialization before operations)"

        # Show countdown
        for ((i=$SSD_MOUNT_WAIT; i>0; i--)); do
            if [ $i -eq 30 ] || [ $i -eq 10 ] || [ $i -eq 5 ]; then
                echo "  ${i} seconds remaining..."
            fi
            sleep 1
        done

        log_info "Filesystem ready for operations"
    else
        log_error "Failed to mount SSD"
        exit 1
    fi
}

umount_ssd() {
    log_info "Unmounting SSD..."

    if ! findmnt -n "$SSD_DEVICE" &>/dev/null; then
        log_error "SSD is not mounted"
        exit 1
    fi

    CURRENT_MOUNT=$(findmnt -n -o TARGET "$SSD_DEVICE")
    log_info "Unmounting $CURRENT_MOUNT"

    sudo umount "$CURRENT_MOUNT"

    if [ $? -eq 0 ]; then
        log_info "SSD unmounted successfully"
    else
        log_error "Failed to unmount SSD"
        log_error "Check if any processes are using the mount point:"
        lsof +D "$CURRENT_MOUNT" 2>/dev/null || true
        exit 1
    fi
}

format_ssd() {
    if [ "$FORCE" != "true" ]; then
        log_info "WARNING: This will FORMAT the SSD and ERASE ALL DATA!"
        log_info "Device: $SSD_DEVICE"
        echo ""
        read -p "Are you sure you want to continue? (type 'yes' to confirm): " CONFIRM

        if [ "$CONFIRM" != "yes" ]; then
            log_info "Format cancelled"
            exit 0
        fi
    fi

    if ! check_ssd_device; then
        exit 1
    fi

    # Check if mounted and unmount if necessary
    if findmnt -n "$SSD_DEVICE" &>/dev/null; then
        if [ "$FORCE" = "true" ]; then
            log_info "SSD is mounted, unmounting..."
            CURRENT_MOUNT=$(findmnt -n -o TARGET "$SSD_DEVICE")
            sudo umount "$CURRENT_MOUNT" || {
                log_error "Failed to unmount SSD"
                exit 1
            }
        else
            log_error "SSD is currently mounted. Unmount first:"
            log_error "  sudo $0 umount"
            exit 1
        fi
    fi

    log_info "Formatting $SSD_DEVICE with ext4 filesystem..."
    sudo mkfs.ext4 -F "$SSD_DEVICE"

    if [ $? -eq 0 ]; then
        log_info "SSD formatted successfully"
        if [ "$FORCE" != "true" ]; then
            log_info "You can now mount it using: sudo $0 mount"
        fi
    else
        log_error "Failed to format SSD"
        exit 1
    fi
}

show_info() {
    get_ssd_info
    echo ""
    if [ -b "$SSD_DEVICE" ]; then
        echo "SMART data:"
        sudo smartctl -a "$SSD_DEVICE" 2>/dev/null || echo "(install smartmontools for more info)"
    fi
}

show_temp() {
    if ! check_ssd_device; then
        exit 1
    fi

    TEMP=$(get_ssd_temp)
    if [ "$TEMP" = "N/A" ]; then
        log_error "Cannot read temperature (install nvme-cli)"
        exit 1
    fi

    echo "$SSD_DEVICE: ${TEMP}°C (target: ${SSD_TARGET_TEMP}°C)"
    if [ "$TEMP" -gt "$SSD_TARGET_TEMP" ]; then
        echo "  -> above target, may need cooldown"
    fi
}

monitor_temp() {
    if ! check_ssd_device; then
        exit 1
    fi

    if ! command -v nvme &> /dev/null; then
        log_error "nvme-cli not installed"
        exit 1
    fi

    echo "Monitoring $SSD_DEVICE (target: ${SSD_TARGET_TEMP}°C) - Ctrl+C to stop"
    echo ""

    while true; do
        TEMP=$(get_ssd_temp)
        if [ "$TEMP" = "N/A" ]; then
            echo "[$(date +'%H:%M:%S')] N/A"
        else
            echo "[$(date +'%H:%M:%S')] ${TEMP}°C"
        fi
        sleep 5
    done
}

run_cooldown() {
    "${SCRIPT_DIR}/wait_for_cooldown.sh" "$SSD_TARGET_TEMP"
}

reset_ssd() {
    log_info "Resetting SSD: $SSD_DEVICE"

    if [ "$FORCE" != "true" ]; then
        read -p "This will erase all data. Continue? (yes/no): " CONFIRM
        if [ "$CONFIRM" != "yes" ]; then
            echo "Cancelled"
            exit 0
        fi
    fi

    # Unmount if mounted
    if findmnt -n "$SSD_DEVICE" &>/dev/null; then
        CURRENT_MOUNT=$(findmnt -n -o TARGET "$SSD_DEVICE")
        log_info "Unmounting $CURRENT_MOUNT"
        sudo umount "$CURRENT_MOUNT" || { log_error "Unmount failed"; exit 1; }
    fi

    # NVMe format
    log_info "NVMe format..."
    check_ssd_device || exit 1
    sudo nvme format -f "$SSD_DEVICE" || { log_error "NVMe format failed"; exit 1; }

    # ext4 format
    log_info "Creating ext4..."
    sudo mkfs.ext4 -F "$SSD_DEVICE" || { log_error "mkfs failed"; exit 1; }

    # Mount
    log_info "Mounting..."
    [ ! -d "$SSD_MOUNT" ] && sudo mkdir -p "$SSD_MOUNT"
    sudo mount "$SSD_DEVICE" "$SSD_MOUNT" || { log_error "Mount failed"; exit 1; }
    sudo chmod 777 "$SSD_MOUNT"
    echo "$(date +%s)" | sudo tee "${SSD_MOUNT}/.mount_timestamp" > /dev/null 2>&1

    # Wait for filesystem
    log_info "Waiting ${SSD_MOUNT_WAIT}s for filesystem..."
    sleep "$SSD_MOUNT_WAIT"

    # Temperature check
    TEMP=$(get_ssd_temp)
    if [ "$TEMP" != "N/A" ] && [ "$TEMP" -gt "$SSD_TARGET_TEMP" ]; then
        if [ "$SSD_COOLDOWN_ENABLED" = "true" ]; then
            log_info "Waiting for cooldown (${TEMP}°C > ${SSD_TARGET_TEMP}°C)..."
            run_cooldown
        fi
    fi

    log_info "Done. Mounted at $SSD_MOUNT"
    df -h "$SSD_MOUNT"
}

# Execute action
case $ACTION in
    check)
        check_ssd
        ;;
    mount)
        mount_ssd
        ;;
    umount|unmount)
        umount_ssd
        ;;
    format)
        format_ssd
        ;;
    reset)
        reset_ssd
        ;;
    nvme-format)
        "${SCRIPT_DIR}/format_ssd.sh"
        ;;
    info)
        show_info
        ;;
    temp|temperature)
        show_temp
        ;;
    monitor)
        monitor_temp
        ;;
    cooldown)
        run_cooldown
        ;;
    -h|--help)
        usage
        ;;
    *)
        log_error "Unknown action: $ACTION"
        usage
        ;;
esac
