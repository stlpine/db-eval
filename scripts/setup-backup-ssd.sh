#!/bin/bash
# Backup SSD Setup Script

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"

usage() {
    echo "Usage: $0 <action> [--force]"
    echo ""
    echo "Actions: check, mount, umount, format"
    echo ""
    echo "  --force  Skip confirmations"
    exit 1
}

ACTION=${1:-check}
FORCE=false

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

check_backup_ssd_device() {
    if [ -z "$BACKUP_SSD_DEVICE" ]; then
        log_error "Backup SSD device not found. Check scripts/find_backup_ssd.sh"
        return 1
    fi
    if [ ! -b "$BACKUP_SSD_DEVICE" ]; then
        log_error "Backup SSD device $BACKUP_SSD_DEVICE is not a valid block device!"
        return 1
    fi
    log_info "Backup SSD device found: ${BACKUP_SSD_DEVICE}"
    return 0
}

check_backup_ssd() {
    echo "Backup SSD Check"
    echo "----------------"

    if ! check_backup_ssd_device; then
        echo "ERROR: Backup SSD device not found"
        return 1
    fi

    if findmnt -n -o TARGET "$BACKUP_SSD_DEVICE" &>/dev/null; then
        CURRENT_MOUNT=$(findmnt -n -o TARGET "$BACKUP_SSD_DEVICE")
        echo "Mounted at: $CURRENT_MOUNT"
    else
        echo "NOT mounted. Run: sudo $0 mount"
    fi

    FS_TYPE=$(lsblk -n -o FSTYPE "$BACKUP_SSD_DEVICE" 2>/dev/null)
    if [ -n "$FS_TYPE" ]; then
        echo "Filesystem: $FS_TYPE"
    else
        echo "No filesystem. Run: sudo $0 format"
    fi

    echo ""
    df -h "$BACKUP_SSD_MOUNT" 2>/dev/null || echo "Mount point not available"
}

mount_backup_ssd() {
    log_info "Mounting backup SSD..."

    if ! check_backup_ssd_device; then
        exit 1
    fi

    if findmnt -n "$BACKUP_SSD_DEVICE" &>/dev/null; then
        CURRENT_MOUNT=$(findmnt -n -o TARGET "$BACKUP_SSD_DEVICE")
        log_error "Backup SSD is already mounted at: $CURRENT_MOUNT"
        exit 1
    fi

    FS_TYPE=$(lsblk -n -o FSTYPE "$BACKUP_SSD_DEVICE" 2>/dev/null)
    if [ -z "$FS_TYPE" ]; then
        log_error "No filesystem found on $BACKUP_SSD_DEVICE"
        log_error "Format the device first using: sudo $0 format"
        exit 1
    fi

    if [ ! -d "$BACKUP_SSD_MOUNT" ]; then
        log_info "Creating mount point: $BACKUP_SSD_MOUNT"
        sudo mkdir -p "$BACKUP_SSD_MOUNT"
    fi

    log_info "Mounting $BACKUP_SSD_DEVICE to $BACKUP_SSD_MOUNT"
    sudo mount "$BACKUP_SSD_DEVICE" "$BACKUP_SSD_MOUNT"

    if [ $? -eq 0 ]; then
        log_info "Backup SSD mounted successfully"
        sudo chmod 777 "$BACKUP_SSD_MOUNT"
        df -h "$BACKUP_SSD_MOUNT"
    else
        log_error "Failed to mount backup SSD"
        exit 1
    fi
}

umount_backup_ssd() {
    log_info "Unmounting backup SSD..."

    if ! findmnt -n "$BACKUP_SSD_DEVICE" &>/dev/null; then
        log_error "Backup SSD is not mounted"
        exit 1
    fi

    CURRENT_MOUNT=$(findmnt -n -o TARGET "$BACKUP_SSD_DEVICE")
    log_info "Unmounting $CURRENT_MOUNT"

    sudo umount "$CURRENT_MOUNT"

    if [ $? -eq 0 ]; then
        log_info "Backup SSD unmounted successfully"
    else
        log_error "Failed to unmount backup SSD"
        exit 1
    fi
}

format_backup_ssd() {
    if [ "$FORCE" != "true" ]; then
        log_info "WARNING: This will FORMAT the backup SSD and ERASE ALL DATA!"
        log_info "Device: $BACKUP_SSD_DEVICE"
        echo ""
        read -p "Are you sure you want to continue? (type 'yes' to confirm): " CONFIRM

        if [ "$CONFIRM" != "yes" ]; then
            log_info "Format cancelled"
            exit 0
        fi
    fi

    if ! check_backup_ssd_device; then
        exit 1
    fi

    if findmnt -n "$BACKUP_SSD_DEVICE" &>/dev/null; then
        if [ "$FORCE" = "true" ]; then
            log_info "Backup SSD is mounted, unmounting..."
            CURRENT_MOUNT=$(findmnt -n -o TARGET "$BACKUP_SSD_DEVICE")
            sudo umount "$CURRENT_MOUNT" || {
                log_error "Failed to unmount backup SSD"
                exit 1
            }
        else
            log_error "Backup SSD is currently mounted. Unmount first."
            exit 1
        fi
    fi

    log_info "Formatting $BACKUP_SSD_DEVICE with ext4 filesystem..."
    sudo mkfs.ext4 -F "$BACKUP_SSD_DEVICE"

    if [ $? -eq 0 ]; then
        log_info "Backup SSD formatted successfully"
    else
        log_error "Failed to format backup SSD"
        exit 1
    fi
}

case $ACTION in
    check)
        check_backup_ssd
        ;;
    mount)
        mount_backup_ssd
        ;;
    umount|unmount)
        umount_backup_ssd
        ;;
    format)
        format_backup_ssd
        ;;
    -h|--help)
        usage
        ;;
    *)
        log_error "Unknown action: $ACTION"
        usage
        ;;
esac
