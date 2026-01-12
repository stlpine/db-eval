#!/bin/bash
# Wait for SSD to cool down to target temperature before benchmarking

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"

usage() {
    echo "Usage: $0 [target_temp]"
    echo "Default: ${SSD_TARGET_TEMP}°C"
    exit 1
}

TARGET_TEMP=${1:-${SSD_TARGET_TEMP}}

log_info "Waiting for SSD to cool to ${TARGET_TEMP}°C (device: $SSD_DEVICE)"

# Check if nvme-cli is installed
if ! command -v nvme &> /dev/null; then
    log_error "nvme-cli is not installed. Install it with: sudo apt install nvme-cli"
    exit 1
fi

# Check if SSD device exists
if ! check_ssd_device; then
    log_error "SSD device not found or invalid"
    exit 1
fi

# Get initial temperature
get_ssd_temp() {
    TEMP=$(sudo nvme smart-log "$SSD_DEVICE" 2>/dev/null | awk '/temperature/ {print $3}')
    if [ -z "$TEMP" ]; then
        log_error "Failed to read SSD temperature"
        return 1
    fi
    echo "$TEMP"
}

# Initial temperature check
CURRENT_TEMP=$(get_ssd_temp)
if [ $? -ne 0 ]; then
    log_error "Cannot read SSD temperature. Check permissions and nvme-cli installation."
    exit 1
fi

log_info "Initial temperature: ${CURRENT_TEMP}°C"

# Check if already at target temperature
if [ "$CURRENT_TEMP" -le "$TARGET_TEMP" ]; then
    log_info "SSD is already at target temperature (${CURRENT_TEMP}°C <= ${TARGET_TEMP}°C)"
    exit 0
fi

# Wait for cooldown
log_info "Waiting for SSD to cool down..."
log_info "You can speed up cooling by ensuring proper airflow"
echo ""

START_TIME=$(date +%s)
LAST_TEMP=$CURRENT_TEMP

while true; do
    CURRENT_TEMP=$(get_ssd_temp)
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))

    # Calculate temperature change
    TEMP_CHANGE=$((LAST_TEMP - CURRENT_TEMP))

    # Estimate time remaining (simple linear extrapolation)
    if [ "$TEMP_CHANGE" -gt 0 ]; then
        RATE=$(echo "scale=2; $TEMP_CHANGE / ($ELAPSED / 60)" | bc 2>/dev/null || echo "0")
        REMAINING_TEMP=$((CURRENT_TEMP - TARGET_TEMP))
        if [ "$RATE" != "0" ]; then
            ETA=$(echo "scale=0; $REMAINING_TEMP / $RATE" | bc 2>/dev/null || echo "?")
            log_info "[$(date +'%H:%M:%S')] Temperature: ${CURRENT_TEMP}°C (cooling ~${RATE}°C/min, ETA: ~${ETA}min)"
        else
            log_info "[$(date +'%H:%M:%S')] Temperature: ${CURRENT_TEMP}°C"
        fi
    else
        log_info "[$(date +'%H:%M:%S')] Temperature: ${CURRENT_TEMP}°C"
    fi

    # Check if target reached
    if [ "$CURRENT_TEMP" -le "$TARGET_TEMP" ]; then
        echo ""
        log_info "SSD cooled down to ${CURRENT_TEMP}°C (target: ${TARGET_TEMP}°C)"
        log_info "Cooldown time: $((ELAPSED / 60)) minutes $((ELAPSED % 60)) seconds"
        exit 0
    fi

    LAST_TEMP=$CURRENT_TEMP
    sleep 10
done
