#!/bin/bash
# Find the backup SSD device path

ALIAS="/dev/disk/by-id/nvme-Samsung_SSD_990_PRO_1TB_S6Z1NJ0XC01846F"

if [ -e "${ALIAS}" ]; then
    SSD="$(realpath "${ALIAS}" 2>/dev/null)"
    echo "$SSD"
fi
