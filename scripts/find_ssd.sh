#!/bin/bash
# Find the SSD device path

ALIAS="/dev/disk/by-id/nvme-FADU_Delta_U.2_1.92TB_2624FQ1HV6U1ES300077"
SSD="$(realpath "${ALIAS}")"

echo $SSD
