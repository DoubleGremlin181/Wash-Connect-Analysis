#!/bin/bash
# cleanup.sh - Deletes all files except those starting with W0 or parsed
# in /root/Wash-Connect-Analysis/data and its subdirectories
# Also truncates the log file to 10MB to avoid uncontrolled growth.

set -euo pipefail

TARGET_DIR="/root/Wash-Connect-Analysis/data"
LOG_FILE="/root/Wash-Connect-Analysis/logs/bulk_scraper.log"

# Truncate the log file to 10MB (keeps the oldest 10MB)
truncate -s 10M "$LOG_FILE"

# Safety: only proceed if directory exists
if [ -d "$TARGET_DIR" ]; then
    find "$TARGET_DIR" -type f ! -name 'W0*' ! -name 'parsed*' -delete
else
    echo "Error: $TARGET_DIR does not exist" >&2
    exit 1
fi
