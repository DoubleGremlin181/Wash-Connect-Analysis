#!/bin/bash
# cleanup.sh - Deletes all files except those starting with W0 or parsed
# in /root/Wash-Connect-Analysis/data and its subdirectories

set -euo pipefail
# -e : exit on any command failure
# -u : error on unset variables
# -o pipefail : fail if any part of a pipeline fails

TARGET_DIR="/root/Wash-Connect-Analysis/data"

# Safety: only proceed if directory exists
if [ -d "$TARGET_DIR" ]; then
    find "$TARGET_DIR" -type f ! -name 'W0*' ! -name 'parsed*' -delete
else
    echo "Error: $TARGET_DIR does not exist" >&2
    exit 1
fi
