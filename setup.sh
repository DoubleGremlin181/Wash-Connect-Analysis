#!/bin/bash

# Simple API Scraper Setup

set -e

echo "Installing uv..."
curl -LsSf https://astral.sh/uv/install.sh | sh
. ~/.bashrc

echo "Getting uv path..."
UV_PATH=$(which uv)

echo "Making scraper executable..."
chmod +x scraper.py

echo "Enter location code to monitor:"
read LOCATION_CODE

SCRIPT_DIR="$(pwd)"
RUN_SCRIPT="$SCRIPT_DIR/run_scraper.sh"

echo "Creating helper script: $RUN_SCRIPT..."

cat > "$RUN_SCRIPT" <<'EOF'
#!/bin/bash
# Usage: ./run_scraper.sh LOCATION_CODE

if [ -z "$1" ]; then
    echo "Error: Please provide a location code as argument."
    exit 1
fi

LOCATION_CODE="$1"
SCRIPT_DIR="$(dirname "$0")"

cd "$SCRIPT_DIR"
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"

UV_PATH=$(which uv)
$UV_PATH run ./scraper.py "$LOCATION_CODE" >> "$SCRIPT_DIR/logs/$LOCATION_CODE.log" 2>&1
EOF

chmod +x "$RUN_SCRIPT"

echo "Adding helper script to crontab (runs every 5 minutes)..."
CRON_JOB="*/5 * * * * $RUN_SCRIPT $LOCATION_CODE"

# Check if crontab exists, if not create an empty one
if ! crontab -l; then
    echo "No crontab found. Creating a new one..."
    echo "" | crontab -
fi

# Add to crontab
(crontab -l; echo "$CRON_JOB") | crontab -

echo "Done! Scraper will run every 5 minutes for location: $LOCATION_CODE"
echo "Logs: $SCRIPT_DIR/logs/$LOCATION_CODE.log"
echo "Data: $SCRIPT_DIR/data/$LOCATION_CODE/"