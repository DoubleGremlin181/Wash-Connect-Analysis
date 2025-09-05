#!/bin/bash

# Simple API Scraper Setup

set -e

echo "Installing uv..."
curl -LsSf https://astral.sh/uv/install.sh | sh
exec bash

echo "Making scraper executable..."
chmod +x scraper.py

echo "Enter location code to monitor:"
read LOCATION_CODE

echo "Adding to crontab (runs every 5 minutes)..."
SCRIPT_PATH="$(pwd)/scraper.py"
CRON_JOB="*/5 * * * * $SCRIPT_PATH $LOCATION_CODE"

# Add to crontab
(crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -

echo "Done! Scraper will run every 5 minutes for location: $LOCATION_CODE"
echo "Logs: ./logs/$LOCATION_CODE.log"
echo "Data: ./data/$LOCATION_CODE/"