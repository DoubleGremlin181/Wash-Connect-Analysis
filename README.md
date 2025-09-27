# Wash Connect Analysis

This repository contains tools for scraping and analyzing data from the Wash Connect App. It provides two main approaches for data collection: single location monitoring and bulk continuous scraping, plus location mapping capabilities.

## Features

- **Single Location Scraping**: Monitor individual laundromat locations at fixed intervals
- **Bulk Continuous Scraping**: Monitor multiple locations simultaneously with integrated parsing
- **Location Mapping**: Convert partial addresses to full addresses with coordinates using Google Maps API
- **Automatic Data Parsing**: Convert JSON data to CSV format for analysis
- **Storage Optimization**: Automatic cleanup of temporary JSON files
- **Failed Location Tracking**: Automatically skip locations that return 404 errors

## Setup Instructions

1. Clone the repository:
```bash
git clone https://github.com/DoubleGremlin181/Wash-Connect-Analysis.git
```

2. Change to the project directory:
```bash
cd Wash-Connect-Analysis/
```

3. Make scripts executable:
```bash
chmod +x setup.sh scraper.py bulk_scraper.py parser.py location_code_mapper.py
```

4. **For location mapping** (optional):
```bash
# Copy the environment template
cp .env.example .env

# Edit .env and add your Google Maps API key
# GOOGLE_MAPS_API_KEY=your_google_maps_api_key_here
```

## Usage Options

### Option 1: Single Location Monitoring (setup.sh)

Best for monitoring one specific location with regular intervals.

1. Run the setup script:
```bash
./setup.sh
```

This will:
- Install uv (Python package manager)
- Set up a cron job to scrape every 5 minutes
- Create log files in `logs/`
- Store data in `data/<location_code>/`

2. **Parse collected data** (optional):
```bash
./parser.py <location_code>
```

**Files created:**
- Raw JSON data: `data/<location_code>/`
- Logs: `logs/<location_code>.log`
- Parsed CSV: `data/<location_code>/parsed.csv`

### Option 2: Bulk Continuous Scraping (bulk_scraper.py)

Best for monitoring many locations simultaneously with automatic parsing and cleanup.

1. **Run the bulk scraper in screen** (recommended for long-running processes):
```bash
# Start a new screen session
screen -S wash-scraper

# Run the bulk scraper
./bulk_scraper.py W000001 W010000 --interval 15

# Detach from screen: Press Ctrl+A, then D
# Reattach later: screen -r wash-scraper
```

**Parameters:**
- `W000001 W010000`: Location code range (start and end)
- `--interval 15`: Update interval in minutes (default: 15)
- `--max-concurrent 50`: Max concurrent requests (default: 50)
- `--data-dir data`: Data directory (default: data)

**Advanced usage:**
```bash
# Monitor specific range every 10 minutes (in screen)
screen -S wash-scraper-small
./bulk_scraper.py W005000 W005100 --interval 10

# Higher concurrency for faster processing
screen -S wash-scraper-fast
./bulk_scraper.py W000001 W001000 --interval 15 --max-concurrent 100
```

**Files created:**
- Location data: `data/<location_code>/<location_code>.json`
- Parsed CSV: `data/<location_code>/parsed.csv` 
- Failed locations: `data/failed_codes.json`
- Logs: `logs/bulk_scraper.log`

### Option 3: Location Mapping (location_code_mapper.py)

Convert partial location addresses to full addresses with coordinates using Google Maps API.

**Setup:**
1. Get a Google Maps API key from [Google Cloud Console](https://console.cloud.google.com/)
2. Enable the Geocoding API for your project
3. Add your API key to `.env`:
```bash
cp .env.example .env
# Edit .env and add: GOOGLE_MAPS_API_KEY=your_api_key_here
```

**Usage:**
```bash
# Process all locations that have been scraped
./location_code_mapper.py

# Process a single location
./location_code_mapper.py W000001

# Specify custom output file location
./location_code_mapper.py --output-file custom_mapping.csv
```

**What it does:**
- Reads location names and state codes from scraped data
- Uses Google Geocoding API to get full addresses and coordinates  
- Outputs a CSV file with mapping data
- Handles rate limiting and API errors gracefully
- Appends to existing CSV files (won't re-process existing locations)

**Output CSV columns:**
- `location_code`: Original location code (e.g., W000001)
- `location_id`: Internal location ID
- `original_name`: Original location name from API
- `state_code`: State code extracted from ULN
- `formatted_address`: Full address from Google Maps
- `latitude` / `longitude`: Coordinates
- `place_id`: Google Places ID for reference
- `geocoding_success`: Boolean indicating if geocoding worked

## Data Structure

The scraped data includes:
- **Location info**: ID, name, state code, ULN
- **Room details**: Room ID, name, type
- **Machine status**: Number, type, availability, time remaining
- **Timestamps**: Request time, start time (for in-use machines)
- **Calculated status**: available, in_use, error

## Monitoring and Maintenance

### Check scraper status:
```bash
# View recent log entries
tail -f logs/bulk_scraper.log

# Check if screen session is running
screen -list

# Reattach to running scraper
screen -r wash-scraper

# Check cron jobs
crontab -l

# Monitor data directory size
du -sh data/
```

### Log Rotation

Set up automatic log rotation to manage log file sizes:

```bash
sudo tee /etc/logrotate.d/wash-scraper << 'EOF'
/root/Wash-Connect-Analysis/logs/*.log {
    daily
    rotate 7
    notifempty
    missingok
    compress
    delaycompress
    copytruncate
    create 644 root root
    dateext
    dateformat -%Y-%m-%d
}
EOF
```

This configuration:

- Rotates logs daily
- Keeps 7 days of logs
- Compresses old logs
- Names files with date suffix
- Skips empty log files
- Creates new files with 644 permissions

### Stop continuous scraping

In the screen session, press `Ctrl+C`, or detach and kill:
```bash
# Detach from screen: Ctrl+A, then D
# Kill the screen session
screen -S wash-scraper -X quit

# Or kill the process directly
pkill -f bulk_scraper.py
```

## Troubleshooting

**Permission errors:**
```bash
chmod +x *.sh *.py
```

**Missing dependencies:**
The scripts use `uv` with inline dependencies - no manual installation needed.

**404 errors for locations:**
Failed locations are automatically tracked in `data/failed_codes.json` and skipped in future runs.

**Google Maps API issues:**
- Ensure your API key is valid and has the Geocoding API enabled
- Check your API quotas and billing in Google Cloud Console
- The mapper includes rate limiting (0.1s delay between requests) to stay within limits
- Failed geocoding attempts are logged but don't stop the process

## File Structure

```
```
Wash-Connect-Analysis/
├── data/
│   ├── W000001/
│   │   ├── W000001.json          # Location data
│   │   └── parsed.csv            # Parsed machine data
│   ├── failed_codes.json         # Failed location codes
│   └── location_code_mapping.csv # Address/coordinate mapping
├── logs/
│   └── bulk_scraper.log          # Scraping logs
├── .env.example                  # Environment template
├── bulk_scraper.py               # Bulk continuous scraper
├── scraper.py                    # Single location scraper
├── parser.py                     # JSON to CSV parser
├── location_code_mapper.py       # Google Maps geocoding
├── setup.sh                      # Single location setup
└── README.md                     # This documentation
```

## Workflow Recommendations

1. **Discovery Phase**: Use bulk scraper to identify valid locations
   ```bash
   ./bulk_scraper.py W000001 W010000 --interval 15
   ```

2. **Location Mapping**: Get full addresses for valid locations
   ```bash
   ./location_code_mapper.py
   ```

3. **Continuous Monitoring**: Focus on specific regions or high-activity locations
   ```bash
   ./bulk_scraper.py W005000 W005100 --interval 10
   ```

4. **Data Analysis**: Use the parsed CSV files for analysis
   - Machine utilization patterns
   - Peak usage times by location
   - Geographic analysis with mapped coordinates

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.