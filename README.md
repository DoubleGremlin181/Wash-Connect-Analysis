# Wash Connect Analysis

This repository contains tools for scraping and analyzing data from the Wash Connect App. It provides two main approaches for data collection: single location monitoring and bulk continuous scraping.

## Features

- **Single Location Scraping**: Monitor individual laundromat locations at fixed intervals
- **Bulk Continuous Scraping**: Monitor multiple locations simultaneously with integrated parsing
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
chmod +x setup.sh scraper.py bulk_scraper.py parser.py cleanup.sh
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

2. **Set up automatic cleanup** (recommended):
```bash
# Add cleanup to crontab (runs every hour)
(crontab -l; echo "0 * * * * /root/Wash-Connect-Analysis/cleanup.sh >> /root/cleanup.log 2>&1") | crontab -
```

**Files created:**
- Location data: `data/<location_code>/<location_code>.json`
- Parsed CSV: `data/<location_code>/parsed.csv` 
- Failed locations: `data/failed_codes.json`
- Logs: `logs/bulk_scraper.log`

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

# Check cleanup logs
tail -f /root/cleanup.log
```

### Storage management:
The `cleanup.sh` script automatically:
- Removes temporary JSON files (keeps parsed CSV and location data)
- Truncates log files to 10MB
- Preserves essential data for analysis

### Stop continuous scraping:
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

**Disk space issues:**
Run cleanup manually:
```bash
./cleanup.sh
```

## File Structure

```
Wash-Connect-Analysis/
├── data/
│   ├── W000001/
│   │   ├── W000001.json          # Location data
│   │   └── parsed.csv            # Parsed machine data
│   └── failed_codes.json         # Failed location codes
├── logs/
│   └── bulk_scraper.log          # Scraping logs
├── bulk_scraper.py               # Bulk continuous scraper
├── scraper.py                    # Single location scraper
├── parser.py                     # JSON to CSV parser
├── setup.sh                      # Single location setup
└── cleanup.sh                    # Storage cleanup script
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.