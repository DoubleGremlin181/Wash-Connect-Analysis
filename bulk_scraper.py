#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.12"
# dependencies = ["requests", "aiohttp", "asyncio", "pandas"]
# ///

"""
Bulk API Scraper for Wash Mobile Pay
Scrapes location data and machine status from API endpoints for a list of location codes.
Distributes requests evenly across time intervals with parallel processing.
Now includes integrated parsing to CSV and cleanup of JSON files.

Usage:
  # Using a range (original functionality)
  uv run bulk_scraper.py --range W000001 W010000 --interval 15

  # Using a file with location codes
  uv run bulk_scraper.py --file location_codes.txt --interval 15

  # Using specific codes directly
  uv run bulk_scraper.py --codes W000001 W000002 W000003 --interval 15
"""

import argparse
import asyncio
import json
import logging
import sys
import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Set
import re
import math
import importlib.util

import aiohttp
import pandas as pd

# Import parser module at global scope
parser_path = Path(__file__).parent / "parser.py"
if not parser_path.exists():
    raise ImportError(f"parser.py not found at {parser_path}")

spec = importlib.util.spec_from_file_location("parser", parser_path)
parser = importlib.util.module_from_spec(spec)
spec.loader.exec_module(parser)


def setup_logging(log_dir: Path) -> logging.Logger:
    """Setup logging configuration."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "bulk_scraper.log"

    logger = logging.getLogger("bulk_api_scraper")
    logger.setLevel(logging.INFO)

    # Remove existing handlers to avoid duplicates
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)

    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def parse_location_code(location_code: str) -> tuple[str, int]:
    """Parse location code to extract prefix and number."""
    match = re.match(r"([A-Z]+)(\d+)", location_code.upper())
    if not match:
        raise ValueError(f"Invalid location code format: {location_code}")
    return match.group(1), int(match.group(2))


def generate_location_codes(start_code: str, end_code: str) -> List[str]:
    """Generate list of location codes in range."""
    start_prefix, start_num = parse_location_code(start_code)
    end_prefix, end_num = parse_location_code(end_code)

    if start_prefix != end_prefix:
        raise ValueError(
            f"Location codes must have same prefix: {start_prefix} vs {end_prefix}"
        )

    if start_num > end_num:
        raise ValueError(f"Start code must be <= end code: {start_num} vs {end_num}")

    # Determine padding length from original codes
    start_padding = len(start_code) - len(start_prefix)

    codes = []
    for num in range(start_num, end_num + 1):
        code = f"{start_prefix}{num:0{start_padding}d}"
        codes.append(code)

    return codes


def load_location_codes_from_file(filepath: Path, logger: logging.Logger) -> List[str]:
    """Load location codes from a text file (one per line)."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            codes = []
            for line_num, line in enumerate(f, 1):
                line = line.strip()

                # Skip empty lines and comments
                if not line or line.startswith("#"):
                    continue

                # Validate location code format
                try:
                    parse_location_code(line)
                    codes.append(line.upper())
                except ValueError as e:
                    logger.warning(
                        f"Invalid location code on line {line_num}: '{line}' - {e}"
                    )
                    continue

        logger.info(f"Loaded {len(codes)} valid location codes from {filepath}")
        return codes

    except FileNotFoundError:
        logger.error(f"Location codes file not found: {filepath}")
        raise
    except Exception as e:
        logger.error(f"Error reading location codes file {filepath}: {e}")
        raise


def validate_location_codes(codes: List[str], logger: logging.Logger) -> List[str]:
    """Validate and clean up location codes."""
    valid_codes = []
    for code in codes:
        try:
            parse_location_code(code)
            valid_codes.append(code.upper())
        except ValueError as e:
            logger.warning(f"Invalid location code '{code}': {e}")

    return valid_codes


def load_failed_codes(data_dir: Path) -> Set[str]:
    """Load list of location codes that have failed with 404."""
    failed_file = data_dir / "failed_codes.json"
    try:
        if failed_file.exists():
            with open(failed_file, "r") as f:
                return set(json.load(f))
    except Exception:
        pass
    return set()


def save_failed_codes(failed_codes: Set[str], data_dir: Path):
    """Save list of location codes that have failed with 404."""
    failed_file = data_dir / "failed_codes.json"
    try:
        data_dir.mkdir(parents=True, exist_ok=True)
        with open(failed_file, "w") as f:
            json.dump(list(failed_codes), f, indent=2)
    except Exception as e:
        logging.getLogger("bulk_api_scraper").error(f"Failed to save failed codes: {e}")


async def make_request(
    session: aiohttp.ClientSession, url: str, logger: logging.Logger, timeout: int = 30
) -> tuple[Optional[Dict[str, Any]], int]:
    """Make HTTP request and return JSON response and status code."""
    try:
        async with session.get(
            url, timeout=aiohttp.ClientTimeout(total=timeout)
        ) as response:
            status_code = response.status
            if status_code == 200:
                data = await response.json()
                logger.debug(f"Request successful for: {url}")
                return data, status_code
            else:
                logger.warning(f"HTTP {status_code} error for URL: {url}")
                return None, status_code

    except asyncio.TimeoutError:
        logger.error(f"Timeout error for URL: {url}")
        return None, 0
    except Exception as e:
        logger.error(f"Request failed for URL {url}: {e}")
        return None, 0


async def get_location_data(
    session: aiohttp.ClientSession, location_code: str, logger: logging.Logger
) -> tuple[Optional[Dict[str, Any]], int]:
    """Get location data from the first API endpoint."""
    url = f"https://us-central1-washmobilepay.cloudfunctions.net/locations?srcode={location_code}"
    return await make_request(session, url, logger)


async def get_machine_status(
    session: aiohttp.ClientSession, uln: str, logger: logging.Logger
) -> tuple[Optional[Dict[str, Any]], int]:
    """Get machine status from the second API endpoint."""
    url = f"https://us-central1-washmobilepay.cloudfunctions.net/get_machine_status_v1?uln={uln}"
    return await make_request(session, url, logger)


def save_json(data: Dict[str, Any], filepath: Path, logger: logging.Logger) -> bool:
    """Save data to JSON file."""
    try:
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.debug(f"Successfully saved data to: {filepath}")
        return True
    except Exception as e:
        logger.error(f"Failed to save file {filepath}: {e}")
        return False


def load_json(filepath: Path) -> Optional[Dict[str, Any]]:
    """Load data from JSON file."""
    try:
        if filepath.exists():
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return None


def parse_and_cleanup_location_data(
    location_code: str, data_dir: Path, logger: logging.Logger
) -> bool:
    """Parse location data to CSV and cleanup JSON files."""
    try:
        # Parse the location data
        records = parser.parse_location_code_data(location_code, data_dir, logger)

        if not records:
            logger.warning(f"No records found for {location_code}")
            return False

        # Create DataFrame and save to CSV
        df = pd.DataFrame(records)
        df = df.sort_values(["request_time", "room_id", "machine_number"])

        # Save to CSV
        output_dir = data_dir / location_code
        output_file = output_dir / "parsed.csv"

        # Append to existing CSV or create new one
        df.to_csv(output_file, index=False, mode="a", header=not output_file.exists())
        logger.info(f"Parsed and saved {len(df)} records to {output_file}")

        # Cleanup: Remove JSON status files (keep location file)
        location_dir = data_dir / location_code
        uln_pattern = re.compile(
            r"^[A-Z]{2}[A-Z0-9]+-\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{4}Z\.json$"
        )

        removed_count = 0
        for json_file in location_dir.glob("*.json"):
            # Only remove status files, keep location files
            if uln_pattern.match(json_file.name):
                try:
                    json_file.unlink()
                    removed_count += 1
                    logger.debug(f"Removed JSON file: {json_file}")
                except Exception as e:
                    logger.warning(f"Failed to remove {json_file}: {e}")

        if removed_count > 0:
            logger.info(
                f"Cleaned up {removed_count} JSON status files for {location_code}"
            )

        return True

    except Exception as e:
        logger.error(f"Failed to parse and cleanup data for {location_code}: {e}")
        return False


async def scrape_location_batch(
    session: aiohttp.ClientSession,
    location_codes: List[str],
    data_dir: Path,
    failed_codes: Set[str],
    logger: logging.Logger,
) -> tuple[Dict[str, str], Set[str]]:
    """Scrape location data for a batch of codes."""
    tasks = []
    code_to_task = {}

    for code in location_codes:
        if code in failed_codes:
            continue

        location_file = data_dir / code / f"{code}.json"
        if location_file.exists():
            continue

        task = get_location_data(session, code, logger)
        tasks.append(task)
        code_to_task[task] = code

    if not tasks:
        return {}, failed_codes

    results = await asyncio.gather(*tasks, return_exceptions=True)
    location_to_uln = {}
    new_failed_codes = set()

    for i, result in enumerate(results):
        task = tasks[i]
        code = code_to_task[task]

        if isinstance(result, Exception):
            logger.error(f"Exception for {code}: {result}")
            continue

        data, status_code = result

        if status_code == 404:
            logger.warning(f"Location {code} not found (404) - adding to failed codes")
            new_failed_codes.add(code)
            continue
        elif data is None:
            logger.warning(f"Failed to get location data for {code}")
            continue

        try:
            uln = data["location"]["uln"].strip()
            location_file = data_dir / code / f"{code}.json"
            if save_json(data, location_file, logger):
                location_to_uln[code] = uln
                logger.info(f"Saved location data for {code} (ULN: {uln})")
        except KeyError:
            logger.error(f"No ULN found in location data for {code}")

    return location_to_uln, failed_codes | new_failed_codes


async def scrape_machine_status_batch(
    session: aiohttp.ClientSession,
    location_to_uln: Dict[str, str],
    data_dir: Path,
    logger: logging.Logger,
) -> int:
    """Scrape machine status for a batch of locations, parse to CSV, and cleanup."""
    if not location_to_uln:
        return 0

    tasks = []
    uln_to_code = {}

    for code, uln in location_to_uln.items():
        task = get_machine_status(session, uln, logger)
        tasks.append(task)
        uln_to_code[task] = (code, uln)

    results = await asyncio.gather(*tasks, return_exceptions=True)
    success_count = 0
    codes_to_parse = []

    request_time = (
        datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z"
    )

    for i, result in enumerate(results):
        task = tasks[i]
        code, uln = uln_to_code[task]

        if isinstance(result, Exception):
            logger.error(f"Exception for {code} (ULN: {uln}): {result}")
            continue

        data, status_code = result

        if data is None:
            logger.warning(f"Failed to get machine status for {code} (ULN: {uln})")
            continue

        status_file = data_dir / code / f"{uln}-{request_time}.json"
        if save_json(data, status_file, logger):
            success_count += 1
            codes_to_parse.append(code)
            logger.debug(f"Saved machine status for {code}")

    # Parse and cleanup for all successfully scraped codes
    for code in codes_to_parse:
        parse_and_cleanup_location_data(code, data_dir, logger)

    return success_count


def get_existing_locations(data_dir: Path, location_codes: List[str]) -> Dict[str, str]:
    """Get existing location data and extract ULNs."""
    location_to_uln = {}

    for code in location_codes:
        location_file = data_dir / code / f"{code}.json"
        data = load_json(location_file)
        if data:
            try:
                uln = data["location"]["uln"].strip()
                location_to_uln[code] = uln
            except KeyError:
                pass

    return location_to_uln


def calculate_batch_parameters(
    total_requests: int,
    interval_seconds: int,
    min_batch_size: int = 5,
    max_batch_size: int = 50,
) -> tuple[int, float]:
    """Calculate optimal batch size and interval for request distribution."""
    if total_requests == 0:
        return 0, 0

    # Target requests per second
    requests_per_second = total_requests / interval_seconds

    # Start with ideal batch size (requests per second rounded to nearest integer)
    ideal_batch_size = max(1, round(requests_per_second))

    # Clamp to reasonable bounds
    batch_size = max(min_batch_size, min(ideal_batch_size, max_batch_size))

    # Calculate number of batches needed
    num_batches = math.ceil(total_requests / batch_size)

    # Calculate actual interval between batches
    batch_interval = interval_seconds / num_batches if num_batches > 1 else 0

    return batch_size, batch_interval


async def run_bulk_scraper(
    location_codes: List[str],
    interval_minutes: int,
    data_dir: Path,
    max_concurrent: int,  # Now used as absolute maximum only
    logger: logging.Logger,
):
    """Run the bulk scraper with distributed timing and integrated parsing."""
    failed_codes = load_failed_codes(data_dir)
    logger.info(f"Loaded {len(failed_codes)} previously failed codes")

    # Filter out failed codes for location scraping
    active_codes = [code for code in location_codes if code not in failed_codes]
    logger.info(
        f"Processing {len(active_codes)} active codes out of {len(location_codes)} total"
    )

    # Get existing locations
    existing_locations = get_existing_locations(data_dir, location_codes)
    logger.info(f"Found {len(existing_locations)} existing locations with cached data")

    # Codes that need location data
    codes_needing_location = [
        code for code in active_codes if code not in existing_locations
    ]
    logger.info(f"Need to fetch location data for {len(codes_needing_location)} codes")

    interval_seconds = interval_minutes * 60

    async with aiohttp.ClientSession() as session:
        start_time = asyncio.get_event_loop().time()

        # Phase 1: Scrape location data for codes that need it (one-time setup)
        if codes_needing_location:
            # Calculate dynamic batch parameters for location scraping
            location_batch_size, location_batch_interval = calculate_batch_parameters(
                len(codes_needing_location),
                interval_seconds,
                max_batch_size=max_concurrent,
            )

            total_location_batches = math.ceil(
                len(codes_needing_location) / location_batch_size
            )

            logger.info(
                f"Phase 1: Scraping location data for {len(codes_needing_location)} codes"
            )
            logger.info(
                f"Location batch size: {location_batch_size}, interval: {location_batch_interval:.2f}s, total batches: {total_location_batches}"
            )
            logger.info(
                f"Estimated rate: {len(codes_needing_location) / interval_seconds:.2f} requests/second"
            )

            new_locations = {}

            for i in range(0, len(codes_needing_location), location_batch_size):
                batch = codes_needing_location[i : i + location_batch_size]
                batch_start = asyncio.get_event_loop().time()

                batch_num = i // location_batch_size + 1
                logger.info(
                    f"Processing location batch {batch_num}/{total_location_batches} ({len(batch)} codes)"
                )

                batch_locations, failed_codes = await scrape_location_batch(
                    session, batch, data_dir, failed_codes, logger
                )

                new_locations.update(batch_locations)

                # Wait for next batch timing (except for last batch)
                if batch_num < total_location_batches:
                    elapsed = asyncio.get_event_loop().time() - batch_start
                    sleep_time = max(0, location_batch_interval - elapsed)
                    if sleep_time > 0:
                        logger.debug(
                            f"Waiting {sleep_time:.2f}s before next location batch"
                        )
                        await asyncio.sleep(sleep_time)

            # Save updated failed codes
            save_failed_codes(failed_codes, data_dir)
            logger.info(
                f"Phase 1 complete: Found {len(new_locations)} new locations, {len(failed_codes)} total failed codes"
            )

            # Update existing locations with new ones
            existing_locations.update(new_locations)

        # Phase 2: Continuous machine status scraping with parsing
        if not existing_locations:
            logger.warning("No valid locations found for machine status scraping")
            return

        # Calculate timing for next phase
        phase1_duration = asyncio.get_event_loop().time() - start_time

        # If we're in the same interval, wait for next interval boundary
        if codes_needing_location and phase1_duration < interval_seconds:
            remaining_time = interval_seconds - phase1_duration
            logger.info(
                f"Waiting {remaining_time:.2f}s before starting continuous machine status phase"
            )
            await asyncio.sleep(remaining_time)

        # Calculate dynamic batch parameters for machine status scraping
        status_batch_size, status_batch_interval = calculate_batch_parameters(
            len(existing_locations), interval_seconds, max_batch_size=max_concurrent
        )

        total_status_batches = math.ceil(len(existing_locations) / status_batch_size)

        logger.info(
            f"Phase 2: Starting continuous machine status scraping for {len(existing_locations)} locations"
        )
        logger.info(
            f"Status batch size: {status_batch_size}, interval: {status_batch_interval:.2f}s, total batches: {total_status_batches}"
        )
        logger.info(
            f"Rate: {len(existing_locations) / interval_seconds:.2f} requests/second"
        )
        logger.info(f"Each location will be updated every {interval_minutes} minutes")
        logger.info("Press Ctrl+C to stop the continuous scraping...")

        location_items = list(existing_locations.items())
        cycle_count = 0

        try:
            while True:
                cycle_count += 1
                cycle_start_time = asyncio.get_event_loop().time()
                total_success = 0

                logger.info(
                    f"Starting cycle {cycle_count} - processing {len(existing_locations)} locations"
                )

                # Process machine status in batches with parsing
                for i in range(0, len(location_items), status_batch_size):
                    batch_dict = dict(location_items[i : i + status_batch_size])
                    batch_start = asyncio.get_event_loop().time()

                    batch_num = i // status_batch_size + 1
                    logger.debug(
                        f"Cycle {cycle_count}: Processing batch {batch_num}/{total_status_batches} ({len(batch_dict)} locations)"
                    )

                    success_count = await scrape_machine_status_batch(
                        session, batch_dict, data_dir, logger
                    )

                    total_success += success_count

                    # Wait for next batch timing (except for last batch)
                    if batch_num < total_status_batches:
                        elapsed = asyncio.get_event_loop().time() - batch_start
                        sleep_time = max(0, status_batch_interval - elapsed)
                        if sleep_time > 0:
                            await asyncio.sleep(sleep_time)

                cycle_duration = asyncio.get_event_loop().time() - cycle_start_time
                logger.info(
                    f"Cycle {cycle_count} complete: {total_success} successful updates and parses in {cycle_duration:.2f}s"
                )

                # Wait for the remainder of the interval before starting next cycle
                remaining_cycle_time = interval_seconds - cycle_duration
                if remaining_cycle_time > 0:
                    logger.info(
                        f"Waiting {remaining_cycle_time:.2f}s before next cycle..."
                    )
                    await asyncio.sleep(remaining_cycle_time)
                else:
                    logger.warning(
                        f"Cycle took {cycle_duration:.2f}s, longer than {interval_seconds}s interval!"
                    )

        except KeyboardInterrupt:
            logger.info(f"Stopping continuous scraping after {cycle_count} cycles")
        except Exception as e:
            logger.error(f"Error in continuous scraping: {e}")
            raise


def create_argument_groups(parser):
    """Create mutually exclusive argument groups for different input methods."""
    input_group = parser.add_mutually_exclusive_group(required=True)

    input_group.add_argument(
        "--range",
        nargs=2,
        metavar=("START", "END"),
        help="Range of location codes (e.g., --range W000001 W010000)",
    )

    input_group.add_argument(
        "--file",
        type=Path,
        help="Text file containing location codes (one per line, comments with #)",
    )

    input_group.add_argument(
        "--codes",
        nargs="+",
        help="Specific location codes (e.g., --codes W000001 W000002 W000003)",
    )


def main():
    parser = argparse.ArgumentParser(
        description="Bulk scrape Wash Mobile Pay API with integrated parsing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using a range (original functionality)
  ./bulk_scraper.py --range W000001 W010000 --interval 15
  
  # Using a file with location codes
  ./bulk_scraper.py --file location_codes.txt --interval 15
  
  # Using specific codes directly
  ./bulk_scraper.py --codes W000001 W000002 W000003 --interval 15

Location codes file format:
  W000001
  W000050
  W001234
  # This is a comment
  W005678
        """,
    )

    create_argument_groups(parser)

    parser.add_argument(
        "--interval",
        "-i",
        type=int,
        default=15,
        help="Time interval in minutes to distribute requests (default: 15)",
    )
    parser.add_argument(
        "--max-concurrent",
        "-c",
        type=int,
        default=50,
        help="Maximum concurrent requests per batch - used as upper bound (default: 50)",
    )
    parser.add_argument(
        "--data-dir", default="data", help="Directory to store data files"
    )
    parser.add_argument(
        "--log-dir", default="logs", help="Directory to store log files"
    )

    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    log_dir = Path(args.log_dir)

    # Setup logging
    logger = setup_logging(log_dir)

    try:
        # Determine location codes based on input method
        if args.range:
            start_code, end_code = args.range
            location_codes = generate_location_codes(start_code, end_code)
            logger.info(
                f"Generated {len(location_codes)} codes from range {start_code} to {end_code}"
            )

        elif args.file:
            location_codes = load_location_codes_from_file(args.file, logger)
            logger.info(f"Loaded {len(location_codes)} codes from file {args.file}")

        elif args.codes:
            location_codes = validate_location_codes(args.codes, logger)
            logger.info(f"Using {len(location_codes)} specified codes")

        else:
            logger.error("No input method specified")
            sys.exit(1)

        if not location_codes:
            logger.error("No valid location codes found")
            sys.exit(1)

    except ValueError as e:
        logger.error(f"Error processing location codes: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error loading location codes: {e}")
        sys.exit(1)

    logger.info(
        f"Starting bulk scraper with integrated parsing for {len(location_codes)} codes"
    )
    logger.info(
        f"Interval: {args.interval} minutes, Max concurrent: {args.max_concurrent}"
    )

    # Run the scraper
    asyncio.run(
        run_bulk_scraper(
            location_codes, args.interval, data_dir, args.max_concurrent, logger
        )
    )


if __name__ == "__main__":
    main()
