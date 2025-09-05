#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.12"
# dependencies = ["requests"]
# ///

"""
API Scraper for Wash Mobile Pay
Scrapes location data and machine status from the API endpoints.
Usage: uv run scraper.py <location_code>
"""

import argparse
import json
import logging
import sys
import datetime
from pathlib import Path
from typing import Dict, Any, Optional

import requests


def setup_logging(log_dir: Path, location_code: str) -> logging.Logger:
    """Setup logging configuration."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{location_code}.log"

    logger = logging.getLogger("api_scraper")
    logger.setLevel(logging.INFO)

    # Remove existing handlers to avoid duplicates
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)

    # Console handler for errors
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.ERROR)

    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def make_request(
    url: str, logger: logging.Logger, timeout: int = 30
) -> Optional[Dict[str, Any]]:
    """Make HTTP request and return JSON response."""
    try:
        logger.info(f"Making request to: {url}")
        response = requests.get(url, timeout=timeout)

        if response.status_code == 200:
            logger.info(f"Request successful for: {url}")
            return response.json()
        else:
            logger.error(f"HTTP {response.status_code} error for URL: {url}")
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for URL {url}: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error for URL {url}: {e}")
        return None


def get_location_data(
    location_code: str, logger: logging.Logger
) -> Optional[Dict[str, Any]]:
    """Get location data from the first API endpoint."""
    url = f"https://us-central1-washmobilepay.cloudfunctions.net/locations?srcode={location_code}"
    return make_request(url, logger)


def get_machine_status(uln: str, logger: logging.Logger) -> Optional[Dict[str, Any]]:
    """Get machine status from the second API endpoint."""
    url = f"https://us-central1-washmobilepay.cloudfunctions.net/get_machine_status_v1?uln={uln}"
    return make_request(url, logger)


def save_json(data: Dict[str, Any], filepath: Path, logger: logging.Logger) -> bool:
    """Save data to JSON file."""
    try:
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"Successfully saved data to: {filepath}")
        return True
    except Exception as e:
        logger.error(f"Failed to save file {filepath}: {e}")
        return False


def load_json(filepath: Path, logger: logging.Logger) -> Optional[Dict[str, Any]]:
    """Load data from JSON file."""
    try:
        if filepath.exists():
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.info(f"Successfully loaded cached data from: {filepath}")
            return data
    except Exception as e:
        logger.error(f"Failed to load file {filepath}: {e}")
    return None


def main():
    parser = argparse.ArgumentParser(description="Scrape Wash Mobile Pay API")
    parser.add_argument("location_code", help="Location code to query")
    parser.add_argument(
        "--data-dir", default="data", help="Directory to store data files"
    )
    parser.add_argument(
        "--log-dir", default="logs", help="Directory to store log files"
    )

    args = parser.parse_args()
    location_code = args.location_code
    data_dir = Path(args.data_dir)
    log_dir = Path(args.log_dir)

    # Setup logging
    logger = setup_logging(log_dir, location_code)

    # Create location-specific directory
    location_dir = data_dir / location_code
    location_file = location_dir / f"{location_code}.json"

    logger.info(f"Starting API scraper for location code: {location_code}")

    # Step 1: Get location data (only if not already cached)
    location_data = load_json(location_file, logger)

    if location_data is None:
        logger.info("Location data not cached, fetching from API...")
        location_data = get_location_data(location_code, logger)

        if location_data is None:
            logger.error(f"Failed to get location data for {location_code}")
            sys.exit(1)

        # Save location data
        if not save_json(location_data, location_file, logger):
            logger.error("Failed to save location data")
            sys.exit(1)
    else:
        logger.info("Using cached location data")

    # Step 2: Extract ULN from location data
    try:
        uln = location_data["location"]["uln"].strip()
        logger.info(f"Extracted ULN: {uln}")
    except KeyError as e:
        logger.error(f"Failed to extract ULN from location data. Missing key: {e}")
        logger.error(f"Location data structure: {json.dumps(location_data, indent=2)}")
        sys.exit(1)

    # Step 3: Get machine status
    logger.info("Fetching machine status...")
    machine_status = get_machine_status(uln, logger)

    if machine_status is None:
        logger.error(f"Failed to get machine status for ULN: {uln}")
        sys.exit(1)

    # Step 4: Save machine status with timestamp
    request_time = (
        datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z"
    )
    status_file = location_dir / f"{uln}-{request_time}.json"

    if not save_json(machine_status, status_file, logger):
        logger.error("Failed to save machine status")
        sys.exit(1)

    logger.info(
        f"Script completed successfully! Machine status saved to: {status_file}"
    )


if __name__ == "__main__":
    main()
