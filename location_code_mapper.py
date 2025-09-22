#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.12"
# dependencies = ["requests", "pandas", "python-dotenv"]
# ///

"""
Location Code Mapper for Wash Connect Data
Uses Google Geocoding API to convert partial addresses to full addresses with coordinates.

Setup:
1. Copy .env.example to .env
2. Add your Google Maps API key to .env

Usage:
  uv run location_code_mapper.py                    # Process all locations
  uv run location_code_mapper.py W000001            # Process single location
"""

import argparse
import json
import logging
import sys
import os
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import time

import requests
import pandas as pd
from dotenv import load_dotenv


def setup_logging() -> logging.Logger:
    """Setup logging configuration."""
    logger = logging.getLogger("location_mapper")
    logger.setLevel(logging.INFO)

    # Remove existing handlers to avoid duplicates
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)

    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)

    return logger


def load_location_data(
    location_file: Path, logger: logging.Logger
) -> Optional[Dict[str, Any]]:
    """Load location JSON file."""
    try:
        with open(location_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.debug(f"Loaded location data from: {location_file}")
        return data
    except Exception as e:
        logger.error(f"Failed to load location file {location_file}: {e}")
        return None


def extract_partial_address(location_data: Dict[str, Any]) -> Tuple[str, str, str]:
    """Extract partial address components from location data."""
    try:
        location_info = location_data["location"]
        location_name = location_info["location_name"]
        uln = location_info["uln"].strip()
        location_id = location_info["location_id"]

        # Extract state code from ULN (first two characters)
        state_code = uln[:2] if len(uln) >= 2 else ""

        return location_name, state_code, location_id
    except KeyError as e:
        raise ValueError(f"Missing required field in location data: {e}")


def geocode_address(
    api_key: str,
    partial_address: str,
    state_code: str,
    logger: logging.Logger,
    timeout: int = 30,
) -> Optional[Dict[str, Any]]:
    """Use Google Geocoding API to get full address and coordinates."""
    try:
        # Construct search query
        search_query = f"{partial_address}, {state_code}, USA"

        logger.debug(f"Geocoding query: {search_query}")

        # Construct API URL
        base_url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {"address": search_query, "key": api_key}

        # Make HTTP request
        response = requests.get(base_url, params=params, timeout=timeout)
        response.raise_for_status()

        geocode_result = response.json()

        # Check API response status
        if geocode_result.get("status") != "OK":
            status = geocode_result.get("status", "UNKNOWN")
            error_message = geocode_result.get(
                "error_message", "No error message provided"
            )
            logger.warning(
                f"Geocoding API returned status '{status}' for '{search_query}': {error_message}"
            )
            return None

        results = geocode_result.get("results", [])
        if not results:
            logger.warning(f"No geocoding results for: {search_query}")
            return None

        # Get the first (best) result
        result = results[0]

        # Extract relevant information
        geometry = result.get("geometry", {})
        location = geometry.get("location", {})

        geocoding_data = {
            "formatted_address": result.get("formatted_address", ""),
            "address_components": result.get("address_components", []),
            "latitude": location.get("lat"),
            "longitude": location.get("lng"),
            "place_id": result.get("place_id", ""),
            "types": ", ".join(result.get("types", [])),
            "location_type": geometry.get("location_type", ""),
            "search_query": search_query,
        }

        logger.info(
            f"Successfully geocoded: {search_query} -> {geocoding_data['formatted_address']}"
        )
        return geocoding_data

    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP request error for '{partial_address}': {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error for '{partial_address}': {e}")
        return None
    except Exception as e:
        logger.error(f"Geocoding error for '{partial_address}': {e}")
        return None


def get_all_location_codes(data_dir: Path) -> List[str]:
    """Get all location codes that have data directories."""
    location_codes = []

    if not data_dir.exists():
        return location_codes

    for item in data_dir.iterdir():
        if item.is_dir() and item.name.startswith("W"):
            # Check if location JSON file exists
            location_file = item / f"{item.name}.json"
            if location_file.exists():
                location_codes.append(item.name)

    return sorted(location_codes)


def process_location(
    location_code: str,
    data_dir: Path,
    api_key: str,
    logger: logging.Logger,
    timeout: int = 30,
) -> Optional[Dict[str, Any]]:
    """Process a single location code and return mapping data."""
    location_dir = data_dir / location_code
    location_file = location_dir / f"{location_code}.json"

    if not location_file.exists():
        logger.error(f"Location file not found: {location_file}")
        return None

    # Load location data
    location_data = load_location_data(location_file, logger)
    if not location_data:
        return None

    try:
        # Extract partial address
        location_name, state_code, location_id = extract_partial_address(location_data)
        logger.info(f"Processing {location_code}: {location_name}, {state_code}")

        # Geocode the address
        geocoding_data = geocode_address(
            api_key, location_name, state_code, logger, timeout
        )

        # Prepare record
        record = {
            "location_code": location_code,
            "location_id": location_id,
            "original_name": location_name,
            "state_code": state_code,
        }

        if geocoding_data:
            record.update(geocoding_data)
            record["geocoding_success"] = True
        else:
            # Fill with None values if geocoding failed
            record.update(
                {
                    "formatted_address": None,
                    "address_components": None,
                    "latitude": None,
                    "longitude": None,
                    "place_id": None,
                    "types": None,
                    "location_type": None,
                    "search_query": f"{location_name}, {state_code}, USA",
                    "geocoding_success": False,
                }
            )

        return record

    except ValueError as e:
        logger.error(f"Error processing {location_code}: {e}")
        return None


def load_existing_csv(output_file: Path) -> pd.DataFrame:
    """Load existing CSV file if it exists."""
    try:
        if output_file.exists():
            df = pd.read_csv(output_file)
            return df
    except Exception:
        pass
    return pd.DataFrame()


def main():
    # Load environment variables from .env file
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Map location codes to full addresses using Google Geocoding API"
    )
    parser.add_argument(
        "location_code",
        nargs="?",
        help="Single location code to process (optional - if not provided, processes all)",
    )
    parser.add_argument(
        "--data-dir", default="data", help="Directory containing location data files"
    )
    parser.add_argument(
        "--output-file",
        default="location_code_mapping.csv",
        help="Output CSV file name",
    )

    args = parser.parse_args()

    # Setup logging
    logger = setup_logging()

    # Get configuration from environment variables or arguments
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key:
        logger.error(
            "Google Maps API key required. Set GOOGLE_MAPS_API_KEY in .env file, environment variable."
        )
        logger.error("Copy .env.example to .env and add your API key")
        sys.exit(1)

    # Get delay and timeout from environment or arguments
    delay = 0.1  # seconds
    timeout = 30  # seconds

    logger.info("Google Maps API key loaded successfully")
    logger.info(f"Request delay: {delay}s, timeout: {timeout}s")

    data_dir = Path(args.data_dir)
    output_file = data_dir.joinpath(args.output_file)

    # Determine location codes to process
    if args.location_code:
        location_codes = [args.location_code]
        logger.info(f"Processing single location: {args.location_code}")
    else:
        location_codes = get_all_location_codes(data_dir)
        logger.info(
            f"Processing all {len(location_codes)} locations found in {data_dir}"
        )

    if not location_codes:
        logger.error("No location codes to process")
        sys.exit(1)

    # Load existing data to append new records to
    processed_codes = set()
    existing_df = load_existing_csv(output_file)
    if not existing_df.empty:
        processed_codes = set(existing_df["location_code"].tolist())
        logger.info(f"Found existing data for {len(processed_codes)} locations")

    # Process locations
    all_records = []
    success_count = 0
    fail_count = 0
    skip_count = 0

    for i, location_code in enumerate(location_codes, 1):
        if location_code in processed_codes:
            logger.info(f"Skipping {location_code} (already processed)")
            skip_count += 1
            continue

        logger.info(f"Processing {i}/{len(location_codes)}: {location_code}")

        record = process_location(location_code, data_dir, api_key, logger, timeout)

        if record:
            all_records.append(record)
            if record.get("geocoding_success", False):
                success_count += 1
            else:
                fail_count += 1
        else:
            fail_count += 1

        # Rate limiting
        if i < len(location_codes) and delay > 0:
            time.sleep(delay)

    # Create DataFrame from new records
    if all_records:
        new_df = pd.DataFrame(all_records)

        # Combine with existing data
        if not existing_df.empty:
            # Ensure columns match
            for col in new_df.columns:
                if col not in existing_df.columns:
                    existing_df[col] = None
            for col in existing_df.columns:
                if col not in new_df.columns:
                    new_df[col] = None

            # Reorder columns to match existing
            new_df = new_df[existing_df.columns]
            final_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            final_df = new_df

        # Sort by location code
        final_df = final_df.sort_values("location_code").reset_index(drop=True)

        # Save to CSV
        final_df.to_csv(output_file, index=False)

        logger.info(f"\nProcessing complete!")
        logger.info(f"Output saved to: {output_file}")
        logger.info(f"Total records in output: {len(final_df)}")
        logger.info(f"New records processed: {len(all_records)}")
        logger.info(f"Successful geocoding: {success_count}")
        logger.info(f"Failed geocoding: {fail_count}")
        if skip_count > 0:
            logger.info(f"Skipped (already processed): {skip_count}")

    else:
        logger.warning("No records processed successfully")


if __name__ == "__main__":
    main()
