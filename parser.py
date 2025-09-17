#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.12"
# dependencies = ["pandas"]
# ///

"""
JSON to CSV Parser for Wash Connect Data
Parses location and machine status JSON files and outputs a consolidated CSV.
Usage: uv run parser.py <location_code>
"""

import argparse
import json
import logging
import sys
import re
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

import pandas as pd


def setup_logging() -> logging.Logger:
    """Setup logging configuration."""
    logger = logging.getLogger("parser")
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


def extract_state_code(uln: str) -> str:
    """Extract state code from ULN (first two characters)."""
    return uln[:2] if len(uln) >= 2 else ""


def extract_request_time(filename: str) -> Optional[str]:
    """Extract request time from filename format: {uln}-{request_time}.json"""
    # Pattern to match timestamp in filename
    pattern = r"-(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{4}Z)\.json$"
    match = re.search(pattern, filename)
    return match.group(1) if match else None


def parse_datetime(datetime_str: str) -> datetime:
    """Parse datetime string to datetime object."""
    return datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))


def calculate_status(machine: Dict[str, Any], request_time_str: str) -> str:
    """
    Calculate machine status based on the logic:
    - If status is ERROR, return 'error'
    - If time_remaining = 0, return 'available'
    - If request_time - start_time > time_remaining, return 'in_use', else 'available'
    """
    # Check for error status first
    if machine.get("status") == "ERROR":
        return "error"

    time_remaining = int(machine.get("time_remaining"))

    # If no time remaining, machine is available
    if time_remaining == 0:
        return "available"

    start_time_str = machine.get("start_time")
    if not start_time_str:
        return "available"

    try:
        request_time = parse_datetime(request_time_str)
        start_time = parse_datetime(start_time_str)

        # Calculate elapsed time in minutes
        elapsed_minutes = (request_time - start_time).total_seconds() // 60

        # If elapsed time > time_remaining, machine should be available
        if elapsed_minutes > time_remaining:
            return "available"
        else:
            return "in_use"

    except Exception as e:
        logging.warning(f"Error parsing datetime: {e}")
        return "available"


def load_location_data(
    location_file: Path, logger: logging.Logger
) -> Optional[Dict[str, Any]]:
    """Load location JSON file."""
    try:
        with open(location_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"Loaded location data from: {location_file}")
        return data
    except Exception as e:
        logger.error(f"Failed to load location file {location_file}: {e}")
        return None


def load_machine_status(
    status_file: Path, logger: logging.Logger
) -> Optional[Dict[str, Any]]:
    """Load machine status JSON file."""
    try:
        with open(status_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"Loaded machine status from: {status_file}")
        return data
    except Exception as e:
        logger.error(f"Failed to load status file {status_file}: {e}")
        return None


def parse_location_code_data(
    location_code: str, data_dir: Path, logger: logging.Logger
) -> List[Dict[str, Any]]:
    """Parse all JSON files for a location code and return consolidated data."""
    location_dir = data_dir / location_code

    if not location_dir.exists():
        logger.error(f"Location directory not found: {location_dir}")
        return []

    # Load location data
    location_file = location_dir / f"{location_code}.json"
    location_data = load_location_data(location_file, logger)

    if not location_data:
        logger.error(f"Failed to load location data for {location_code}")
        return []

    # Extract location fields
    try:
        location_info = location_data["location"]
        location_id = location_info["location_id"]
        location_name = location_info["location_name"]
        sitecode = location_info["sitecode"]
        uln = location_info["uln"].strip()
        state_code = extract_state_code(uln)

        # Extract rooms information
        rooms = location_data.get("rooms", [])
        room_mapping = {room["room_id"]: room for room in rooms}

        logger.info(f"Location: {location_name} ({location_id})")
        logger.info(f"ULN: {uln}, State: {state_code}")
        logger.info(f"Found {len(rooms)} rooms")

    except KeyError as e:
        logger.error(f"Missing key in location data: {e}")
        return []

    # Process all machine status files
    all_records = []
    status_files = list(location_dir.glob(f"{uln}-*.json"))

    logger.info(f"Found {len(status_files)} status files")

    for status_file in status_files:
        request_time = extract_request_time(status_file.name)
        if not request_time:
            logger.warning(f"Could not extract request time from: {status_file.name}")
            continue

        status_data = load_machine_status(status_file, logger)
        if not status_data:
            continue

        # Process machines for each room
        machines_data = status_data.get("data", {})

        for room_id, room_data in machines_data.items():
            if room_id not in room_mapping:
                logger.warning(f"Room ID {room_id} not found in location data")
                continue

            room_info = room_mapping[room_id]
            machines = room_data.get("machines", [])

            for machine in machines:
                record = {
                    # Location fields
                    "location_id": location_id,
                    "location_name": location_name,
                    "sitecode": sitecode,
                    "uln": uln,
                    "state_code": state_code,
                    # Room fields
                    "room_id": room_info["room_id"],
                    "room_name": room_info["room_name"],
                    "id": room_info["id"],
                    # Machine fields
                    "machine_number": machine.get("machine_number"),
                    "start_time": machine.get("start_time"),
                    "time_remaining": int(machine.get("time_remaining")),
                    "type": machine.get("type"),
                    "request_time": request_time,
                    "status_raw": machine.get("status"),
                    # Calculated status
                    "status": calculate_status(machine, request_time),
                }

                all_records.append(record)

    logger.info(f"Processed {len(all_records)} machine records")
    return all_records


def main():
    parser = argparse.ArgumentParser(description="Parse Wash Connect JSON data to CSV")
    parser.add_argument("location_code", help="Location code to parse")
    parser.add_argument(
        "--data-dir", default="data", help="Directory containing data files"
    )
    parser.add_argument(
        "--output-dir", default=None, help="Directory to save CSV output"
    )

    args = parser.parse_args()
    location_code = args.location_code
    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir if args.output_dir else f"data/{location_code}")

    # Setup logging
    logger = setup_logging()

    logger.info(f"Starting parser for location code: {location_code}")

    # Parse data
    records = parse_location_code_data(location_code, data_dir, logger)

    if not records:
        logger.error("No records found to process")
        sys.exit(1)

    # Create DataFrame
    df = pd.DataFrame(records)

    # Sort by request_time and machine_number for better organization
    df = df.sort_values(["request_time", "room_id", "machine_number"])

    # Save to CSV
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "parsed.csv"

    # If file exists, append without header, otherwise create new with header
    df.to_csv(output_file, index=False, mode="a", header=not output_file.exists())
    logger.info(
        f"CSV {'appended to' if output_file.exists() else 'saved to'}: {output_file}"
    )
    logger.info(f"Total records added: {len(df)}")

    # Print summary statistics
    logger.info("\nSummary:")
    logger.info(f"Unique machines: {df['machine_number'].nunique()}")
    logger.info(f"Unique rooms: {df['room_id'].nunique()}")
    logger.info(f"Time range: {df['request_time'].min()} to {df['request_time'].max()}")

    status_counts = df["status"].value_counts()
    logger.info("Status distribution:")
    for status, count in status_counts.items():
        logger.info(f"  {status}: {count}")


if __name__ == "__main__":
    main()
