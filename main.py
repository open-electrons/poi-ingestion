from pathlib import Path
import pycountry
import sys, os
import json
from typing import Optional, List
import time

# Add src folder to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.poi_ingestion.clients.openchargemap import fetch_pois
from app.poi_ingestion.service.poi_processor import process_json_to_db


# Optional: load environment variables if using Supabase
from dotenv import load_dotenv
load_dotenv()

DATA_RAW = Path("data/raw")
DATA_RAW.mkdir(parents=True, exist_ok=True)

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)

def save_pois_to_json(pois: list, file_path: Path, country_code: str):
    """Save a list of POIs to a JSON file."""
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(pois, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved {len(pois)} POIs for {country_code} to {file_path}")


def fetch_and_save_country_pois(country_codes: Optional[List[str]] = None):
    """
    Fetch POIs for given ISO country codes, save each as a separate JSON.
    If no list is provided, fetch all countries.
    """
    if country_codes is None:
        country_codes = [c.alpha_2 for c in pycountry.countries]

    for code in country_codes:
        code = code.upper()
        file_path = DATA_RAW / f"pois_{code}.json"

        if file_path.exists():
            print(f"{file_path} already exists, skipping...")
            continue

        try:
            pois = fetch_pois(country_code=code)
            time.sleep(1)  # avoid hitting API rate limits
        except Exception as e:
            logger.error(f"Error fetching {code}: {e}")
            continue

        if not pois:
            logger.info(f"No POIs for {code}, skipping...")
            continue

        save_pois_to_json(pois, file_path, code)


def merge_country_jsons(output_file: Path = DATA_RAW / "pois_all.json"):
    """
    Merge all per-country JSON files into one big JSON file.
    """
    all_pois = []

    for json_file in DATA_RAW.glob("pois_*.json"):
        logger.info(f"Reading {json_file}")
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            code = json_file.stem.split("_")[1]
            for poi in data:
                poi["country_code"] = code
            all_pois.extend(data)

    logger.info(f"Merging complete: {len(all_pois)} total POIs")

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_pois, f, ensure_ascii=False, indent=2)

    logger.info(f"Merged JSON saved to {output_file}")


def main():
    logger.info("\nStep 1: Fetch and save per-country JSONs")
    fetch_and_save_country_pois(None) # (["DE", "FR", "JP"])  # can pass None for all countries

    logger.info("\nStep 2: Merge all country JSONs into one file")
    merged_file = DATA_RAW / "pois_all.json"
    merge_country_jsons(merged_file)

    logger.info("\nStep 3: Normalize and insert into database")
    process_json_to_db(merged_file)

    logger.info("\nDone! Database contains POIs and Connections.")


if __name__ == "__main__":
    main()
