import json
import hashlib
from pathlib import Path
from typing import Optional
import pandas as pd
import logging

from poi_ingestion.transform.normalize import normalize_pois, normalize_connections
from poi_ingestion.db.repository import engine, upsert_dataframe

logger = logging.getLogger(__name__)

def extract_pois(pois_json: list) -> pd.DataFrame:
    rows = []

    for poi in pois_json:
        uuid = poi.get("UUID")
        if not uuid:
            continue  # skip broken entries

        address = poi.get("AddressInfo", {}) or {}

        row = {
            "poi_uuid": uuid,
            "country_code": address.get("Country", {}).get("ISOCode"),
            "title": address.get("Title"),
            "latitude": address.get("Latitude"),
            "longitude": address.get("Longitude"),
            "raw_json": poi
        }

        rows.append(row)

    return pd.DataFrame(rows)


def extract_connections(pois_json: list) -> pd.DataFrame:
    rows = []

    for poi in pois_json:
        poi_uuid = poi.get("UUID")
        if not poi_uuid:
            continue

        connections = poi.get("Connections", []) or []

        for conn in connections:
            conn_id = conn.get("ID")
            if not conn_id:
                continue

            row = {
                "poi_uuid": poi_uuid,  # link to parent POI
                "connection_id": conn.get("ID"),  # original JSON ID
                "connection_type": conn.get("ConnectionType", {}).get("Title"),
                "power_kw": conn.get("PowerKW"),
                "voltage": conn.get("Voltage"),
                "amperage": conn.get("Amps"),
                "raw_json": conn  # store full JSON for future-proofing
            }

            rows.append(row)

    return pd.DataFrame(rows)


def load_json_safely(json_file: Path) -> Optional[list]:
    """
    Safely load and validate JSON file.

    Args:
        json_file: Path to JSON file

    Returns:
        List of POI dictionaries, or None if error
    """
    if not json_file.exists():
        logger.error(f"❌ File does not exist: {json_file}")
        return None

    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"❌ Invalid JSON in {json_file}: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Error reading {json_file}: {e}")
        return None

    if not data:
        logger.warning(f"⚠️  Empty JSON file: {json_file}")
        return None

    return data


def process_json_to_db(json_file: Path):
    logger.info(f"📂 Processing {json_file.name}")

    pois_json = load_json_safely(json_file)
    if not pois_json:
        logger.warning("⚠️ No JSON data found")
        return

    logger.info(f"📦 Loaded {len(pois_json)} POIs")

    # Step 1: Extract POIs
    df_pois = extract_pois(pois_json)

    if not df_pois.empty:
        logger.info(f"📊 Upserting {len(df_pois)} POIs")
        upsert_dataframe(df_pois, "pois", engine)
        logger.info("✅ POIs upserted")
    else:
        logger.warning("⚠️ No POIs extracted")

    # Step 2: Extract Connections
    df_connections = extract_connections(pois_json)

    if not df_connections.empty:
        logger.info(f"📊 Upserting {len(df_connections)} connections")
        upsert_dataframe(df_connections, "connections", engine)
        logger.info("✅ Connections upserted")
    else:
        logger.warning("⚠️ No connections extracted")

    logger.info(f"🎉 Finished processing {json_file.name}")
