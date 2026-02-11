"""
Optimized process_json_to_db function with improvements:

1. Better error handling and early returns
2. Separated ID generation logic into reusable functions
3. Improved logging with progress indicators
4. Transaction-like behavior (POIs before connections)
5. More maintainable structure
"""

import json
import hashlib
from pathlib import Path
from typing import Optional
import pandas as pd
import logging

from poi_ingestion.transform.normalize import normalize_pois, normalize_connections
from poi_ingestion.db.repository import engine, upsert_dataframe

logger = logging.getLogger(__name__)

"""
Fixed version addressing:
1. Pandas SettingWithCopyWarning
2. Duplicate primary keys in upsert batches
"""

import json
import hashlib
from pathlib import Path
from typing import Optional
import pandas as pd


def generate_poi_id(row: pd.Series) -> int:
    """
    Generate a deterministic ID for a POI based on its unique attributes.

    Args:
        row: DataFrame row containing POI data

    Returns:
        12-digit integer ID
    """
    unique_string = f"{row.get('Title', '')}_{row.get('Latitude', '')}_{row.get('Longitude', '')}"
    hash_hex = hashlib.sha256(unique_string.encode('utf-8')).hexdigest()
    return int(hash_hex, 16) % 10 ** 12


def generate_connection_id(row: pd.Series, index: int) -> int:
    """
    Generate a deterministic ID for a connection based on POI ID and index.

    Args:
        row: DataFrame row containing connection data
        index: Row index in the DataFrame

    Returns:
        12-digit integer ID
    """
    unique_string = f"{row.get('poi_id', '')}_{index}"
    hash_hex = hashlib.sha256(unique_string.encode('utf-8')).hexdigest()
    return int(hash_hex, 16) % 10 ** 12


def ensure_poi_ids(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure POI DataFrame has valid 'id' column and remove duplicates.

    FIXES:
    - Creates explicit copy to avoid SettingWithCopyWarning
    - Removes duplicate IDs before upsert
    - Logs duplicate removals

    Args:
        df: POI DataFrame

    Returns:
        DataFrame with unique 'id' column
    """
    if df.empty:
        return df

    # FIX #1: Create explicit copy to avoid pandas warning
    df = df.copy()

    if 'ID' in df.columns and df['ID'].notna().any():
        # Use existing ID field
        df['id'] = df['ID']
        logger.info(f"📋 Using existing ID field for {len(df)} POIs")
    else:
        # Generate deterministic IDs
        df['id'] = df.apply(generate_poi_id, axis=1)
        logger.info(f"🔑 Generated IDs for {len(df)} POIs")

    # FIX #2: Remove duplicate IDs (keep first occurrence)
    original_count = len(df)
    df = df.drop_duplicates(subset=['id'], keep='first')
    duplicates_removed = original_count - len(df)

    if duplicates_removed > 0:
        logger.warning(f"⚠️  Removed {duplicates_removed} duplicate POI IDs")

    return df


def ensure_connection_ids(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure connections DataFrame has valid 'id' column and remove duplicates.

    FIXES:
    - Creates explicit copy to avoid SettingWithCopyWarning
    - Removes duplicate IDs before upsert
    - Logs duplicate removals

    Args:
        df: Connections DataFrame

    Returns:
        DataFrame with unique 'id' column
    """
    if df.empty:
        return df

    # FIX #1: Create explicit copy to avoid pandas warning
    df = df.copy()

    if 'ID' in df.columns and df['ID'].notna().any():
        # Use existing ID field
        df['id'] = df['ID']
        logger.info(f"📋 Using existing ID field for {len(df)} connections")
    else:
        # Generate deterministic IDs based on poi_id and row position
        df['id'] = [generate_connection_id(row, idx) for idx, row in df.iterrows()]
        logger.info(f"🔑 Generated IDs for {len(df)} connections")

    # FIX #2: Remove duplicate IDs (keep first occurrence)
    original_count = len(df)
    df = df.drop_duplicates(subset=['id'], keep='first')
    duplicates_removed = original_count - len(df)

    if duplicates_removed > 0:
        logger.warning(f"⚠️  Removed {duplicates_removed} duplicate connection IDs")

    return df


def prepare_dataframe_for_upsert(df: pd.DataFrame, extra_columns: list) -> pd.DataFrame:
    """
    Prepare DataFrame for database upsert by adding raw_json column.

    FIXES:
    - Creates explicit copy to avoid SettingWithCopyWarning
    - Handles raw_json serialization safely

    Args:
        df: DataFrame to prepare
        extra_columns: Columns to serialize into raw_json

    Returns:
        DataFrame with raw_json column added
    """
    if df.empty:
        return df

    # FIX: Create explicit copy to avoid pandas warning
    df_to_insert = df.copy()

    # Add raw_json column with extra data
    if extra_columns:
        df_to_insert["raw_json"] = df[extra_columns].apply(
            lambda r: json.dumps(r.to_dict(), ensure_ascii=False),
            axis=1
        )

    return df_to_insert


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
    """
    Load JSON, normalize POIs and Connections, ensure IDs exist, insert into DB.

    FIXES APPLIED:
    - Explicit DataFrame copies to avoid SettingWithCopyWarning
    - Duplicate ID removal before upsert
    - Better error messages for debugging

    Args:
        json_file: Path to JSON file containing POI data
    """
    logger.info(f"📂 Processing {json_file.name}")

    # Step 1: Load JSON safely
    pois_json = load_json_safely(json_file)
    if pois_json is None:
        return

    logger.info(f"📦 Loaded {len(pois_json)} POIs from JSON")

    # Step 2: Normalize POIs
    logger.info("🔄 Normalizing POIs...")
    df_pois = normalize_pois(pois_json)

    if df_pois.empty:
        logger.warning(f"⚠️  No POIs extracted after normalization")
    else:
        # Ensure IDs exist and remove duplicates
        df_pois = ensure_poi_ids(df_pois)

        logger.info(f"📊 Ready to upsert {len(df_pois)} unique POIs")

        # Upsert to database
        try:
            upsert_dataframe(df_pois, "pois", engine)
            logger.info(f"✅ Upserted {len(df_pois)} POIs")
        except Exception as e:
            logger.error(f"❌ Failed to upsert POIs: {e}")
            # Log first few IDs for debugging
            if len(df_pois) > 0:
                sample_ids = df_pois['id'].head(5).tolist()
                logger.debug(f"Sample POI IDs: {sample_ids}")
            return

    # Step 3: Normalize Connections
    logger.info("🔄 Normalizing connections...")
    df_connections = normalize_connections(pois_json)

    if df_connections.empty:
        logger.warning(f"⚠️  No connections extracted")
    else:
        # Ensure IDs exist and remove duplicates
        df_connections = ensure_connection_ids(df_connections)

        logger.info(f"📊 Ready to upsert {len(df_connections)} unique connections")

        # Upsert to database
        try:
            upsert_dataframe(df_connections, "connections", engine)
            logger.info(f"✅ Upserted {len(df_connections)} connections")
        except Exception as e:
            logger.error(f"❌ Failed to upsert connections: {e}")
            # Log first few IDs for debugging
            if len(df_connections) > 0:
                sample_ids = df_connections['id'].head(5).tolist()
                logger.debug(f"Sample connection IDs: {sample_ids}")
            return

    logger.info(f"🎉 Successfully processed {json_file.name}")


# ============================================================================
# ALTERNATIVE: More robust ID generation to prevent collisions
# ============================================================================

def generate_poi_id_v2(row: pd.Series) -> int:
    """
    Enhanced POI ID generation with better uniqueness.

    Includes more fields to reduce hash collisions:
    - Title
    - Latitude (rounded to 6 decimals)
    - Longitude (rounded to 6 decimals)
    - Country code (if available)

    Args:
        row: DataFrame row containing POI data

    Returns:
        12-digit integer ID
    """
    # Round coordinates to 6 decimal places (~0.1m precision)
    lat = f"{float(row.get('Latitude', 0)):.6f}" if row.get('Latitude') else ''
    lon = f"{float(row.get('Longitude', 0)):.6f}" if row.get('Longitude') else ''
    title = str(row.get('Title', '')).strip()
    country = str(row.get('country_code', '')).strip()

    unique_string = f"{title}_{lat}_{lon}_{country}"
    hash_hex = hashlib.sha256(unique_string.encode('utf-8')).hexdigest()
    return int(hash_hex, 16) % 10 ** 12


def generate_connection_id_v2(row: pd.Series, index: int) -> int:
    """
    Enhanced connection ID generation with better uniqueness.

    Includes more fields to reduce hash collisions:
    - POI ID
    - Connection type
    - Power (kW)
    - Row index

    Args:
        row: DataFrame row containing connection data
        index: Row index in the DataFrame

    Returns:
        12-digit integer ID
    """
    poi_id = str(row.get('poi_id', ''))
    conn_type = str(row.get('connection_type', '')).strip()
    power = str(row.get('power_kw', '')).strip()

    unique_string = f"{poi_id}_{conn_type}_{power}_{index}"
    hash_hex = hashlib.sha256(unique_string.encode('utf-8')).hexdigest()
    return int(hash_hex, 16) % 10 ** 12