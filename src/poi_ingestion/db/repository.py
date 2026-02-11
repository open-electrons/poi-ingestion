# src/poi_ingestion/db/repository.py
from sqlalchemy import create_engine
import os

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise ValueError("❌ DB_URL environment variable is not set. Please set it in your .env file.")

engine = create_engine(DB_URL)

import json
import pandas as pd
import logging
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import insert

logger = logging.getLogger(__name__)

def upsert_dataframe(df: pd.DataFrame, table_name: str, engine, chunksize: int = 1000):
    """
    UPSERT a DataFrame into a PostgreSQL table.
    - Only inserts columns that exist in the DB table
    - Converts dict/list columns to JSON strings
    - Supports chunked inserts
    """
    if df.empty:
        logger.warning(f"⚠️ No data to insert into {table_name}")
        return

    # Convert object columns (dict/list) to JSON strings
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].apply(
                lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x
            )

    # Load table schema
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    table_columns = [c.name for c in table.columns]

    # Filter DataFrame columns to those present in the table
    df_to_insert = df[[col for col in df.columns if col in table_columns]]

    # Optional: store all extra fields as raw_json
    extra_columns = [c for c in df.columns if c not in table_columns]
    if extra_columns:
        df_to_insert["raw_json"] = df[extra_columns].apply(lambda r: json.dumps(r.to_dict(), ensure_ascii=False), axis=1)
        if "raw_json" not in table_columns:
            logger.warning(f"⚠️ Table {table_name} has no 'raw_json' column. Extra fields ignored.")

    records = df_to_insert.to_dict(orient="records")

    # Insert in chunks
    for i in range(0, len(records), chunksize):
        batch = records[i:i + chunksize]
        stmt = insert(table).values(batch)

        # UPSERT on primary key
        stmt = stmt.on_conflict_do_update(
            index_elements=["id"],
            set_={col: stmt.excluded[col] for col in df_to_insert.columns if col != "id"}
        )

        with engine.begin() as conn:
            conn.execute(stmt)

    logger.info(f"✅ Upserted {len(df_to_insert)} rows into {table_name}")

