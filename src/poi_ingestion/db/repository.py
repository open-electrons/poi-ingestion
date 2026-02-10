# src/poi_ingestion/db/repository.py
from sqlalchemy import create_engine
import pandas as pd
import os
import json

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise ValueError("❌ DB_URL environment variable is not set. Please set it in your .env file.")

engine = create_engine(DB_URL)


def upsert_dataframe(df: pd.DataFrame, table_name: str, chunksize: int = 1000):
    """
    Insert a DataFrame into the database in batches.
    Convert non-scalar columns (lists/dicts) to JSON strings for SQLite.
    """
    if df.empty:
        return

    # Convert all columns with object types to JSON strings
    for col in df.columns:
        if df[col].dtype == "object":
            # Only convert if it’s a list or dict
            df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x)

    df.to_sql(table_name, engine, if_exists="append", index=False, chunksize=chunksize)
    print(f"Inserted {len(df)} rows into {table_name}")
