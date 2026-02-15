# app/poi_ingestion/db/repository.py
from sqlalchemy import create_engine
import os

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise ValueError("❌ DB_URL environment variable is not set. Please set it in your .env file.")

engine = create_engine(DB_URL, echo=False)

import json
import pandas as pd
import logging
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import insert

logger = logging.getLogger(__name__)
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

import hashlib


def upsert_dataframe(df: pd.DataFrame, table_name: str, engine, chunksize: int = 1000):
    """
    UPSERT a DataFrame into a PostgreSQL table.

    - Only inserts columns that exist in the DB table
    - Converts dict/list columns to JSON strings
    - Automatically coerces numeric types to match DB schema
    - Supports chunked inserts
    """

    if df.empty:
        logger.warning(f"⚠️ No data to insert into {table_name}")
        return

    # Always work on a copy (prevents SettingWithCopyWarning)
    df = df.copy()

    # Load table schema
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    table_columns = {c.name: c for c in table.columns}

    # --------------------------------------------------
    # 1️⃣ Type coercion based on DB column types
    # --------------------------------------------------
    for col_name, column in table_columns.items():
        if col_name not in df.columns:
            continue

        try:
            if column.type.python_type == int:
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce").astype("Int64")
            elif column.type.python_type == float:
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
        except Exception:
            # Some types (e.g. JSONB) don’t expose python_type cleanly
            pass

    # --------------------------------------------------
    # 2️⃣ Convert dict/list columns to JSON strings
    # --------------------------------------------------
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].apply(
                lambda x: json.dumps(x, ensure_ascii=False)
                if isinstance(x, (dict, list))
                else x
            )

    # --------------------------------------------------
    # 3️⃣ Keep only columns existing in DB table
    # --------------------------------------------------
    df_to_insert = df[[col for col in df.columns if col in table_columns]].copy()

    # --------------------------------------------------
    # 4️⃣ Store extra fields in raw_json (if exists)
    # --------------------------------------------------
    extra_columns = [c for c in df.columns if c not in table_columns]

    if extra_columns and "raw_json" in table_columns:
        df_to_insert.loc[:, "raw_json"] = df[extra_columns].apply(
            lambda r: json.dumps(r.to_dict(), ensure_ascii=False), axis=1
        )
    elif extra_columns:
        logger.warning(f"⚠️ Table {table_name} has no 'raw_json' column. Extra fields ignored.")

    # -----------------------------
    # 🔥 ADD HASH
    # -----------------------------
    if "payload_hash" in table_columns:
        def compute_hash(row):
            # Only hash real data columns (exclude row_hash itself)
            row_dict = {k: row[k] for k in df_to_insert.columns if k != "payload_hash"}
            row_string = json.dumps(row_dict, sort_keys=True, default=str)
            return hashlib.sha256(row_string.encode("utf-8")).hexdigest()

        df_to_insert.loc[:, "payload_hash"] = df_to_insert.apply(compute_hash, axis=1)

    # --------------------------------------------------
    # 5️⃣ Remove duplicate conflict keys (prevents cardinality violation)
    # --------------------------------------------------
    if table_name == "pois":
        conflict_keys = ["poi_uuid"]
    elif table_name == "connections":
        conflict_keys = ["poi_uuid", "connection_id"]
    else:
        conflict_keys = [col.name for col in table.primary_key.columns]

    df_to_insert = df_to_insert.drop_duplicates(subset=conflict_keys)

    records = df_to_insert.to_dict(orient="records")

    # --------------------------------------------------
    # 6️⃣ Perform chunked UPSERT
    # --------------------------------------------------
    # Insert in chunks
    total_affected_rows = 0
    for i in range(0, len(records), chunksize):
        batch = records[i:i + chunksize]
        stmt = insert(table).values(batch)
        logger.debug(stmt)
        # -----------------------------
        # 🔥 CONDITIONAL UPDATE
        # -----------------------------
        if "payload_hash" in table_columns:
            stmt = stmt.on_conflict_do_update(
                index_elements=conflict_keys,
                set_={
                    col: stmt.excluded[col]
                    for col in df_to_insert.columns
                    if col not in conflict_keys
                },
                where=table.c.payload_hash.is_distinct_from(
                    stmt.excluded.payload_hash
                )
            )
        else:
            stmt = stmt.on_conflict_do_update(
                index_elements=conflict_keys,
                set_={
                    col: stmt.excluded[col]
                    for col in df_to_insert.columns
                    if col not in conflict_keys
                }
            )

        with engine.begin() as conn:
            result = conn.execute(stmt)
            total_affected_rows += result.rowcount

    logger.info(
        f"✅ Attempted: {len(df_to_insert)} | "
        f"Inserted/Updated: {total_affected_rows} | "
        f"Skipped: {len(df_to_insert) - total_affected_rows}"
    )

