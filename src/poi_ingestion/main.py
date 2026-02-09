import os
import sys
from datetime import datetime

from poi_ingestion.clients.openchargemap import fetch_pois
from poi_ingestion.transform.normalize import (
    normalize_pois,
    normalize_connections,
)
from poi_ingestion.db.repository import insert_dataframe
from poi_ingestion.config import DATA_DIR


def main() -> None:
    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    # ------------------------------------------------------------------
    # 1. Fetch POIs from Open Charge Map
    # ------------------------------------------------------------------
    pois = fetch_pois()

    if not pois:
        print("No POIs returned from API, exiting.")
        return

    # ------------------------------------------------------------------
    # 2. Normalize JSON into tabular structures
    # ------------------------------------------------------------------
    poi_df = normalize_pois(pois)
    conn_df = normalize_connections(pois)

    # ------------------------------------------------------------------
    # 3. Persist to database
    # ------------------------------------------------------------------
    insert_dataframe(poi_df, table_name="poi")
    insert_dataframe(conn_df, table_name="poi_connection")

    # ------------------------------------------------------------------
    # 4. Export CSV snapshot
    # ------------------------------------------------------------------
    csv_dir = os.path.join(DATA_DIR, "csv")
    os.makedirs(csv_dir, exist_ok=True)

    poi_csv_path = os.path.join(csv_dir, f"poi_{run_ts}.csv")
    conn_csv_path = os.path.join(csv_dir, f"poi_connections_{run_ts}.csv")

    poi_df.to_csv(poi_csv_path, index=False)
    conn_df.to_csv(conn_csv_path, index=False)

    print(f"Ingestion completed successfully at {run_ts}")
    print(f"POI CSV: {poi_csv_path}")
    print(f"Connections CSV: {conn_csv_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Fatal error during ingestion: {exc}", file=sys.stderr)
        sys.exit(1)
