import os
import sys
from datetime import datetime

import pycountry

from poi_ingestion.clients.openchargemap import fetch_pois
from poi_ingestion.transform.normalize import normalize_pois, normalize_connections
from poi_ingestion.db.repository import insert_dataframe
from poi_ingestion.config import DATA_DIR


def all_iso_country_codes():
    """Return all ISO 3166-1 alpha-2 codes."""
    return [country.alpha_2 for country in pycountry.countries]


def main() -> None:
    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    csv_dir = os.path.join(DATA_DIR, "csv", run_ts)
    os.makedirs(csv_dir, exist_ok=True)

    total_pois = 0
    total_connections = 0

    for country_code in all_iso_country_codes():
        try:
            pois = fetch_pois(country_code=country_code)
        except Exception as exc:
            print(f"[WARN] Failed to fetch {country_code}: {exc}")
            continue

        if not pois:
            print(f"[INFO] No POIs for {country_code}, skipping")
            continue

        print(f"[INFO] Processing {len(pois)} POIs for {country_code}")

        poi_df = normalize_pois(pois)
        conn_df = normalize_connections(pois)

        insert_dataframe(poi_df, table_name="poi")
        insert_dataframe(conn_df, table_name="poi_connection")

        poi_csv = os.path.join(csv_dir, f"poi_{country_code}.csv")
        conn_csv = os.path.join(csv_dir, f"poi_connections_{country_code}.csv")

        poi_df.to_csv(poi_csv, index=False)
        conn_df.to_csv(conn_csv, index=False)

        total_pois += len(poi_df)
        total_connections += len(conn_df)

    print(
        f"[DONE] Ingestion finished. "
        f"Total POIs: {total_pois}, "
        f"Total Connections: {total_connections}"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[FATAL] Unhandled error: {exc}", file=sys.stderr)
        sys.exit(1)
