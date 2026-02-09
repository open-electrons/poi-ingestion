from poi_ingestion.clients.openchargemap import fetch_pois
from poi_ingestion.transform.normalize import normalize_pois, normalize_connections
from poi_ingestion.db.repository import insert_dataframe
from poi_ingestion.config import DATA_DIR
import os

def main():
    pois = fetch_pois()
    poi_df = normalize_pois(pois)
    conn_df = normalize_connections(pois)

    insert_dataframe(poi_df, "poi")
    insert_dataframe(conn_df, "poi_connection")

    os.makedirs(f"{DATA_DIR}/csv", exist_ok=True)
    poi_df.to_csv(f"{DATA_DIR}/csv/poi.csv", index=False)
    conn_df.to_csv(f"{DATA_DIR}/csv/poi_connections.csv", index=False)

if __name__ == "__main__":
    main()
