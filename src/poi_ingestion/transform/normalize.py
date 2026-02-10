import pandas as pd

def normalize_pois(pois_json: list) -> pd.DataFrame:
    """
    Flatten POI JSON into a DataFrame.
    """
    if not pois_json:
        return pd.DataFrame()

    df = pd.json_normalize(
        pois_json,
        sep="_",
        errors='ignore'
    )
    return df


def normalize_connections(pois_json: list) -> pd.DataFrame:
    """
    Flatten Connections into a separate DataFrame linked by POI ID.
    """
    rows = []
    for poi in pois_json:
        poi_id = poi.get("ID")
        for conn in poi.get("Connections", []):
            conn_row = conn.copy()
            conn_row["POI_ID"] = poi_id
            rows.append(conn_row)

    if not rows:
        return pd.DataFrame()

    df = pd.json_normalize(rows, sep="_", errors='ignore')
    return df
