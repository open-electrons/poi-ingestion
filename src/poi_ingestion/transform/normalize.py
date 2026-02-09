import pandas as pd

def normalize_pois(pois):
    rows = []
    for p in pois:
        addr = p.get("AddressInfo", {})
        rows.append({
            "id": p.get("ID"),
            "uuid": p.get("UUID"),
            "title": addr.get("Title"),
            "latitude": addr.get("Latitude"),
            "longitude": addr.get("Longitude"),
            "town": addr.get("Town"),
            "postcode": addr.get("Postcode"),
            "number_of_points": p.get("NumberOfPoints"),
            "raw": p
        })
    return pd.DataFrame(rows)

def normalize_connections(pois):
    rows = []
    for p in pois:
        for c in p.get("Connections", []):
            rows.append({
                "id": c.get("ID"),
                "poi_id": p.get("ID"),
                "power_kw": c.get("PowerKW"),
                "quantity": c.get("Quantity"),
                "raw": c
            })
    return pd.DataFrame(rows)
