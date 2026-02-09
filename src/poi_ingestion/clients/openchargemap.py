import requests
from poi_ingestion.config import API_URL, API_KEY

def fetch_pois():
    params = {
        "output": "json",
        "compact": "true",
        "verbose": "false",
        "key": API_KEY
    }
    response = requests.get(API_URL, params=params, timeout=120)
    response.raise_for_status()
    return response.json()
