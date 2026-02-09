import time
from typing import List, Dict, Any

import requests

from poi_ingestion.config import API_URL, API_KEY


class OpenChargeMapClientError(Exception):
    pass


def fetch_pois(
    max_retries: int = 3,
    timeout_seconds: int = 120,
    sleep_between_retries: int = 5,
) -> List[Dict[str, Any]]:
    """
    Fetch POIs from the Open Charge Map API.

    Returns:
        List of POI JSON objects
    """
    if not API_KEY:
        raise OpenChargeMapClientError("API_KEY is not set")

    params = {
        "output": "json",
        "compact": "true",
        "verbose": "false",
        "key": API_KEY,
    }

    last_exception: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(
                API_URL,
                params=params,
                timeout=timeout_seconds,
            )
            response.raise_for_status()

            data = response.json()

            if not isinstance(data, list):
                raise OpenChargeMapClientError(
                    f"Unexpected response type: {type(data)}"
                )

            return data

        except (requests.RequestException, ValueError) as exc:
            last_exception = exc
            if attempt < max_retries:
                time.sleep(sleep_between_retries)
            else:
                break

    raise OpenChargeMapClientError(
        f"Failed to fetch POIs after {max_retries} attempts"
    ) from last_exception
