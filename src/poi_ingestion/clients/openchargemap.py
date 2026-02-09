import requests
import pycountry

from typing import List, Dict, Optional

BASE_URL = "https://api.openchargemap.io/v3/poi"
DEFAULT_TIMEOUT = 30


class OpenChargeMapError(Exception):
    pass


def fetch_pois(
    country_code: str,
    max_results: Optional[int] = None
) -> List[Dict]:
    """
    Fetch POIs for a given ISO 3166-1 alpha-2 country code.

    Args:
        country_code: ISO country code (e.g. 'DE', 'US')
        max_results: Optional limit for testing/debugging

    Returns:
        List of POI JSON objects
    """
    # ✅ Lazy-load API key at runtime (allows monkeypatching in tests)
    import os
    OPENCHARGEMAP_API_KEY = os.getenv("OPENCHARGEMAP_API_KEY", "")

    country_code = country_code.upper()

    # Length check
    if not country_code or len(country_code) != 2:
        raise ValueError("country_code must be ISO 3166-1 alpha-2")

    # Real ISO code check
    if pycountry.countries.get(alpha_2=country_code) is None:
        raise ValueError(f"{country_code} is not a valid ISO 3166-1 alpha-2 code")

    params = {
        "key": OPENCHARGEMAP_API_KEY,
        "countrycode": country_code.upper(),
        "output": "json",
        "compact": False,
        "verbose": False,
    }

    if max_results:
        params["maxresults"] = max_results

    try:
        response = requests.get(
            BASE_URL,
            params=params,
            timeout=DEFAULT_TIMEOUT,
        )
    except requests.RequestException as exc:
        raise OpenChargeMapError(
            f"Network error for country {country_code}"
        ) from exc

    if response.status_code != 200:
        raise OpenChargeMapError(
            f"HTTP {response.status_code} for country {country_code}"
        )

    try:
        data = response.json()
    except ValueError as exc:
        raise OpenChargeMapError(
            f"Invalid JSON response for country {country_code}"
        ) from exc

    if not isinstance(data, list):
        raise OpenChargeMapError(
            f"Unexpected response format for country {country_code}"
        )

    return data
