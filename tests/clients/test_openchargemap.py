import pytest
import requests

from poi_ingestion.clients.openchargemap import (
    fetch_pois,
    OpenChargeMapError,
)

BASE_URL = "https://api.openchargemap.io/v3/poi"


def test_fetch_pois_success(requests_mock):
    mock_response = [
        {"ID": 1, "AddressInfo": {"CountryID": 87}},
        {"ID": 2, "AddressInfo": {"CountryID": 87}},
    ]

    requests_mock.get(BASE_URL, json=mock_response, status_code=200)

    result = fetch_pois(country_code="DE")

    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["ID"] == 1


def test_fetch_pois_empty_response(requests_mock):
    requests_mock.get(BASE_URL, json=[], status_code=200)

    result = fetch_pois(country_code="AQ")  # Antarctica-style case

    assert result == []


def test_fetch_pois_http_error(requests_mock):
    requests_mock.get(BASE_URL, status_code=500)

    with pytest.raises(OpenChargeMapError):
        fetch_pois(country_code="US")


def test_fetch_pois_invalid_json(requests_mock):
    requests_mock.get(BASE_URL, text="not-json", status_code=200)

    with pytest.raises(OpenChargeMapError):
        fetch_pois(country_code="FR")


def test_fetch_pois_invalid_country_code():
    with pytest.raises(ValueError):
        fetch_pois(country_code="")

    with pytest.raises(ValueError):
        fetch_pois(country_code="USA")

    with pytest.raises(ValueError):
        fetch_pois(country_code="1A")


def test_fetch_pois_network_error(monkeypatch):
    def raise_timeout(*args, **kwargs):
        raise requests.Timeout("timeout")

    monkeypatch.setattr(requests, "get", raise_timeout)

    with pytest.raises(OpenChargeMapError):
        fetch_pois(country_code="GB")
