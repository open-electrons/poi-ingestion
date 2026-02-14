import pandas as pd
import logging
from poi_ingestion.transform.normalize import normalize_pois
from poi_ingestion.transform.normalize import normalize_pois
from poi_ingestion.transform.normalize import normalize_connections


def test_normalize_pois_basic():
    input_data = [
        {
            "ID": 1,
            "AddressInfo": {
                "Title": "Station A",
                "Latitude": 50.1
            }
        }
    ]

    df = normalize_pois(input_data)

    assert not df.empty
    assert "ID" in df.columns
    assert "AddressInfo_Title" in df.columns
    assert "AddressInfo_Latitude" in df.columns
    assert df.iloc[0]["AddressInfo_Title"] == "Station A"


def test_normalize_pois_empty_input():
    df = normalize_pois([])

    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_normalize_pois_nested_structure():
    input_data = [
        {
            "ID": 1,
            "Country": {
                "ISOCode": "DE"
            }
        }
    ]

    df = normalize_pois(input_data)

    assert "Country_ISOCode" in df.columns
    assert df.iloc[0]["Country_ISOCode"] == "DE"


def test_normalize_connections_basic():
    input_data = [
        {
            "ID": 100,
            "Connections": [
                {"ID": 1, "PowerKW": 22},
                {"ID": 2, "PowerKW": 50}
            ]
        }
    ]

    df = normalize_connections(input_data)

    assert len(df) == 2
    assert "POI_ID" in df.columns
    assert set(df["ID"]) == {1, 2}
    assert all(df["POI_ID"] == 100)


def test_normalize_connections_no_connections():
    input_data = [
        {"ID": 1, "Connections": []}
    ]

    df = normalize_connections(input_data)

    assert df.empty


def test_normalize_connections_missing_connections_key():
    input_data = [
        {"ID": 1}
    ]

    df = normalize_connections(input_data)

    assert df.empty


def test_normalize_pois_logs_warning(caplog):
    caplog.set_level(logging.WARNING)

    df = normalize_pois([{}])

    assert df.empty
    assert "empty DataFrame" in caplog.text
