import pandas as pd

from poi_ingestion.transform.normalize import (
    normalize_pois,
    normalize_connections,
)


def sample_poi_json():
    return [
        {
            "ID": 1,
            "UUID": "uuid-1",
            "NumberOfPoints": 2,
            "AddressInfo": {
                "Title": "Test Location",
                "Latitude": 52.52,
                "Longitude": 13.405,
                "Town": "Berlin",
                "Postcode": "10115",
            },
            "Connections": [
                {
                    "ID": 10,
                    "PowerKW": 22,
                    "Quantity": 2,
                },
                {
                    "ID": 11,
                    "PowerKW": 50,
                    "Quantity": 1,
                },
            ],
        }
    ]


def test_normalize_pois_returns_dataframe():
    df = normalize_pois(sample_poi_json())
    assert isinstance(df, pd.DataFrame)


def test_normalize_pois_row_count():
    df = normalize_pois(sample_poi_json())
    assert len(df) == 1


def test_normalize_pois_fields_extracted_correctly():
    df = normalize_pois(sample_poi_json())
    row = df.iloc[0]

    assert row["id"] == 1
    assert row["uuid"] == "uuid-1"
    assert row["title"] == "Test Location"
    assert row["latitude"] == 52.52
    assert row["longitude"] == 13.405
    assert row["town"] == "Berlin"
    assert row["postcode"] == "10115"
    assert row["number_of_points"] == 2
    assert isinstance(row["raw"], dict)


def test_normalize_connections_returns_dataframe():
    df = normalize_connections(sample_poi_json())
    assert isinstance(df, pd.DataFrame)


def test_normalize_connections_row_count():
    df = normalize_connections(sample_poi_json())
    assert len(df) == 2


def test_normalize_connections_fields_extracted_correctly():
    df = normalize_connections(sample_poi_json())

    first = df.iloc[0]
    second = df.iloc[1]

    assert first["id"] == 10
    assert first["poi_id"] == 1
    assert first["power_kw"] == 22
    assert first["quantity"] == 2

    assert second["id"] == 11
    assert second["poi_id"] == 1
    assert second["power_kw"] == 50
    assert second["quantity"] == 1


def test_normalize_handles_missing_connections():
    poi_without_connections = [
        {
            "ID": 2,
            "UUID": "uuid-2",
            "AddressInfo": {},
        }
    ]

    df = normalize_connections(poi_without_connections)
    assert df.empty


def test_normalize_handles_missing_address_info():
    poi_missing_address = [
        {
            "ID": 3,
            "UUID": "uuid-3",
            "Connections": [],
        }
    ]

    df = normalize_pois(poi_missing_address)
    row = df.iloc[0]

    assert row["id"] == 3
    assert row["title"] is None
    assert row["latitude"] is None
    assert row["longitude"] is None
