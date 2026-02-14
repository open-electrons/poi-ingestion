from unittest.mock import patch
import json

from poi_ingestion.service.poi_processor import load_json_safely
from poi_ingestion.service.poi_processor import process_json_to_db
from poi_ingestion.service.poi_processor import extract_pois
from poi_ingestion.service.poi_processor import extract_connections


def test_extract_pois_basic():
    input_data = [
        {
            "UUID": "123",
            "AddressInfo": {
                "Title": "Test Station",
                "Latitude": 50.0,
                "Longitude": 8.0,
                "Country": {"ISOCode": "DE"}
            }
        }
    ]

    df = extract_pois(input_data)

    assert len(df) == 1
    assert df.iloc[0]["poi_uuid"] == "123"
    assert df.iloc[0]["country_code"] == "DE"
    assert df.iloc[0]["title"] == "Test Station"
    assert df.iloc[0]["latitude"] == 50.0
    assert df.iloc[0]["longitude"] == 8.0


def test_extract_pois_skips_missing_uuid():
    input_data = [{"AddressInfo": {}}]

    df = extract_pois(input_data)

    assert df.empty


def test_extract_connections_multiple():
    input_data = [
        {
            "UUID": "poi1",
            "Connections": [
                {"ID": 1, "PowerKW": 22},
                {"ID": 2, "PowerKW": 50},
            ]
        }
    ]

    df = extract_connections(input_data)

    assert len(df) == 2
    assert set(df["connection_id"]) == {1, 2}
    assert all(df["poi_uuid"] == "poi1")


def test_extract_connections_skips_missing_id():
    input_data = [
        {
            "UUID": "poi1",
            "Connections": [{"PowerKW": 22}]
        }
    ]

    df = extract_connections(input_data)

    assert df.empty


def test_load_json_safely_valid(tmp_path):
    file = tmp_path / "test.json"
    file.write_text(json.dumps([{"UUID": "1"}]))

    data = load_json_safely(file)

    assert data == [{"UUID": "1"}]


def test_load_json_safely_missing(tmp_path):
    file = tmp_path / "missing.json"

    data = load_json_safely(file)

    assert data is None


def test_load_json_safely_invalid_json(tmp_path):
    file = tmp_path / "invalid.json"
    file.write_text("{ invalid json")

    data = load_json_safely(file)

    assert data is None


def test_process_json_to_db_happy_path(tmp_path):
    file = tmp_path / "test.json"
    file.write_text(json.dumps([
        {
            "UUID": "1",
            "AddressInfo": {"Title": "Test"},
            "Connections": [{"ID": 10}]
        }
    ]))

    with patch("poi_ingestion.service.poi_processor.upsert_dataframe") as mock_upsert, \
         patch("poi_ingestion.service.poi_processor.os.remove") as mock_remove:

        process_json_to_db(file)

        # called twice: pois + connections
        assert mock_upsert.call_count == 2

        mock_remove.assert_called_once_with(file)
