## poi-ingestion

TODO

### Mapping

## ✅ JSON → POIS Table Mapping

| JSON Field | JSON Path | DB Column | Notes |
|------------|-----------|-----------|-------|
| UUID | `UUID` | `poi_uuid` | ✅ Primary key |
| ID | `ID` | (stored in raw_json) | Legacy OCM ID |
| Country Code | `AddressInfo.Country.ISOCode` | `country_code` | ISO-2 |
| Title | `AddressInfo.Title` | `title` | Site name |
| Latitude | `AddressInfo.Latitude` | `latitude` | |
| Longitude | `AddressInfo.Longitude` | `longitude` | |
| Everything else | entire JSON | `raw_json` | Full JSONB backup |

## ✅ JSON → CONNECTIONS Table Mapping

Each POI has:
```json
"Connections": [ {...}, {...} ]
```

For each connection:

| JSON Field | JSON Path | DB Column | Notes |
|------------|-----------|-----------|-------|
| POI UUID | parent `UUID` | `poi_uuid` | FK |
| ID | `Connections[].ID` | `connection_id` | Unique per POI |
| Connection Type | `Connections[].ConnectionType.Title` | `connection_type` | |
| Power | `Connections[].PowerKW` | `power_kw` | |
| Voltage | `Connections[].Voltage` | `voltage` | |
| Amperage | `Connections[].Amps` | `amperage` | |
| Everything else | full connection JSON | `raw_json` | JSONB backup |

## Running the Application

To visualize the ingested POI's, run the user interface as below:

### macOS / Linux / Windows

```bash
python run_app.py
