# app/db/queries.py

import pandas as pd
from sqlalchemy.orm import Session
from app.poi_ingestion.db.models import POI, Connection


def get_all_pois(session: Session):
    return session.query(POI).all()


def get_pois_by_country(session: Session, country_code: str):
    return (
        session.query(POI)
        .filter(POI.country_code == country_code)
        .all()
    )


def get_all_connections(session: Session):
    return session.query(Connection).all()


def get_pois_with_connections(session: Session):
    return (
        session.query(POI)
        .join(Connection)
        .all()
    )


def get_pois_dataframe(session: Session):
    pois = session.query(POI).all()

    data = [
        {
            "poi_uuid": p.poi_uuid,
            "title": p.title,
            "country": p.country_code,
            "latitude": p.latitude,
            "longitude": p.longitude,
        }
        for p in pois
    ]

    return pd.DataFrame(data)


def get_connections_dataframe(session: Session) -> pd.DataFrame:
    """Return all connections as a Pandas DataFrame"""
    conns = session.query(Connection).all()
    data = [
        {
            "id": c.id,
            "poi_uuid": c.poi_uuid,
            "connection_id": c.connection_id,
            "connection_type": c.connection_type,
            "power_kw": c.power_kw,
            "voltage": c.voltage,
            "amperage": c.amperage,
        }
        for c in conns
    ]
    return pd.DataFrame(data)