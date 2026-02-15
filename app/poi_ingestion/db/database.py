# app/poi_ingestion/db/database.py

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Load .env from project root
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
load_dotenv(dotenv_path=os.path.join(PROJECT_ROOT, ".env"))

# Get DB URL
DATABASE_URL = os.getenv("DB_URL")

if not DATABASE_URL:
    raise ValueError(
        "DATABASE_URL not set! Please check your .env file in the project root."
    )

# Create engine and session factory
engine = create_engine(DATABASE_URL, echo=False)  # echo=True for SQL logs
SessionLocal = sessionmaker(bind=engine)

def get_session():
    """Get a new SQLAlchemy session"""
    return SessionLocal()
