from sqlalchemy import create_engine
from poi_ingestion.config import DB_URL

engine = create_engine(DB_URL)

def insert_dataframe(df, table):
    if df.empty:
        return
    df.to_sql(table, engine, if_exists="append", index=False, method="multi")
