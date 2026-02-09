import os
from dotenv import load_dotenv

load_dotenv()

API_URL = os.getenv("API_URL", "https://api.openchargemap.io/v3/poi/")
API_KEY = os.getenv("API_KEY")

DB_URL = os.getenv("DB_URL")

DATA_DIR = os.getenv("DATA_DIR", "data")
