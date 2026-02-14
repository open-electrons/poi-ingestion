import os
from dotenv import load_dotenv

load_dotenv(override=False)

OPENCHARGEMAP_API_KEY = os.getenv("OPENCHARGEMAP_API_KEY", "")

DATA_DIR = os.getenv("DATA_DIR", "data")
