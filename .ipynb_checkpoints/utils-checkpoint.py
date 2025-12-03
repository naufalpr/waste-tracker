# utils.py
import os
from sqlalchemy import create_engine
import logging

# Setup Logging Sederhana
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger("waste_tracker")

def get_engine():
    # Ganti kredensial sesuai database lokal Anda
    DB_USER = "postgres"
    DB_PASS = "admin123"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_NAME = "waste"
    
    # Prioritaskan Environment Variable, fallback ke hardcoded
    db_url = os.environ.get(
        "WASTE_DB_URL",
        f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    
    return create_engine(db_url, echo=False, future=False)