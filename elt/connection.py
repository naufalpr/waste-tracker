# etl/connection.py (Pastikan ini adalah sumber tunggal)
import os
from sqlalchemy import create_engine
import logging

logger = logging.getLogger("waste_tracker")
# Pastikan logger disetup
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(h)
logger.setLevel(logging.INFO)

# Definisikan nilai default (sesuai dengan yang ada di utils.py)
DEFAULT_DB_URL = "postgresql+psycopg2://postgres:admin123@localhost:5432/waste"

def get_engine():
    # Mengambil dari environment variable, jika tidak ada, pakai default
    db_url = os.environ.get("WASTE_DB_URL", DEFAULT_DB_URL)
    logger.info("Using DB: %s", db_url)
    
    # Gunakan future=False agar kompatibel dengan kode text(q) di project Anda
    return create_engine(
        db_url,
        pool_pre_ping=True,
        future=False 
    )