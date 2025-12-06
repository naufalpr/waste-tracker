# utils.py
import streamlit as st
from sqlalchemy import create_engine
import os

def get_engine():
    # 1. Ambil dari Streamlit Secrets (Prioritas Utama untuk Cloud)
    try:
        # Mengakses section [connections.postgresql] di secrets.toml
        db_conf = st.secrets["connections"]["postgresql"]
        
        # Susun Connection String
        db_url = f"postgresql+psycopg2://{db_conf['username']}:{db_conf['password']}@{db_conf['host']}:{db_conf['port']}/{db_conf['database']}"
        
        return create_engine(db_url)
        
    except Exception:
        pass
    
    # 2. Fallback: Coba ambil dari Environment Variable (Opsional)
    db_url = os.environ.get("WASTE_DB_URL")
    if db_url:
        return create_engine(db_url)

    # 3. Fallback Terakhir: Localhost (Hanya jalan di lokal)
    return create_engine("postgresql+psycopg2://postgres:admin123@localhost:5432/waste")