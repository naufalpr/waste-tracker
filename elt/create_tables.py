# etl/create_tables.py
from sqlalchemy import text
import os
import logging
from etl.connection import get_engine 

# Inisialisasi Logger
logger = logging.getLogger("waste_tracker")
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(h)
logger.setLevel(logging.INFO)


def create_warehouse_tables():
    """
    Menghapus dan membuat ulang semua skema dan tabel dimensional/fakta.
    Perintah DROP TABLE CASCADE diperlukan untuk mengatasi masalah UndefinedColumn
    jika skema lama tidak lengkap.
    """
    engine = get_engine()
    logger.info("Memulai DDL: Menghapus dan membuat ulang skema.")

    with engine.begin() as conn:
        # 1. BUAT SKEMA
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS warehouse;"))
        
        # 2. DROP TABEL LAMA
        # DROP TABLE CASCADE menghapus tabel fakta yang memiliki foreign key ke dimensi
        conn.execute(text("DROP TABLE IF EXISTS warehouse.fact_waste CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS warehouse.dim_time;"))
        conn.execute(text("DROP TABLE IF EXISTS warehouse.dim_location;"))
        conn.execute(text("DROP TABLE IF EXISTS warehouse.dim_fleet;"))
        
        # 3. DIMENSI WAKTU
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS warehouse.dim_time (
                id SERIAL PRIMARY KEY,
                date DATE UNIQUE,
                year INTEGER,
                month INTEGER,
                day INTEGER
            );
        """))

        # 4. DIMENSI LOKASI (Memastikan kolom penduduk & luas_km2 ada)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS warehouse.dim_location (
                id SERIAL PRIMARY KEY,
                kecamatan VARCHAR(100) UNIQUE, -- Kunci Bisnis
                kota_administrasi VARCHAR(100),
                penduduk INTEGER,             -- Kolom yang hilang
                luas_km2 DECIMAL              -- Kolom yang hilang
            );
        """))
        
        # 5. DIMENSI ARMADA (Fleet)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS warehouse.dim_fleet (
                id SERIAL PRIMARY KEY,
                kecamatan VARCHAR(100) UNIQUE,
                armada_total INTEGER,
                armada_operasional INTEGER,
                ritase_harian DECIMAL,
                kapasitas_m3 DECIMAL
            );
        """))

        # 6. TABEL FAKTA
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS warehouse.fact_waste (
                id SERIAL PRIMARY KEY,
                time_id INTEGER REFERENCES warehouse.dim_time(id),
                location_id INTEGER REFERENCES warehouse.dim_location(id),
                fleet_id INTEGER, 
                volume DECIMAL(10, 2) NOT NULL,
                category VARCHAR(50), 
                source VARCHAR(50)
            );
        """))
    logger.info("DDL Warehouse Tables berhasil dibuat ulang dengan skema baru.")

if __name__ == "__main__":
    create_warehouse_tables()