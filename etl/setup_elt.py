# etl/setup_elt.py
from sqlalchemy import text
import sys
import os

# Tambahkan root ke path agar bisa import utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import get_engine

def setup_elt_database():
    engine = get_engine()
    
    # ---------------------------------------------------------
    # 1. TABEL RAW (Tempat Data Kotor Mendarat)
    # ---------------------------------------------------------
    ddl_raw = """
    CREATE SCHEMA IF NOT EXISTS staging;

    -- Raw Waste: Semua kolom TEXT agar loading tidak pernah gagal
    DROP TABLE IF EXISTS staging.raw_waste CASCADE;
    CREATE TABLE staging.raw_waste (
        tanggal TEXT,
        kecamatan TEXT,
        volume_ton TEXT,
        jenis_sampah TEXT,
        sumber_sampah TEXT
    );

    -- Raw SIPSN
    DROP TABLE IF EXISTS staging.raw_sipsn CASCADE;
    CREATE TABLE staging.raw_sipsn (
        kecamatan TEXT,
        armada_total TEXT,
        armada_operasional TEXT,
        ritase_harian TEXT,
        kapasitas_m3 TEXT,
        penduduk TEXT,
        luas_km2 TEXT
    );
    """

    # ---------------------------------------------------------
    # 2. SQL VIEWS (Logika Transformasi & Pembersihan)
    # ---------------------------------------------------------
    ddl_views = """
    -- VIEW: Waste Cleaned
    -- Membersihkan spasi, karakter aneh, dan casting tipe data
    CREATE OR REPLACE VIEW staging.view_waste_clean AS
    SELECT
        TO_DATE(tanggal, 'YYYY-MM-DD') AS tanggal,
        -- LOGIKA AGGRESSIVE CLEAN DI SQL:
        -- 1. UpperCase
        -- 2. Hapus karakter non-alphanumeric (kecuali spasi)
        -- 3. Trim spasi ganda menjadi tunggal
        TRIM(REGEXP_REPLACE(UPPER(kecamatan), '[^A-Z0-9 ]', '', 'g')) AS kecamatan,
        CAST(NULLIF(volume_ton, '') AS DECIMAL(10,2)) AS volume_ton,
        jenis_sampah,
        sumber_sampah
    FROM staging.raw_waste
    WHERE volume_ton IS NOT NULL;

    -- VIEW: SIPSN Cleaned
    CREATE OR REPLACE VIEW staging.view_sipsn_clean AS
    SELECT
        TRIM(REGEXP_REPLACE(UPPER(kecamatan), '[^A-Z0-9 ]', '', 'g')) AS kecamatan,
        CAST(NULLIF(armada_total, '') AS INTEGER) AS armada_total,
        CAST(NULLIF(armada_operasional, '') AS INTEGER) AS armada_operasional,
        CAST(NULLIF(ritase_harian, '') AS DECIMAL(5,1)) AS ritase_harian,
        CAST(NULLIF(kapasitas_m3, '') AS DECIMAL(10,1)) AS kapasitas_m3,
        CAST(NULLIF(penduduk, '') AS INTEGER) AS penduduk,
        CAST(NULLIF(luas_km2, '') AS DECIMAL(10,2)) AS luas_km2
    FROM staging.raw_sipsn;
    """
    
    # ---------------------------------------------------------
    # 3. WAREHOUSE TABLES (Tabel Akhir)
    # ---------------------------------------------------------
    ddl_warehouse = """
    CREATE SCHEMA IF NOT EXISTS warehouse;

    -- Dimensi Waktu
    CREATE TABLE IF NOT EXISTS warehouse.dim_time (
        id SERIAL PRIMARY KEY,
        date DATE UNIQUE,
        year INTEGER,
        month INTEGER,
        day INTEGER
    );

    -- Dimensi Lokasi
    CREATE TABLE IF NOT EXISTS warehouse.dim_location (
        id SERIAL PRIMARY KEY,
        kecamatan VARCHAR(100) UNIQUE,
        kota_administrasi VARCHAR(100),
        penduduk INTEGER,
        luas_km2 DECIMAL
    );

    -- Dimensi Armada
    CREATE TABLE IF NOT EXISTS warehouse.dim_fleet (
        id SERIAL PRIMARY KEY,
        kecamatan VARCHAR(100) UNIQUE,
        armada_total INTEGER,
        armada_operasional INTEGER,
        ritase_harian DECIMAL,
        kapasitas_m3 DECIMAL
    );

    -- Fact Waste
    CREATE TABLE IF NOT EXISTS warehouse.fact_waste (
        id SERIAL PRIMARY KEY,
        time_id INTEGER REFERENCES warehouse.dim_time(id),
        location_id INTEGER REFERENCES warehouse.dim_location(id),
        fleet_id INTEGER,
        volume DECIMAL(10, 2),
        category VARCHAR(50),
        source VARCHAR(50)
    );
    """

    with engine.begin() as conn:
        print("🛠️  Menyiapkan Struktur Database (Schema, Tables, Views)...")
        conn.execute(text(ddl_raw))
        conn.execute(text(ddl_views))
        conn.execute(text(ddl_warehouse))
        print("✅ Setup Database ELT Selesai.")

if __name__ == "__main__":
    setup_elt_database()