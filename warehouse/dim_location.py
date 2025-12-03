from sqlalchemy import text
from utils import get_engine

def load_dim_location():
    engine = get_engine()
    # 1. Insert Kunci Kecamatan dari Waste Data
    q_insert = """
    INSERT INTO warehouse.dim_location (kecamatan)
    SELECT DISTINCT kecamatan 
    FROM staging.view_waste_clean
    WHERE kecamatan IS NOT NULL
    ON CONFLICT (kecamatan) DO NOTHING;
    """
    # 2. Update Data Profil dari SIPSN
    q_update = """
    UPDATE warehouse.dim_location dl
    SET penduduk = s.penduduk, luas_km2 = s.luas_km2
    FROM staging.view_sipsn_clean s
    WHERE dl.kecamatan = s.kecamatan;
    """
    with engine.begin() as conn:
        conn.execute(text(q_insert))
        conn.execute(text(q_update))