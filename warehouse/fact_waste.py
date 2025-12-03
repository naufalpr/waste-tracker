from sqlalchemy import text
from utils import get_engine

def load_fact_waste():
    engine = get_engine()
    
    # Hapus data lama agar tidak duplikat (untuk demo)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE warehouse.fact_waste RESTART IDENTITY;"))

    q = """
    INSERT INTO warehouse.fact_waste (time_id, location_id, volume, category, source)
    SELECT 
        t.id, l.id, s.volume_ton, s.jenis_sampah, s.sumber_sampah
    FROM staging.view_waste_clean s
    JOIN warehouse.dim_time t ON t.date = s.tanggal
    JOIN warehouse.dim_location l ON l.kecamatan = s.kecamatan;
    """
    with engine.begin() as conn:
        conn.execute(text(q))