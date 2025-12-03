from sqlalchemy import text
from utils import get_engine

def load_dim_fleet():
    engine = get_engine()
    q = """
    INSERT INTO warehouse.dim_fleet (kecamatan, armada_total, armada_operasional, ritase_harian, kapasitas_m3)
    SELECT kecamatan, armada_total, armada_operasional, ritase_harian, kapasitas_m3
    FROM staging.view_sipsn_clean
    WHERE kecamatan IS NOT NULL
    ON CONFLICT (kecamatan) DO UPDATE
    SET armada_total = EXCLUDED.armada_total,
        armada_operasional = EXCLUDED.armada_operasional,
        ritase_harian = EXCLUDED.ritase_harian,
        kapasitas_m3 = EXCLUDED.kapasitas_m3;
    """
    with engine.begin() as conn:
        conn.execute(text(q))