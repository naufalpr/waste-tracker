from sqlalchemy import text
from utils import get_engine

def load_dim_time():
    engine = get_engine()
    q = """
    INSERT INTO warehouse.dim_time (date, year, month, day)
    SELECT DISTINCT 
        tanggal,
        EXTRACT(YEAR FROM tanggal),
        EXTRACT(MONTH FROM tanggal),
        EXTRACT(DAY FROM tanggal)
    FROM staging.view_waste_clean
    ON CONFLICT (date) DO NOTHING;
    """
    with engine.begin() as conn:
        conn.execute(text(q))