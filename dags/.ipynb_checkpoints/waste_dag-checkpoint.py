import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import text

# --- KONFIGURASI PATH ---
# Kita perlu menambahkan root folder proyek ke sys.path agar Airflow bisa mengimpor modul 'etl', 'utils', dll.
# Ganti path ini sesuai lokasi proyek Anda di WSL/Linux.
# Contoh: "/mnt/d/coolyeah/Semester 5/PID/waste-tracker"
PROJECT_ROOT = os.environ.get("WASTE_PROJECT_ROOT", "/opt/airflow/dags/repo")
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# --- IMPORT MODULE PROYEK ---
try:
    from utils import get_engine
    from elt.setup_elt import setup_elt_database
    from elt.validator import validate_waste_data, validate_sipsn_data
    from warehouse.dim_time import load_dim_time
    from warehouse.dim_location import load_dim_location
    from warehouse.dim_fleet import load_dim_fleet
    from warehouse.fact_waste import load_fact_waste
except ImportError as e:
    print(f"❌ Gagal Import Module: {e}")
    print(f"Current Path: {sys.path}")

# --- FUNGSI WRAPPER UNTUK AIRFLOW TASKS ---

def task_setup_db():
    print("🛠️ Mempersiapkan Database ELT...")
    setup_elt_database()

def task_process_waste(**kwargs):
    print("📥 Extract, Validate & Load: Waste Data")
    engine = get_engine()
    data_dir = os.path.join(PROJECT_ROOT, "data")
    file_path = os.path.join(data_dir, "waste.csv")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {file_path} tidak ditemukan.")
    
    # 1. Extract
    df = pd.read_csv(file_path, dtype=str)
    
    # 2. Validate (Firewall)
    if not validate_waste_data(df):
        raise ValueError("Validasi Data Waste GAGAL. Pipeline dihentikan.")
    
    # 3. Load to Staging
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE staging.raw_waste;"))
    
    df.to_sql('raw_waste', engine, schema='staging', if_exists='append', index=False)
    print(f"✅ Berhasil memuat {len(df)} baris ke staging.raw_waste")

def task_process_sipsn(**kwargs):
    print("📥 Extract, Validate & Load: SIPSN Data")
    engine = get_engine()
    data_dir = os.path.join(PROJECT_ROOT, "data")
    file_path = os.path.join(data_dir, "sipsn.csv")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {file_path} tidak ditemukan.")
    
    # 1. Extract
    df = pd.read_csv(file_path, dtype=str)
    
    # 2. Validate (Firewall)
    if not validate_sipsn_data(df):
        raise ValueError("Validasi Data SIPSN GAGAL. Pipeline dihentikan.")
    
    # 3. Load to Staging
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE staging.raw_sipsn;"))
        
    df.to_sql('raw_sipsn', engine, schema='staging', if_exists='append', index=False)
    print(f"✅ Berhasil memuat {len(df)} baris ke staging.raw_sipsn")

def task_update_warehouse():
    print("🏭 Memperbarui Data Warehouse (Transform via SQL Views)...")
    load_dim_time()
    load_dim_location()
    load_dim_fleet()
    load_fact_waste()
    print("✅ Warehouse Updated Successfully.")

# --- DEFINISI DAG ---

default_args = {
    'owner': 'kelompok10',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'waste_tracker_elt_pipeline',
    default_args=default_args,
    description='Pipeline ELT untuk Waste Tracker Case 4',
    schedule_interval='0 1 * * *', # Jalan setiap jam 01:00 pagi (Sesuai Proposal)
    start_date=days_ago(1),
    catchup=False,
    tags=['waste-tracker', 'elt', 'case4'],
) as dag:

    # 1. Define Tasks
    t1_setup = PythonOperator(
        task_id='setup_infrastructure',
        python_callable=task_setup_db,
    )

    t2_waste = PythonOperator(
        task_id='process_waste_data',
        python_callable=task_process_waste,
    )

    t3_sipsn = PythonOperator(
        task_id='process_sipsn_data',
        python_callable=task_process_sipsn,
    )

    t4_warehouse = PythonOperator(
        task_id='update_warehouse',
        python_callable=task_update_warehouse,
    )

    # 2. Define Dependencies
    # Setup dulu, lalu load Waste & SIPSN secara paralel, setelah keduanya selesai baru update Warehouse
    t1_setup >> [t2_waste, t3_sipsn] >> t4_warehouse