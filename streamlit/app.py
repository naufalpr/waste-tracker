# streamlit/app.py
import streamlit as st
import pandas as pd
import geopandas as gpd
import os
import sys
import re
import plotly.express as px
from sqlalchemy import create_engine, text

# Setup Page Config Paling Awal
st.set_page_config("Waste Tracker Jakarta", layout="wide")

# --- PATH CONFIGURATION ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

# --- 0. HELPER FUNCTIONS ---
def aggressive_clean_py(text):
    if not isinstance(text, str): return None
    text = text.strip().upper()
    text = text.replace('\xa0', ' ')
    text = re.sub(r'[^\w\s]', '', text) 
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# --- 1. DATA LOADING ENGINE (ROBUST MODE) ---

def get_data_path(filename):
    """Mencari file data di berbagai kemungkinan lokasi"""
    # Coba path relatif dari file app.py (../data)
    path1 = os.path.join(ROOT_DIR, "data", filename)
    if os.path.exists(path1): return path1
    
    # Coba path langsung (jika dijalankan dari root)
    path2 = os.path.join("data", filename)
    if os.path.exists(path2): return path2
    
    return None

def load_data_csv():
    """Mode Fallback: Baca CSV"""
    waste_path = get_data_path("waste.csv")
    
    if not waste_path:
        st.error("CRITICAL ERROR: File 'data/waste.csv' tidak ditemukan di GitHub!")
        st.stop()
        
    df = pd.read_csv(waste_path)
    df['kecamatan'] = df['kecamatan'].apply(aggressive_clean_py)
    df['date'] = pd.to_datetime(df['tanggal']).dt.date
    df['volume'] = pd.to_numeric(df['volume_ton'], errors='coerce').fillna(0)
    
    # Agregasi
    df_agg = df.groupby(['date', 'kecamatan'], as_index=False)['volume'].sum()
    return df_agg

@st.cache_data
def load_data():
    """Smart Load: Coba DB dulu, jika gagal/tak ada config, pakai CSV"""
    # Cek apakah ada secrets DB
    has_secrets = "connections" in st.secrets and "postgresql" in st.secrets["connections"]
    
    if has_secrets:
        try:
            db_conf = st.secrets["connections"]["postgresql"]
            url = f"postgresql+psycopg2://{db_conf['username']}:{db_conf['password']}@{db_conf['host']}:{db_conf['port']}/{db_conf['database']}"
            engine = create_engine(url)
            
            # --- PERBAIKAN UTAMA DI SINI ---
            # Menggunakan connection context manager untuk SQLAlchemy 2.0 compatibility
            with engine.connect() as conn:
                q = """
                SELECT t.date::date as date, l.kecamatan, SUM(f.volume) as volume
                FROM warehouse.fact_waste f
                JOIN warehouse.dim_time t ON f.time_id = t.id
                JOIN warehouse.dim_location l ON f.location_id = l.id
                GROUP BY t.date, l.kecamatan
                ORDER BY t.date;
                """
                # Pass 'conn' (connection), BUKAN 'engine'
                df = pd.read_sql(text(q), conn)
                
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
            return df
        except Exception as e:
            # Jika DB gagal (koneksi atau query), print error di log server tapi lanjut ke CSV di UI
            print(f"⚠️ Database Error (Switching to CSV): {e}")
    
    # Fallback ke CSV jika tidak ada secrets atau DB error
    return load_data_csv()

@st.cache_data
def load_geo():
    geo_path = get_data_path("kecamatan.geojson")
    
    if not geo_path:
        st.warning("Peta tidak muncul karena 'kecamatan.geojson' tidak ditemukan.")
        return None

    try:
        gdf = gpd.read_file(geo_path).to_crs(4326)
        if "district" in gdf.columns:
            gdf = gdf.rename(columns={"district": "kecamatan"})
        if "kecamatan" in gdf.columns:
            gdf["kecamatan"] = gdf["kecamatan"].apply(aggressive_clean_py)
        return gdf
    except Exception as e:
        st.warning(f"Gagal load peta: {e}")
        return None

def load_fleet_csv(start_date, end_date):
    sipsn_path = get_data_path("sipsn.csv")
    if not sipsn_path: return pd.DataFrame()
    
    df_fleet = pd.read_csv(sipsn_path)
    df_fleet['kecamatan'] = df_fleet['kecamatan'].apply(aggressive_clean_py)
    
    # Hitung rata-rata volume dari fungsi load_data_csv (reuse)
    df_waste = load_data_csv()
    mask = (df_waste['date'] >= start_date) & (df_waste['date'] <= end_date)
    df_avg = df_waste[mask].groupby('kecamatan', as_index=False)['volume'].mean()
    df_avg.rename(columns={'volume': 'avg_daily_waste_ton'}, inplace=True)
    
    return pd.merge(df_fleet, df_avg, on="kecamatan", how="inner")

# --------------------------------------------------------
# MAIN UI
# --------------------------------------------------------
st.title("📊 Waste Tracker — Monitoring Sampah Kota")

# Load Data Utama
df = load_data()

if df.empty:
    st.error("Data kosong atau gagal dimuat. Pastikan folder 'data/' ada di GitHub.")
    st.stop()

# --- SIDEBAR ---
st.sidebar.header("🎛️ Filter")
df["date"] = pd.to_datetime(df["date"]).dt.date
min_d, max_d = df["date"].min(), df["date"].max()

start_date = st.sidebar.date_input("Mulai", min_d, min_value=min_d, max_value=max_d)
end_date = st.sidebar.date_input("Akhir", max_d, min_value=min_d, max_value=max_d)

all_kec = sorted(df['kecamatan'].unique())
sel_kec = st.sidebar.multiselect("Wilayah", all_kec)

# Filter Logic
mask = (df["date"] >= start_date) & (df["date"] <= end_date)
if sel_kec:
    mask &= df["kecamatan"].isin(sel_kec)
    
df_filt = df[mask]

if df_filt.empty:
    st.warning("Tidak ada data.")
    st.stop()

# --- VISUALS ---
total = df_filt['volume'].sum()
days = (end_date - start_date).days + 1
avg = total / days if days > 0 else 0

c1, c2 = st.columns(2)
c1.metric("Total Volume", f"{total:,.0f} Ton")
c2.metric("Rata-rata Harian", f"{avg:,.0f} Ton/Hari")

st.markdown("---")
st.subheader("📈 Tren Harian")
daily = df_filt.groupby("date", as_index=False)["volume"].sum()
st.plotly_chart(px.line(daily, x="date", y="volume", markers=True), use_container_width=True)

st.subheader("🗺️ Peta Persebaran")
gdf = load_geo()
if gdf is not None:
    if sel_kec: gdf = gdf[gdf['kecamatan'].isin(sel_kec)]
    map_agg = df_filt.groupby("kecamatan", as_index=False)["volume"].sum()
    merged = gdf.merge(map_agg, on="kecamatan", how="left").fillna(0).set_index("kecamatan")
    
    if not merged.empty:
        st.plotly_chart(px.choropleth_mapbox(
            merged, geojson=merged.geometry, locations=merged.index, color="volume",
            color_continuous_scale="Reds", mapbox_style="carto-positron",
            center={"lat": -6.2, "lon": 106.8}, zoom=9.5, opacity=0.7
        ), use_container_width=True)

# Fleet Analysis (Simplified Logic)
st.markdown("---")
st.subheader("🚚 Analisis Armada")
# Fallback ke CSV Fleet agar aman dari DB Error
df_fleet = load_fleet_csv(start_date, end_date) 

if not df_fleet.empty:
    if sel_kec: df_fleet = df_fleet[df_fleet['kecamatan'].isin(sel_kec)]
    
    # Avoid division by zero
    df_fleet['cap_ton'] = df_fleet['armada_operasional'] * df_fleet['ritase_harian'] * df_fleet['kapasitas_m3'] * 0.33
    df_fleet['cap_ton'] = df_fleet['cap_ton'].replace(0, 0.1)
    
    df_fleet['load_ratio'] = (df_fleet['avg_daily_waste_ton'] / df_fleet['cap_ton']) * 100
    df_fleet['status'] = df_fleet['load_ratio'].apply(lambda x: "CRITICAL" if x > 110 else "SAFE")
    
    c_a, c_b = st.columns([2,1])
    with c_a:
        st.plotly_chart(px.scatter(
            df_fleet, x="cap_ton", y="avg_daily_waste_ton", color="status", 
            size="armada_total", hover_name="kecamatan",
            color_discrete_map={"SAFE": "green", "CRITICAL": "red"}
        ), use_container_width=True)
    with c_b:
        st.dataframe(df_fleet[['kecamatan', 'load_ratio']], hide_index=True)