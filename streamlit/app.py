# streamlit/app.py
import streamlit as st
import pandas as pd
import geopandas as gpd
import os
import sys
import re
import plotly.express as px
from sqlalchemy import text # <--- Tambahan Wajib untuk SQLAlchemy 2.0

# --- PATH CONFIGURATION ---
try:
    ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if ROOT_DIR not in sys.path:
        sys.path.insert(0, ROOT_DIR)
except Exception:
    pass

from utils import get_engine

st.set_page_config("Waste Tracker Jakarta", layout="wide")
engine = get_engine()

# --------------------------------------------------------
# 0. HELPER FUNCTIONS
# --------------------------------------------------------
def aggressive_clean_py(text):
    """Membersihkan nama kecamatan agar cocok dengan SQL ELT"""
    if not isinstance(text, str): return None
    text = text.strip().upper()
    text = text.replace('\xa0', ' ')
    text = re.sub(r'[^\w\s]', '', text) 
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# --------------------------------------------------------
# 1. LOAD DATA FUNCTIONS
# --------------------------------------------------------
@st.cache_data
def load_data():
    q = """
    SELECT t.date::date as date, l.kecamatan, SUM(f.volume) as volume
    FROM warehouse.fact_waste f
    JOIN warehouse.dim_time t ON f.time_id = t.id
    JOIN warehouse.dim_location l ON f.location_id = l.id
    GROUP BY t.date, l.kecamatan
    ORDER BY t.date;
    """
    # PERBAIKAN: Menggunakan connection context manager
    with engine.connect() as conn:
        df = pd.read_sql(text(q), conn)
        
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
    return df

@st.cache_data
def load_geo():
    path = os.environ.get("WASTE_DATA_DIR", "./data")
    gfile = os.path.join(path, "kecamatan.geojson")

    if not os.path.exists(gfile):
        return None

    try:
        gdf = gpd.read_file(gfile).to_crs(4326)
        if "district" in gdf.columns:
            gdf = gdf.rename(columns={"district": "kecamatan"})
        if "kecamatan" in gdf.columns:
            gdf["kecamatan"] = gdf["kecamatan"].apply(aggressive_clean_py)
        return gdf
    except Exception:
        return None

@st.cache_data
def load_fleet_analysis(start_date, end_date):
    # PERBAIKAN: Menggunakan connection context manager untuk kedua query
    with engine.connect() as conn:
        q_fleet = """
        SELECT kecamatan, armada_total, armada_operasional, ritase_harian, kapasitas_m3
        FROM warehouse.dim_fleet;
        """
        df_fleet = pd.read_sql(text(q_fleet), conn)
        
        q_waste = f"""
        SELECT l.kecamatan, AVG(f.volume) as avg_daily_waste_ton
        FROM warehouse.fact_waste f
        JOIN warehouse.dim_location l ON f.location_id = l.id
        JOIN warehouse.dim_time t ON f.time_id = t.id
        WHERE t.date >= '{start_date}' AND t.date <= '{end_date}'
        GROUP BY l.kecamatan;
        """
        df_waste = pd.read_sql(text(q_waste), conn)
    
    return pd.merge(df_fleet, df_waste, on="kecamatan", how="inner")

# --------------------------------------------------------
# MAIN UI & LOGIC
# --------------------------------------------------------
st.title("📊 Waste Tracker — Monitoring Sampah Kota")

# A. LOAD INITIAL DATA
try:
    df = load_data()
except Exception as e:
    st.error(f"Gagal koneksi ke Database: {e}")
    st.stop()

if df.empty:
    st.warning("Database Kosong. Silakan jalankan ELT pipeline.")
    st.stop()

# --- SIDEBAR CONFIGURATION ---
st.sidebar.header("🎛️ Filter Dashboard")

# 1. Filter Tanggal
df["date"] = pd.to_datetime(df["date"]).dt.date
min_date = df["date"].min()
max_date = df["date"].max()

start_date = st.sidebar.date_input("Tanggal Mulai", min_date, min_value=min_date, max_value=max_date)
end_date = st.sidebar.date_input("Tanggal Akhir", max_date, min_value=min_date, max_value=max_date)

if start_date > end_date:
    st.sidebar.error("Tanggal error.")
    st.stop()

# 2. Filter Kecamatan (Multiselect)
all_kecamatan = sorted(df['kecamatan'].unique())
selected_kecamatan = st.sidebar.multiselect(
    "Pilih Wilayah (Kecamatan)", 
    options=all_kecamatan,
    placeholder="Pilih wilayah (opsional)..."
)

# --- FILTERING LOGIC ---
mask_date = (df["date"] >= start_date) & (df["date"] <= end_date)

if selected_kecamatan:
    mask_loc = df["kecamatan"].isin(selected_kecamatan)
    df_filtered = df[mask_date & mask_loc]
    st.sidebar.success(f"Filter aktif: {len(selected_kecamatan)} wilayah.")
else:
    df_filtered = df[mask_date]
    st.sidebar.info("Menampilkan seluruh wilayah.")

if df_filtered.empty:
    st.warning("Tidak ada data dengan filter yang dipilih.")
    st.stop()

# --- VISUALISASI UTAMA ---

# C. METRIK
total_vol = df_filtered['volume'].sum()
# Hindari error division by zero jika days=0
days_count = (end_date - start_date).days + 1
avg_vol = total_vol / days_count if days_count > 0 else 0

col1, col2 = st.columns(2)
col1.metric("Total Volume (Ton)", f"{total_vol:,.0f}")
col2.metric("Rata-rata Harian (Ton/Hari)", f"{avg_vol:,.0f}")

st.markdown("---")

# D. GRAFIK TREN
st.subheader("📈 Tren Volume Sampah")
daily_trend = df_filtered.groupby("date", as_index=False)["volume"].sum()
fig_trend = px.line(daily_trend, x="date", y="volume", markers=True, template="plotly_white")
st.plotly_chart(fig_trend, use_container_width=True)

# E. PETA HEATMAP (STATIC ZOOM)
st.subheader("🗺️ Peta Persebaran")
gdf = load_geo()

if gdf is not None:
    # 1. FILTER GEOJSON (Hanya tampilkan poligon yg dipilih)
    if selected_kecamatan:
        gdf_vis = gdf[gdf['kecamatan'].isin(selected_kecamatan)]
    else:
        gdf_vis = gdf

    # 2. AGREGASI DATA
    map_agg = df_filtered.groupby("kecamatan", as_index=False)["volume"].sum()
    
    # 3. MERGE
    merged = gdf_vis.merge(map_agg, on="kecamatan", how="left").fillna(0).set_index("kecamatan")

    # 4. VISUALISASI (ZOOM TETAP/STATIC)
    if not merged.empty:
        fig_map = px.choropleth_mapbox(
            merged,
            geojson=merged.geometry,
            locations=merged.index,
            color="volume",
            color_continuous_scale="Reds",
            mapbox_style="carto-positron",
            # KOORDINAT & ZOOM TETAP (JAKARTA)
            center={"lat": -6.22, "lon": 106.83}, 
            zoom=9.8,     
            opacity=0.7,
            labels={"volume": "Total Volume (Ton)"}
        )
        fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.warning("Data geometri untuk wilayah terpilih tidak ditemukan.")

# F. ANALISIS ARMADA
st.markdown("---")
st.subheader("🚚 Analisis Performa & Ketersediaan Armada")

df_fleet_all = load_fleet_analysis(start_date, end_date)

if selected_kecamatan:
    df_fleet = df_fleet_all[df_fleet_all['kecamatan'].isin(selected_kecamatan)]
else:
    df_fleet = df_fleet_all

if not df_fleet.empty:
    DENSITY = 0.33
    df_fleet['capacity_ton'] = df_fleet['armada_operasional'] * df_fleet['ritase_harian'] * df_fleet['kapasitas_m3'] * DENSITY
    
    # Hindari division by zero
    df_fleet['capacity_ton'] = df_fleet['capacity_ton'].replace(0, 0.1)
    
    df_fleet['load_ratio'] = (df_fleet['avg_daily_waste_ton'] / df_fleet['capacity_ton']) * 100
    df_fleet['status'] = df_fleet['load_ratio'].apply(lambda x: "CRITICAL" if x > 110 else ("WARNING" if x > 90 else "SAFE"))

    # Layout: Scatter (Kiri) & Table (Kanan)
    col_a, col_b = st.columns([2, 1])
    
    with col_a:
        st.markdown("##### ⚖️ Volume Sampah vs Kapasitas Angkut")
        fig_sc = px.scatter(
            df_fleet, x="capacity_ton", y="avg_daily_waste_ton", 
            color="status", size="armada_total", hover_name="kecamatan",
            color_discrete_map={"SAFE": "green", "WARNING": "orange", "CRITICAL": "red"},
            labels={"capacity_ton": "Kapasitas (Ton)", "avg_daily_waste_ton": "Beban Sampah (Ton)"}
        )
        
        max_cap = df_fleet['capacity_ton'].max()
        max_load = df_fleet['avg_daily_waste_ton'].max()
        max_v = max(max_cap if pd.notna(max_cap) else 100, max_load if pd.notna(max_load) else 100)
        
        fig_sc.add_shape(type="line", x0=0, y0=0, x1=max_v, y1=max_v, line=dict(dash="dash", color="grey"))
        st.plotly_chart(fig_sc, use_container_width=True)

    with col_b:
        st.markdown("##### 🚨 Status Beban Kerja")
        st.dataframe(
            df_fleet[['kecamatan', 'load_ratio', 'status']].sort_values('load_ratio', ascending=False),
            hide_index=True,
            # Update format ke 2 desimal sesuai permintaan sebelumnya agar tidak 0%
            column_config={"load_ratio": st.column_config.ProgressColumn("Load %", format="%.2f%%", min_value=0, max_value=150)}
        )

    # --- GRAFIK KETERSEDIAAN ARMADA (BAR CHART) ---
    st.markdown("##### 🚛 Ketersediaan Armada (Total vs Operasional)")
    
    # Reshape data untuk Bar Chart
    df_bar = df_fleet.melt(
        id_vars=["kecamatan"], 
        value_vars=["armada_total", "armada_operasional"],
        var_name="Kategori", 
        value_name="Jumlah Unit"
    )
    
    fig_bar = px.bar(
        df_bar, 
        x="kecamatan", 
        y="Jumlah Unit", 
        color="Kategori", 
        barmode="group",
        color_discrete_map={"armada_total": "lightgray", "armada_operasional": "#1f77b4"},
        height=400
    )
    st.plotly_chart(fig_bar, use_container_width=True)

else:
    st.info("Tidak ada data armada untuk wilayah yang dipilih.")