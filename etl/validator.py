# etl/validator.py
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def validate_schema(df, required_columns, dataset_name):
    """
    Cek apakah kolom yang dibutuhkan ada semua.
    """
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        logger.error(f"❌ [VALIDASI GAGAL] {dataset_name}: Kolom hilang -> {missing}")
        return False
    return True

def validate_numeric(df, numeric_columns, dataset_name):
    """
    Cek apakah kolom angka benar-benar berisi angka (bukan teks 'abc' atau error).
    """
    is_valid = True
    for col in numeric_columns:
        # Coba konversi ke numeric, jika gagal jadi NaN
        non_numeric = pd.to_numeric(df[col], errors='coerce').isna().sum()
        
        # Jika ada baris yang gagal dikonversi (dan aslinya tidak kosong)
        if non_numeric > 0:
            # Cek apakah itu benar-benar data sampah atau cuma null biasa
            # Di sini kita asumsikan kolom numeric tidak boleh berisi teks aneh
            logger.warning(f"⚠️ [VALIDASI WARNING] {dataset_name}: Kolom '{col}' memiliki {non_numeric} baris non-numerik.")
            # Kita bisa return False jika ingin sangat ketat, atau True jika toleran
            # Untuk firewall ketat:
            # is_valid = False 
    return is_valid

def validate_waste_data(df):
    """
    Validasi spesifik untuk waste.csv
    """
    REQUIRED_COLS = ['tanggal', 'kecamatan', 'volume_ton', 'jenis_sampah', 'sumber_sampah']
    NUMERIC_COLS = ['volume_ton']
    
    print("   🛡️  Menjalankan Validasi Waste Data...")
    
    if df.empty:
        logger.error("❌ [VALIDASI GAGAL] File waste.csv kosong.")
        return False

    if not validate_schema(df, REQUIRED_COLS, "Waste Data"):
        return False

    # Cek Critical Nulls (Kecamatan & Tanggal tidak boleh kosong)
    if df['kecamatan'].isna().any() or df['tanggal'].isna().any():
        logger.error("❌ [VALIDASI GAGAL] Ada baris dengan Kecamatan atau Tanggal kosong.")
        return False

    # Cek Tipe Data Angka
    # Kita tes konversi bayangan (tidak mengubah df asli)
    if not validate_numeric(df, NUMERIC_COLS, "Waste Data"):
        return False

    print("   ✅ Validasi Waste Data Lulus.")
    return True

def validate_sipsn_data(df):
    """
    Validasi spesifik untuk sipsn.csv
    """
    REQUIRED_COLS = ['kecamatan', 'armada_total', 'penduduk', 'luas_km2']
    NUMERIC_COLS = ['armada_total', 'penduduk', 'luas_km2']

    print("   🛡️  Menjalankan Validasi SIPSN Data...")

    if df.empty:
        logger.error("❌ [VALIDASI GAGAL] File sipsn.csv kosong.")
        return False

    if not validate_schema(df, REQUIRED_COLS, "SIPSN Data"):
        return False
    
    if df['kecamatan'].isna().any():
        logger.error("❌ [VALIDASI GAGAL] Ada baris dengan Kecamatan kosong.")
        return False

    print("   ✅ Validasi SIPSN Data Lulus.")
    return True