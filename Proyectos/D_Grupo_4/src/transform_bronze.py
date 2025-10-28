import os
import pandas as pd
from datetime import datetime

RAW_DIR = os.path.join("data", "raw")
BRONZE_DIR = os.path.join("data", "bronze")

def transform_raw_to_bronze():
    
    raw_files = sorted(
        [f for f in os.listdir(RAW_DIR) if f.endswith(".csv")],
        key=lambda x: os.path.getmtime(os.path.join(RAW_DIR, x)),
        reverse=True
    )
    if not raw_files:
        print("⚠️ No hay archivos en data/raw.")
        return

    latest_raw = raw_files[0]
    raw_path = os.path.join(RAW_DIR, latest_raw)
    print(f"Procesando archivo: {raw_path}")

   
    df = pd.read_csv(raw_path)

    
    df = df.drop_duplicates(subset=["track_name", "artist"])
    df["track_name"] = df["track_name"].str.strip()
    df["artist"] = df["artist"].str.strip()
    df["listeners"] = pd.to_numeric(df["listeners"], errors="coerce").fillna(0).astype(int)

   
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    bronze_path = os.path.join(BRONZE_DIR, f"bronze_tracks_peru_{timestamp}.csv")
    df.to_csv(bronze_path, index=False, encoding="utf-8")

    print(f"✅ Archivo transformado y guardado en: {bronze_path}")
    print(df.head())

if __name__ == "__main__":
    transform_raw_to_bronze()