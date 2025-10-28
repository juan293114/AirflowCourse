import os
import pandas as pd
from datetime import datetime

BRONZE_DIR = "data/bronze"
SILVER_DIR = "data/silver"

os.makedirs(SILVER_DIR, exist_ok=True)


bronze_files = sorted(
    [f for f in os.listdir(BRONZE_DIR) if f.endswith(".csv")],
    key=lambda x: os.path.getmtime(os.path.join(BRONZE_DIR, x)),
    reverse=True
)

if not bronze_files:
    print("⚠️ No hay archivos en data/bronze.")

else:
    latest_file = bronze_files[0]
    input_path = os.path.join(BRONZE_DIR, latest_file)
    print(f"Procesando archivo: {input_path}")

    df = pd.read_csv(input_path)

    
    df["listeners"] = df["listeners"].astype(int)

    
    total = df["listeners"].sum()
    df["pct_listeners"] = round(df["listeners"] / total * 100, 2)

   
    df["artist"] = df["artist"].str.strip()
    df["track_name"] = df["track_name"].str.strip()

  
    df = df.sort_values(by="listeners", ascending=False).reset_index(drop=True)

    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(SILVER_DIR, f"silver_tracks_peru_{timestamp}.csv")
    df.to_csv(output_path, index=False, encoding="utf-8")

    print(f"✅ Archivo transformado y guardado en: {output_path}")
    print(df.head())