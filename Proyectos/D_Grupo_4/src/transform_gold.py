import os
import pandas as pd
from datetime import datetime

SILVER_DIR = "data/silver"
GOLD_DIR = "data/gold"

os.makedirs(GOLD_DIR, exist_ok=True)


silver_files = sorted(
    [f for f in os.listdir(SILVER_DIR) if f.endswith(".csv")],
    key=lambda x: os.path.getmtime(os.path.join(SILVER_DIR, x)),
    reverse=True
)

if not silver_files:
    print("⚠️ No hay archivos en data/silver.")
else:
    latest_file = silver_files[0]
    input_path = os.path.join(SILVER_DIR, latest_file)
    print(f"Procesando archivo: {input_path}")

    df = pd.read_csv(input_path)


    artist_summary = (
        df.groupby("artist", as_index=False)
          .agg({"listeners": "sum"})
          .sort_values(by="listeners", ascending=False)
    )

  
    total = artist_summary["listeners"].sum()
    artist_summary["pct_listeners"] = round(artist_summary["listeners"] / total * 100, 2)
    artist_summary["pct_acumulado"] = artist_summary["pct_listeners"].cumsum()

  
    top10 = artist_summary.head(10)

 
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    gold_file = os.path.join(GOLD_DIR, f"gold_top_artists_{timestamp}.csv")
    top10.to_csv(gold_file, index=False, encoding="utf-8")

    print(f"✅ Archivo GOLD generado en: {gold_file}")
    print("\n🏆 Top 10 artistas más escuchados en Perú:")
    print(top10)