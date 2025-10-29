import os
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime


load_dotenv()

API_KEY = os.getenv("LASTFM_API_KEY")
url = "https://ws.audioscrobbler.com/2.0/"

params = {
    "method": "geo.gettoptracks",
    "country": "Peru",
    "api_key": API_KEY,
    "format": "json"
}

response = requests.get(url, params=params)
print("Estado de la solicitud:", response.status_code)

data = response.json()

if "tracks" not in data:
    print("❌ No se encontró la clave 'tracks' en la respuesta de la API.")
else:
    tracks = data["tracks"]["track"][:50] 
    df = pd.DataFrame([
        {
            "rank": t["@attr"]["rank"],
            "track_name": t["name"],
            "artist": t["artist"]["name"],
            "listeners": t["listeners"],
            "url": t["url"]
        }
        for t in tracks
    ])

    
    RAW_DIR = os.path.join("data", "raw")
    os.makedirs(RAW_DIR, exist_ok=True)

   
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(RAW_DIR, f"top_tracks_peru_{timestamp}.csv")
    df.to_csv(output_path, index=False, encoding="utf-8")

    print(f"✅ Archivo CSV guardado en: {output_path}")
    print(df.head())