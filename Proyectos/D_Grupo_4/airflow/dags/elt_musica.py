import os
import csv
from datetime import datetime, date
import pendulum
import requests
import shutil

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


DATA_LAKE  = "/opt/airflow/data_lake"
RAW_DIR    = os.path.join(DATA_LAKE, "raw")
BRONZE_DIR = os.path.join(DATA_LAKE, "bronze")
SILVER_DIR = os.path.join(DATA_LAKE, "silver")
GOLD_DIR   = os.path.join(DATA_LAKE, "gold")
STAR_DIR   = os.path.join(GOLD_DIR, "star")
for d in (RAW_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR, STAR_DIR):
    os.makedirs(d, exist_ok=True)


LIMA_TZ = pendulum.timezone("America/Lima")
API_KEY = Variable.get("LASTFM_API_KEY") 

def _latest_csv(folder: str):
    files = [f for f in os.listdir(folder) if f.lower().endswith(".csv")]
    if not files:
        return None
    files.sort(key=lambda f: os.path.getmtime(os.path.join(folder, f)), reverse=True)
    return os.path.join(folder, files[0])

# =============== 1) RAW ===============
def ingest_raw(**context):
    url = "https://ws.audioscrobbler.com/2.0/"
    country = "Peru"
    snapshot_date = pendulum.instance(context["logical_date"]).in_timezone(LIMA_TZ).date().isoformat()

    params = {
        "method": "geo.gettoptracks",
        "country": country,
        "api_key": API_KEY,
        "format": "json",
        "limit": 200,
        "page": 1,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    tracks = data.get("tracks", {}).get("track", [])

    rows = []
    for i, t in enumerate(tracks, start=1):
        name = t.get("name", "").strip()
        artist = (t.get("artist", {}) or {}).get("name", "").strip()
        listeners_raw = t.get("listeners", "0")
        try:
            listeners = int(str(listeners_raw).strip())
        except Exception:
            listeners = 0
        url_ = t.get("url", "").strip()
        rows.append([i, name, artist, listeners, url_, country, snapshot_date])

    ts = pendulum.instance(context["logical_date"]).in_timezone(LIMA_TZ).format("YYYYMMDD_HHmmss")
    out_path = os.path.join(RAW_DIR, f"top_tracks_peru_{ts}.csv")

    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["rank", "track_name", "artist", "listeners", "url", "country", "snapshot_date"])
        w.writerows(rows)

    print(f"RAW guardado: {out_path} ({len(rows)} filas)")

# =============== 2) BRONZE ===============
def to_bronze(**_):
    inp = _latest_csv(RAW_DIR)
    if not inp:
        print("⚠️ No hay archivos en RAW.")
        return

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = os.path.join(BRONZE_DIR, f"bronze_tracks_peru_{ts}.csv")

    seen = set()
    cleaned = []
    with open(inp, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            key = (row.get("track_name", "").strip(),
                   row.get("artist", "").strip(),
                   row.get("url", "").strip())
            if key in seen:
                continue
            seen.add(key)

            try:
                listeners = int(str(row.get("listeners", "0")).strip())
            except Exception:
                listeners = 0

            cleaned.append({
                "track_name": row.get("track_name", "").strip(),
                "artist": row.get("artist", "").strip(),
                "listeners": listeners,
                "url": row.get("url", "").strip(),
                "country": row.get("country", "").strip(),
                "snapshot_date": row.get("snapshot_date", "").strip(),
            })

    with open(out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["track_name", "artist", "listeners", "url", "country", "snapshot_date"]
        )
        w.writeheader()
        w.writerows(cleaned)

    print(f"BRONZE guardado: {out} ({len(cleaned)} filas)")

# =============== 3) SILVER ===============
def to_silver(**_):
    inp = _latest_csv(BRONZE_DIR)
    if not inp:
        print("⚠️ No hay archivos en BRONZE.")
        return

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = os.path.join(SILVER_DIR, f"silver_tracks_peru_{ts}.csv")

    rows = []
    total = 0
    with open(inp, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        tmp = []
        for row in r:
            listeners = int(row.get("listeners", 0))
            total += listeners
            tmp.append({
                "track_name": row.get("track_name", ""),
                "artist": row.get("artist", ""),
                "listeners": listeners,
                "url": row.get("url", ""),
            })

        tmp.sort(key=lambda x: x["listeners"], reverse=True)
        for idx, row in enumerate(tmp, start=1):
            pct = round(row["listeners"] / total * 100, 2) if total else 0.0
            rows.append({
                "rank": idx,
                **row,
                "pct_listeners": pct
            })

    with open(out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["rank", "track_name", "artist", "listeners", "url", "pct_listeners"],
        )
        w.writeheader()
        w.writerows(rows)

    print(f"SILVER guardado: {out} ({len(rows)} filas, total listeners={total})")

# =============== 4) MODELO ESTRELLA ===============
def build_star_from_silver(**_):
    import glob

    silver = _latest_csv(SILVER_DIR)
    if not silver:
        print("⚠️ No hay archivos en SILVER para STAR.")
        return


    for f in glob.glob(os.path.join(STAR_DIR, "*.csv")):
        try:
            os.remove(f)
        except Exception as e:
            print(f"⚠️ No se pudo eliminar {f}: {e}")
    print("🧹 STAR limpio, se crearán nuevos archivos.")


    with open(silver, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))


    base = os.path.basename(silver)
    date_key = int(base[-19:-11])  
    y, m, d = int(str(date_key)[0:4]), int(str(date_key)[4:6]), int(str(date_key)[6:8])
    dt = date(y, m, d)

    country_value = "Peru"
    snapshot_date_value = dt.isoformat() 


    artist2id, dim_artist = {}, []
    for r in rows:
        a = r["artist"].strip()
        if a and a not in artist2id:
            artist2id[a] = len(artist2id) + 1
            dim_artist.append({"artist_id": artist2id[a], "artist_name": a})


    track2id, dim_track = {}, []
    for r in rows:
        key = (r["track_name"].strip(), r["artist"].strip())
        if key not in track2id:
            track2id[key] = len(track2id) + 1
            dim_track.append({
                "track_id": track2id[key],
                "track_name": key[0],
                "artist_id": artist2id.get(key[1], 0),
                "url": r["url"].strip()
            })

   
    fact = []
    for r in rows:
        artist = r["artist"].strip()
        track  = r["track_name"].strip()
        fact.append({
            "country":        country_value,
            "date_key":       date_key,
            "listeners":      int(r["listeners"]),
            "pct_listeners":  float(r.get("pct_listeners", 0) or 0),
            "snapshot_date":  snapshot_date_value,
            "track_id":       track2id.get((track, artist), 0),
        })

    
    day_name = dt.strftime("%A")    
    month_name = dt.strftime("%B")
    is_weekend = 1 if dt.weekday() >= 5 else 0
    dim_date = [{
        "date_key":   date_key,
        "date":       dt.isoformat(),
        "day":        d,
        "day_name":   day_name,
        "is_weekend": is_weekend,
        "month":      m,
        "month_name": month_name,
        "quarter":    (m - 1)//3 + 1,
        "year":       y,
    }]

 
    outputs = {
        "DimArtist_20251020_162257.csv": (["artist_id", "artist_name"],                        dim_artist),
        "DimTrack_20251020_162913.csv":  (["track_id", "track_name", "artist_id", "url"],      dim_track),
        "FactTrackDaily_20251017.csv":   (["country","date_key","listeners","pct_listeners","snapshot_date","track_id"], fact),
        "DimDate.csv":                   (["date_key","date","day","day_name","is_weekend","month","month_name","quarter","year"], dim_date),
    }
    for fname, (fields, data) in outputs.items():
        out = os.path.join(STAR_DIR, fname)
        with open(out, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(data)
        print(f"✅ Archivo actualizado: {fname} ({len(data)} filas)")

# =============== 5) GOLD ===============
def to_gold(**_):
    inp = _latest_csv(SILVER_DIR)
    if not inp:
        print("⚠️ No hay archivos en SILVER.")
        return

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = os.path.join(GOLD_DIR, f"gold_top_artists_{ts}.csv")


    agg = {}
    with open(inp, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            artist = row["artist"]
            listeners = int(row["listeners"])
            agg[artist] = agg.get(artist, 0) + listeners

    items = sorted(agg.items(), key=lambda x: x[1], reverse=True)
    total = sum(v for _, v in items)

    out_rows = []
    acc = 0.0
    for artist, listeners in items[:10]:
        pct = round(listeners / total * 100, 2) if total else 0.0
        acc = round(acc + pct, 2)
        out_rows.append(
            {"artist": artist, "listeners": listeners, "pct_listeners": pct, "pct_acumulado": acc}
        )

    with open(out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["artist", "listeners", "pct_listeners", "pct_acumulado"])
        w.writeheader()
        w.writerows(out_rows)

    print(f"GOLD guardado: {out}")
    print("Top 3 preview:", out_rows[:3])

    stable = os.path.join(GOLD_DIR, "gold_top_artists_latest.csv")
    shutil.copyfile(out, stable)
    print(f"📌 Copia publicada para Power BI: {stable}")

# --------- DAG ---------
with DAG(
    dag_id="elt_musica_peru",
    description="ETL Last.fm Peru: RAW->BRONZE->SILVER->STAR->GOLD",
    start_date=pendulum.datetime(2025, 10, 1, tz=LIMA_TZ),
    schedule="0 5 * * *",  
    catchup=False,
    tags=["music", "elt", "peru"],
) as dag:

    t_raw    = PythonOperator(task_id="ingest_raw", python_callable=ingest_raw)
    t_bronze = PythonOperator(task_id="to_bronze",  python_callable=to_bronze)
    t_silver = PythonOperator(task_id="to_silver",  python_callable=to_silver)
    t_star   = PythonOperator(task_id="build_star", python_callable=build_star_from_silver)
    t_gold   = PythonOperator(task_id="to_gold",    python_callable=to_gold)

 
    t_raw >> t_bronze >> t_silver
    t_silver >> [t_star, t_gold]