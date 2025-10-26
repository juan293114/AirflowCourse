import json
from pathlib import Path
from typing import List
import pandas as pd

# Definimos las columnas según la documentación de la API OpenSky states/all
# Esto es esencial para asignar nombres a la lista de datos.
OPENSKY_STATES_COLUMNS = [
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
]


def copy_raw_to_bronze(
    raw_root: Path | str,
    bronze_path: Path | str,
) -> str:
    """
    Lee todos los snapshots JSON de la capa Raw, los consolida,
    les asigna columnas y los guarda como un único archivo Parquet en Bronce.
    """
    raw_root = Path(raw_root)
    bronze_path = Path(bronze_path)
    bronze_path.parent.mkdir(parents=True, exist_ok=True)

    # Busca todos los archivos JSON en todos los subdirectorios de la capa raw
    candidate_files = sorted(raw_root.glob("**/*.json"))

    # Creamos una lista para guardar temporalmente los DataFrames de cada archivo
    all_dataframes: List[pd.DataFrame] = []
    
    for json_file in candidate_files:
        payload = json.loads(json_file.read_text(encoding="utf-8"))

        # Validamos que el JSON tenga la estructura esperada
        if isinstance(payload, dict) and "states" in payload and "time" in payload:
            
            # Extraemos el tiempo del snapshot (cuándo se tomó la foto)
            snapshot_time_unix = payload["time"]
            # Extraemos la lista de vuelos
            states_list = payload["states"]

            # Si la lista de 'states' no está vacía, la procesamos
            if states_list:
                # 1. Creamos un DataFrame con los datos de vuelo de ESTE archivo
                df_temp = pd.DataFrame(states_list, columns=OPENSKY_STATES_COLUMNS)
                
                # 2. Añadimos la columna 'snapshot_time' a cada fila,
                #    usando el valor 'time' de la raíz del JSON.
                df_temp["snapshot_time"] = pd.to_datetime(snapshot_time_unix, unit="s", utc=True)
                
                # 3. Añadimos este DataFrame a nuestra lista
                all_dataframes.append(df_temp)

    # 4. Concatenamos todos los DataFrames de todos los archivos en uno solo
    if all_dataframes:
        df = pd.concat(all_dataframes, ignore_index=True)
    else:
        # Si no hay datos, creamos un DataFrame vacío con todas las columnas
        df = pd.DataFrame(columns=OPENSKY_STATES_COLUMNS + ["snapshot_time"])

    # 5. Limpieza básica de la capa Bronce
    if not df.empty:
        # Convertimos los timestamps de Unix a Datetime (UTC)
        df["time_position"] = pd.to_datetime(df["time_position"], unit="s", utc=True)
        df["last_contact"] = pd.to_datetime(df["last_contact"], unit="s", utc=True)
        
        # Limpiamos espacios en 'callsign' (un problema común)
        df["callsign"] = df["callsign"].str.strip()

    # 6. Guardamos el resultado final como un archivo Parquet
    df.to_parquet(bronze_path, index=False)
    
    return str(bronze_path)