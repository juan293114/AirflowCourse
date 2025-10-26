from pathlib import Path
import pandas as pd
import numpy as np


def _clean_column_name(name: str) -> str:
    """
    Limpia los nombres de las columnas para que sean estándar.
    """
    return (
        name.strip()
        .lower()
        .replace(" ", "_")
        .replace(".", "_")
        .replace("-", "_")
    )


def transform_bronze_to_silver(
    bronze_path: Path | str,
    silver_path: Path | str,
) -> str:
    """
    Lee el archivo Parquet de Bronce, aplica limpieza, filtrado,
    y enriquecimiento, y lo guarda en la capa Silver.
    """
    bronze_path = Path(bronze_path)
    silver_path = Path(silver_path)
    silver_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(bronze_path)

    if df.empty:
        df.to_parquet(silver_path, index=False)
        return str(silver_path)

    # Limpieza estándar de nombres de columna
    df.columns = [_clean_column_name(col) for col in df.columns]

    # Convertimos la columna 'sensors' (lista) a un string
    if "sensors" in df.columns:
        df["sensors_str"] = df["sensors"].apply(
            lambda value: ",".join(str(item) for item in value) if isinstance(value, (list, tuple)) else None
        )

    # Transformaciones Específicas de OpenSky
    
    # FILTRADO (Eliminación de "Ruido")
    
    # Nos quedamos solo con aeronaves que están en el aire
    df = df[df["on_ground"] == False].copy()

    # Eliminamos registros sin datos de posición
    df = df.dropna(subset=["longitude", "latitude"])

    # Eliminamos registros sin identificador de vuelo
    df = df.dropna(subset=["callsign"])
    df = df[df["callsign"] != ""]

    
    # ENRIQUECIMIENTO DE DATOS
    
    # Conversión de Unidades a estándar de negocio
    df["velocidad_kmh"] = df["velocity"] * 3.6      # Metros/segundo a Kilómetros/hora
    df["altitud_pies"] = df["baro_altitude"] * 3.28084  # Metros a Pies
    
    # Creación de Categorías
    # Estado del Vuelo (Subiendo, Bajando, Nivelado)
    conditions = [
        df["vertical_rate"] > 1,
        df["vertical_rate"] < -1,
    ]
    choices = ["Subiendo", "Bajando"]
    df["estado_vuelo"] = np.select(conditions, choices, default="Nivelado")

    # Mapeo de Fuente de Posición
    pos_map = {
        0: "ADS-B",
        1: "ASTERIX",
        2: "MLAT",
        3: "FLARM"
    }
    df["fuente_posicion"] = df["position_source"].map(pos_map).fillna("Desconocido")
    
    
    # CALIDAD DE DATOS (Limpieza Final)

    # Cálculo de Latencia (diferencia entre la captura y el reporte del avión)
    df["latencia_segundos"] = (df["snapshot_time"] - df["time_position"]).dt.total_seconds()
    
    # Filtramos registros "viejos" (latencia > 5 minutos)
    MAX_LATENCY_SECONDS = 300
    df = df[df["latencia_segundos"] <= MAX_LATENCY_SECONDS]

    # Eliminamos duplicados (una observación por avión por snapshot)
    df = df.drop_duplicates(subset=["snapshot_time", "icao24"])
    
    # Renombramos columnas a Español para el dashboard
    df.rename(columns={
        'latitude': 'latitud',
        'longitude': 'longitud',
        'icao24': 'codigo_aeronave',
        'callsign': 'codigo_vuelo',
        'origin_country':'pais_origen'
    }, inplace=True)

    
    # Selección Final de Columnas
    
    # Elegimos las columnas que queremos en nuestra capa Silver
    COLUMNAS_SILVER = [
        # Claves e IDs
        "snapshot_time",
        "codigo_aeronave", 
        "codigo_vuelo",    
        "pais_origen",     
        
        # Métricas Limpias (Nuevas Unidades)
        "velocidad_kmh",
        "altitud_pies",
        
        # Posición
        "latitud",         
        "longitud",        
        
        # Categorías (Nuevas Columnas)
        "estado_vuelo",
        "fuente_posicion",
        
        # Columnas de Calidad y Contexto
        "latencia_segundos",
        "sensors_str",
    ]
    
    # Filtramos el DataFrame para que solo contenga estas columnas
    df = df[COLUMNAS_SILVER]

    # Guardado en Silver
    df.to_parquet(silver_path, index=False)
    
    return str(silver_path)