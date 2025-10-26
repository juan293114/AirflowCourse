from pathlib import Path
import pandas as pd
import numpy as np

def build_hechos_trafico_aereo(
    silver_path: Path | str,
    output_path: Path | str,
) -> str:
    """
    Construye la tabla de hechos hechos_trafico_aereo desde la capa Silver.
    Genera las claves foráneas y selecciona las métricas/dimensiones.
    """
    silver_path = Path(silver_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        df = pd.read_parquet(silver_path)
    except Exception:
        # Si el archivo Silver no existe o está vacío, crea un hecho vacío
        fact = pd.DataFrame(columns=["id_tiempo_snapshot", "codigo_aeronave"])
        fact.to_parquet(output_path, index=False)
        return str(output_path)


    if "snapshot_time" not in df.columns:
        fact = pd.DataFrame(columns=["id_tiempo_snapshot", "codigo_aeronave"])
    else:
        fact = df.copy()

        # --- Crear Claves Foráneas ---

        # Creamos la clave para dim_tiempo_snapshot (formato YYYYMMDDHHMM)
        # Esta lógica DEBE ser idéntica a la usada en 'build_dim_tiempo_snapshot'
        fact["id_tiempo_snapshot"] = (
            fact["snapshot_time"]
            .dt.strftime('%Y%M%d%H%M')
            .astype(np.int64)
        )

        # Para las otras dimensiones, usaremos las claves naturales
        # (codigo_aeronave y fuente_posicion) para la relación en Power BI.

        # --- Crear Métricas Base ---
        fact["conteo_observaciones"] = 1

        # --- Seleccionar Columnas Finales ---
        selected_columns = [col for col in [
            
            # --- Claves Foráneas ---
            "id_tiempo_snapshot",  # Para dim_tiempo_snapshot
            "codigo_aeronave",     # Clave natural para dim_aeronave
            "fuente_posicion",   # Clave natural para dim_fuente_posicion
            
            # --- Dimensiones Degeneradas ---
            "codigo_vuelo",
            "estado_vuelo",
            
            # --- Métricas ---
            "latitud",
            "longitud",
            "altitud_pies",
            "velocidad_kmh",
            "latencia_segundos",
            "conteo_observaciones",

        ] if col in fact.columns]

        fact = fact[selected_columns]

        # Aseguramos la granularidad (una observación por avión, por snapshot)
        fact = fact.drop_duplicates(subset=["id_tiempo_snapshot", "codigo_aeronave"])

    fact.to_parquet(output_path, index=False)
    return str(output_path)