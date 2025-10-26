from pathlib import Path
import pandas as pd
import numpy as np

def build_dim_tiempo_snapshot(
    silver_path: Path | str,
    output_path: Path | str,
) -> str:
    """
    Construye la dim_tiempo_snapshot desde la capa Silver.
    Extrae los timestamps únicos, los agrega a nivel de MINUTO
    y los enriquece con atributos de fecha/hora.
    """
    silver_path = Path(silver_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Columnas finales de la dimensión
    DIM_COLUMNS = [
        "id_tiempo_snapshot",
        "snapshot_time",
        "fecha",
        "hora",
        "minuto",
        "dia_semana",
        "franja_horaria"
    ]

    try:
        df = pd.read_parquet(silver_path)
    except Exception:
        dim = pd.DataFrame(columns=DIM_COLUMNS)
        dim.to_parquet(output_path, index=False)
        return str(output_path)

    if "snapshot_time" not in df.columns:
        dim = pd.DataFrame(columns=DIM_COLUMNS)
        dim.to_parquet(output_path, index=False)
        return str(output_path)

    # Obtenemos un DataFrame con solo los timestamps únicos
    dim = pd.DataFrame(df["snapshot_time"].unique(), columns=["snapshot_time"])
    dim.sort_values("snapshot_time", inplace=True, ignore_index=True)

    # Creamos los atributos de tiempo
    dim["fecha"] = dim["snapshot_time"].dt.date
    dim["hora"] = dim["snapshot_time"].dt.hour
    dim["minuto"] = dim["snapshot_time"].dt.minute

    # Eliminamos duplicados a nivel de minuto, conservando el más reciente
    dim.drop_duplicates(subset=["fecha", "hora", "minuto"], keep='last', inplace=True, ignore_index=True)

    # Creamos la clave principal (ID) A NIVEL DE MINUTO
    # Se eliminan los segundos ('%S') para que coincida con la granularidad
    dim["id_tiempo_snapshot"] = dim["snapshot_time"].dt.strftime('%Y%m%d%H%M').astype(np.int64)

    # Mapeamos el día de la semana a Español
    dias_map = {
        0: 'Lunes', 1: 'Martes', 2: 'Miércoles', 3: 'Jueves',
        4: 'Viernes', 5: 'Sábado', 6: 'Domingo'
    }
    dim["dia_semana"] = dim["snapshot_time"].dt.dayofweek.map(dias_map)

    # Creamos la columna 'franja_horaria'
    conditions = [
        (dim['hora'] >= 6) & (dim['hora'] <= 11),
        (dim['hora'] >= 12) & (dim['hora'] <= 18),
        (dim['hora'] >= 19) & (dim['hora'] <= 23),
        (dim['hora'] >= 0) & (dim['hora'] <= 5)
    ]
    choices = ['Mañana', 'Tarde', 'Noche', 'Madrugada']
    dim["franja_horaria"] = np.select(conditions, choices, default='N/A')
    
    # Ordenamos las columnas y guardamos
    dim = dim[DIM_COLUMNS]
    dim.to_parquet(output_path, index=False)
    
    return str(output_path)