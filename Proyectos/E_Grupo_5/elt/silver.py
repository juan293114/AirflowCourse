from pathlib import Path

def _clean_column_name(name: str) -> str:
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
    import pandas as pd
    import numpy as np

    bronze_path = Path(bronze_path)
    silver_path = Path(silver_path)
    silver_path.parent.mkdir(parents=True, exist_ok=True)

    # Leer Bronze
    df = pd.read_parquet(bronze_path)

    # Normalizar nombres de columnas
    df.columns = [_clean_column_name(col) for col in df.columns]

    # Limpiar valores complejos: listas, dicts, arrays
    for col in df.columns:
        def normalize(value):
            if isinstance(value, list):
                return tuple(value)
            if isinstance(value, np.ndarray):
                return tuple(value.tolist())
            if isinstance(value, dict):
                return tuple(sorted(value.items()))
            return value
        df[col] = df[col].apply(normalize)

    # Eliminar duplicados
    df = df.drop_duplicates()

    # Guardar en Silver
    df.to_parquet(silver_path, index=False)
    return str(silver_path)
