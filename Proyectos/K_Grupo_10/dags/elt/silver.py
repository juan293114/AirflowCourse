# transform_bronze_to_silver_paises.py
from pathlib import Path
import pandas as pd

def _limpiar_nombre_columna(nombre: str) -> str:
    return (
        nombre.strip()
        .lower()
        .replace(" ", "_")
        .replace(".", "_")
        .replace("-", "_")
    )


def transformar_bronze_a_silver(
    ruta_bronze: Path | str,
    ruta_silver: Path | str,
) -> str:
    """
    Transforma un parquet BRONZE (paises) a SILVER:
    - Limpia nombres de columnas
    - Renombra los campos al español
    - Elimina duplicados
    """
    ruta_bronze = Path(ruta_bronze)
    ruta_silver = Path(ruta_silver)
    ruta_silver.parent.mkdir(parents=True, exist_ok=True)

    # Leer parquet BRONZE
    df = pd.read_parquet(ruta_bronze)

    # Limpiar nombres de columnas
    df.columns = [_limpiar_nombre_columna(col) for col in df.columns]

    # Renombrar columnas al español
    rename_map = {
        "name": "nombre",
        "capital": "capital",
        "region": "region",
        "subregion": "subregion",
        "population": "poblacion",
        "area": "area",
        "languages": "idiomas",
        "currencies": "monedas",
        "flag_url": "bandera_url",
        "timezones": "zonas_horarias",
        "cca3": "codigo_pais"
    }

    df = df.rename(columns=rename_map)

    # Seleccionar columnas de interés
    columnas_finales = [
        "nombre",
        "capital",
        "region",
        "subregion",
        "poblacion",
        "area",
        "idiomas",
        "monedas",
        "bandera_url",
        "zonas_horarias",
        "codigo_pais"
    ]

    columnas_finales = [c for c in columnas_finales if c in df.columns]
    df_silver = df[columnas_finales].drop_duplicates().reset_index(drop=True)

    # Ordenar por nombre
    if "nombre" in df_silver.columns:
        df_silver = df_silver.sort_values("nombre").reset_index(drop=True)

    # Guardar como parquet
    df_silver.to_parquet(ruta_silver, index=False)
    print(f"✅ SILVER generado en: {ruta_silver.absolute()}")

    return str(ruta_silver)


if __name__ == "__main__":
    print("🔁 Transformando capa BRONZE → SILVER (paises)...")
    salida = transformar_bronze_a_silver("data/bronze/paises_bronze.parquet", "data/silver/paises_silver.parquet")
    print("✅ SILVER guardado en:", salida)
