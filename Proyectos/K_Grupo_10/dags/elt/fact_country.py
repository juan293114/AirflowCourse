from pathlib import Path
import pandas as pd

def build_fact_country(bronze_path: Path | str, output_path: Path | str) -> str:
    bronze_path = Path(bronze_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # 🔹 Leer datos
    df = pd.read_parquet(bronze_path)

    # Asegurar columnas necesarias
    fact_cols = ["name", "region", "subregion", "languages", "currencies", "population", "area"]
    df = df[[c for c in fact_cols if c in df.columns]].rename(columns={
        "name": "pais_nombre",
        "region": "region_nombre",
        "subregion": "subregion_nombre",
        "languages": "idioma",
        "currencies": "moneda",
        "population": "poblacion",
        "area": "area_km2"
    })

    # 🔹 Crear IDs internos de la tabla fact
    df.insert(0, "pais_id", range(1, len(df) + 1))

    # 🔹 Cargar dimensiones GOLD para mapear
    gold_root = Path("/opt/airflow/data_lake/gold")
    try:
        dim_region = pd.read_parquet(gold_root / "dim_region.parquet")
        dim_language = pd.read_parquet(gold_root / "dim_language.parquet")
        dim_currency = pd.read_parquet(gold_root / "dim_currency.parquet")
    except Exception as e:
        print(f"⚠️ No se pudieron cargar las dimensiones: {e}")
        return "Error al mapear dimensiones"

    # 🔹 Mapear IDs desde las dimensiones
    df = df.merge(dim_region, how="left", left_on="region_nombre", right_on="region_nombre")
    df = df.merge(dim_language, how="left", left_on="idioma", right_on="language_name")
    df = df.merge(dim_currency, how="left", left_on="moneda", right_on="currency_codigo")

    # 🔹 Seleccionar columnas finales con relaciones
    fact = df[[
        "pais_id",
        "pais_nombre",
        "region_id",
        "language_id",
        "currency_id",
        "subregion_nombre",
        "poblacion",
        "area_km2"
    ]].drop_duplicates().reset_index(drop=True)

    # 🔹 Guardar resultado
    fact.to_parquet(output_path, index=False)
    print(f"✅ FACT COUNTRY generado correctamente en: {output_path}")
    return str(output_path)
