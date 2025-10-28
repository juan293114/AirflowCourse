import json
from pathlib import Path
import pandas as pd


def copy_raw_to_bronze(
    raw_root: Path | str,
    bronze_path: Path | str,
) -> str:
    """
    Procesa los archivos JSON descargados desde la API del MEF (capa Raw)
    y los consolida en un único archivo Parquet (capa Bronze).

    Parámetros:
    ----------
    raw_root : Path | str
        Carpeta raíz donde están los archivos JSON descargados.
    bronze_path : Path | str
        Ruta donde se guardará el archivo Parquet consolidado.

    Retorna:
    -------
    str : ruta completa del archivo Parquet generado.
    """
    raw_root = Path(raw_root)
    bronze_path = Path(bronze_path)
    bronze_path.parent.mkdir(parents=True, exist_ok=True)

    # Busca los archivos JSON descargados desde la API
    candidate_files = sorted(raw_root.glob("**/mef_data_page_*.json"))

    if not candidate_files:
        raise FileNotFoundError(f"No se encontraron archivos JSON en {raw_root}")

    all_records: list[dict] = []

    for json_file in candidate_files:
        try:
            payload = json.loads(json_file.read_text(encoding="utf-8"))
            if isinstance(payload, list):
                all_records.extend(payload)
            elif isinstance(payload, dict):
                # Si viene en formato de diccionario, intenta extraer "records"
                records = payload.get("result", {}).get("records", [])
                all_records.extend(records)
        except json.JSONDecodeError:
            print(f"⚠️ Archivo inválido: {json_file}")
            continue

    # Convertir los registros a DataFrame
    df = pd.DataFrame(all_records)

    if df.empty:
        raise ValueError("⚠️ No se encontraron registros válidos para procesar.")

    # Limpieza básica: quitar columnas vacías y duplicadas
    df = df.dropna(axis=1, how="all").drop_duplicates()

    # Normaliza nombres de columnas (sin espacios ni mayúsculas)
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    # Guardar en formato Parquet (Capa Bronze)
    df.to_parquet(bronze_path, index=False)
    print(f"✅ Archivo Bronze generado: {bronze_path} ({len(df)} filas)")

    return str(bronze_path)