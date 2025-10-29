import pandas as pd
from pathlib import Path

def copy_raw_to_bronze_csv(raw_file: Path | str, bronze_path: Path | str) -> str:
    """
    Toma un archivo CSV desde /raw y lo guarda como Parquet en /bronze.
    """
    raw_file = Path(raw_file)
    bronze_path = Path(bronze_path)
    bronze_path.parent.mkdir(parents=True, exist_ok=True)

    if not raw_file.exists():
        print(f"⚠️ No se encontró el archivo CSV: {raw_file}")
        return ""

    df = pd.read_csv(raw_file)
    if df.empty:
        print(f"⚠️ El archivo CSV está vacío: {raw_file}")
        return ""

    print(f"📊 DataFrame con {len(df)} registros y columnas: {list(df.columns)}")

    df.to_parquet(bronze_path, index=False)
    print(f"✅ Archivo BRONZE generado: {bronze_path}")

    return str(bronze_path)
