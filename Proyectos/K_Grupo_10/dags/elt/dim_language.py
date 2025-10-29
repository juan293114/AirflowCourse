import pandas as pd
from pathlib import Path

def build_dim_language(bronze_path: Path | str, output_path: Path | str) -> str:
    bronze_path = Path(bronze_path)
    output_path = Path(output_path)

    df = pd.read_parquet(bronze_path)

    if "languages" not in df.columns:
        print("⚠️ No existe la columna 'languages' en el archivo fuente.")
        return "Archivo sin columna languages."

    # Limpiar y normalizar idiomas
    langs = set()
    for item in df["languages"].dropna():
        if isinstance(item, dict):
            langs.update(item.values())
        elif isinstance(item, list):
            langs.update(item)
        elif isinstance(item, str):
            langs.add(item)

    if not langs:
        print("⚠️ No se encontraron idiomas válidos.")
        return "Archivo vacío."

    df_languages = pd.DataFrame(
        [{"language_id": i + 1, "language_name": l} for i, l in enumerate(sorted(langs))]
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_languages.to_parquet(output_path, index=False)

    print(f"✅ Archivo generado correctamente: {output_path}")
    return str(output_path)
