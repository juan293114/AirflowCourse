from pathlib import Path
import pandas as pd

def build_dim_country(bronze_path: Path | str, output_path: Path | str) -> str:
    bronze_path = Path(bronze_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(bronze_path)

    if "cca3" not in df.columns:
        df["cca3"] = df["name"].apply(lambda x: f"ISO_{x[:3].upper()}" if isinstance(x, str) else "N/A")

    dim = (
        df[["name", "region", "subregion", "cca3"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    dim.insert(0, "country_id", range(1, len(dim) + 1))
    dim.to_parquet(output_path, index=False)

    print(f"✅ Archivo generado: {output_path}")
    return str(output_path)
