from pathlib import Path
import pandas as pd

def build_dim_region(bronze_path: Path | str, output_path: Path | str) -> str:
    bronze_path = Path(bronze_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(bronze_path)

    regiones = df["region"].dropna().unique().tolist() if "region" in df.columns else []

    dim = pd.DataFrame(
        [{"region_id": i + 1, "region_nombre": r} for i, r in enumerate(sorted(regiones))]
    )

    dim.to_parquet(output_path, index=False)
    print(f"✅ Archivo generado: {output_path}")
    return str(output_path)
