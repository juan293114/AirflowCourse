import pandas as pd
from pathlib import Path

def build_dim_unidad(silver_path: str, output_path: str):
    silver_path = Path(silver_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(silver_path)
    if "unidad_id" not in df.columns:
        dim = pd.DataFrame(columns=["unidad_id","unidad_name"])
    else:
        columns = ["unidad_id","unidad_name","unidad_tipo"]
        available = [col for col in columns if col in df.columns]
        dim = df[available].drop_duplicates(subset=["unidad_id"]).reindex(columns=columns, fill_value=None)
    dim.sort_values("unidad_name", inplace=True, ignore_index=True)
    dim.to_parquet(output_path, index=False)
    print(f"Dim unidad generado en {output_path}")
