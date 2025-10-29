from pathlib import Path
import pandas as pd
import ast

def build_dim_currency(bronze_path: Path | str, output_path: Path | str) -> str:
    bronze_path = Path(bronze_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(bronze_path)
    all_curr = set()

    if "currencies" in df.columns:
        for item in df["currencies"].dropna():
            if isinstance(item, str):
                try:
                    item = ast.literal_eval(item)
                except Exception:
                    all_curr.add(item)
                    continue
            if isinstance(item, dict):
                all_curr.update(item.keys())
            elif isinstance(item, list):
                all_curr.update(item)
            elif isinstance(item, str):
                all_curr.add(item)

    if not all_curr:
        print("⚠️ No se encontraron monedas.")
        return "Archivo vacío."

    dim = pd.DataFrame(
        [{"currency_id": i + 1, "currency_codigo": c} for i, c in enumerate(sorted(all_curr))]
    )

    dim.to_parquet(output_path, index=False)
    print(f"✅ Archivo generado: {output_path}")
    return str(output_path)
