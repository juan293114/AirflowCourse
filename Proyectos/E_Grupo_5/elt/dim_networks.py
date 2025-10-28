from pathlib import Path

def build_dim_networks(
    silver_path: Path | str,
    output_path: Path | str,
) -> str:
    """
    Construye la dimensión 'networks' o equivalente para el dataset MEF.
    """
    import pandas as pd

    silver_path = Path(silver_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(silver_path)

    records: list[dict] = []

    # Ejemplo genérico: tomar columnas que podrían representar entidades o redes
    if "entidad_id" in df.columns:
        cols = ["entidad_id", "entidad_nombre", "entidad_tipo"]
        available = [col for col in cols if col in df.columns]
        subset = df.loc[df["entidad_id"].notna(), available].drop_duplicates()
        for row in subset.to_dict(orient="records"):
            records.append(
                {
                    "network_id": row.get("entidad_id"),
                    "network_name": row.get("entidad_nombre"),
                    "network_type": row.get("entidad_tipo", "unknown"),
                }
            )

    dim = pd.DataFrame(records, columns=["network_id", "network_name", "network_type"])
    dim.drop_duplicates(subset=["network_id"], inplace=True)
    dim.sort_values("network_name", inplace=True, ignore_index=True)
    dim.to_parquet(output_path, index=False)
    return str(output_path)
