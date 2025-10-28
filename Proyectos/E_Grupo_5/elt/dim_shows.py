from pathlib import Path

def build_dim_shows(
    silver_path: Path | str,
    output_path: Path | str,
) -> str:
    """
    Construye la dimensión 'shows' adaptada al dataset MEF.
    """
    import pandas as pd

    silver_path = Path(silver_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(silver_path)

    # Columna principal para la dimensión (equivalente a show_id)
    main_id = "entidad_id"
    main_name = "entidad_nombre"

    if main_id not in df.columns:
        dim = pd.DataFrame(columns=[main_id, main_name])
    else:
        columns = [main_id, main_name, "tipo_entidad", "estado_entidad", "fecha_creacion"]
        available = [col for col in columns if col in df.columns]
        dim = df[available].drop_duplicates(subset=[main_id]).reindex(columns=columns, fill_value=None)

    dim.sort_values(main_name, inplace=True, ignore_index=True)
    dim.to_parquet(output_path, index=False)
    return str(output_path)
