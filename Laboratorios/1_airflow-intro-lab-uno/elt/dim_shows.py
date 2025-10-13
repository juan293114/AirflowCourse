from pathlib import Path



def build_dim_shows(
    silver_path: Path | str,
    output_path: Path | str,
) -> str:
    import pandas as pd

    silver_path = Path(silver_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(silver_path)

    required_column = "show_id"
    if required_column not in df.columns:
        dim = pd.DataFrame(columns=["show_id", "show_name"])
    else:
        columns = [
            "show_id",
            "show_name",
            "show_type",
            "show_language",
            "show_status",
            "show_genres",
            "show_premiered",
            "show_rating_average",
            "show_webchannel_name",
            "show_network_name",
        ]
        available = [col for col in columns if col in df.columns]
        dim = df[available].drop_duplicates(subset=["show_id"]).reindex(columns=columns, fill_value=None)
        if "show_genres" in dim.columns:
            dim["show_genres"] = dim["show_genres"].apply(
                lambda value: ",".join(str(item) for item in value) if isinstance(value, (list, tuple)) else value
            )

    dim.sort_values("show_name", inplace=True, ignore_index=True)
    dim.to_parquet(output_path, index=False)
    return str(output_path)

