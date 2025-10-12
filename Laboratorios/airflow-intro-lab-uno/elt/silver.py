from pathlib import Path


def _clean_column_name(name: str) -> str:
    return (
        name.strip()
        .lower()
        .replace(" ", "_")
        .replace(".", "_")
        .replace("-", "_")
    )


def transform_bronze_to_silver(
    bronze_path: Path | str,
    silver_path: Path | str,
) -> str:
    import pandas as pd
    import numpy as np

    bronze_path = Path(bronze_path)
    silver_path = Path(silver_path)
    silver_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(bronze_path)

    df.columns = [_clean_column_name(col) for col in df.columns]

    rename_map = {
        "id": "episode_id",
        "name": "episode_name",
        "number": "episode_number",
        "type": "episode_type",
        "runtime": "episode_runtime",
        "summary": "episode_summary",
        "rating_average": "episode_rating",
        "_embedded_show_id": "show_id",
        "_embedded_show_name": "show_name",
        "_embedded_show_type": "show_type",
        "_embedded_show_language": "show_language",
        "_embedded_show_status": "show_status",
        "_embedded_show_genres": "show_genres",
        "_embedded_show_premiered": "show_premiered",
        "_embedded_show_rating_average": "show_rating_average",
        "_embedded_show_webchannel_id": "show_webchannel_id",
        "_embedded_show_webchannel_name": "show_webchannel_name",
        "_embedded_show_webchannel_country_name": "show_webchannel_country_name",
        "_embedded_show_webchannel_country_code": "show_webchannel_country_code",
        "_embedded_show_network_id": "show_network_id",
        "_embedded_show_network_name": "show_network_name",
        "_embedded_show_network_country_name": "show_network_country_name",
        "_embedded_show_network_country_code": "show_network_country_code",
        "_embedded_show_schedule_days": "show_schedule_days",
        "_embedded_show_schedule_time": "show_schedule_time",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    for col in df.columns:
        def normalize(value):
            if isinstance(value, list):
                return tuple(value)
            if isinstance(value, np.ndarray):
                return tuple(value.tolist())
            if isinstance(value, dict):
                return tuple(sorted(value.items()))
            return value
        df[col] = df[col].apply(normalize)

    df = df.drop_duplicates()

    if "show_genres" in df.columns:
        df["show_genres"] = df["show_genres"].apply(
            lambda value: ",".join(str(item) for item in value) if isinstance(value, (list, tuple)) else value
        )

    if "show_schedule_days" in df.columns:
        df["show_schedule_days"] = df["show_schedule_days"].apply(
            lambda value: ",".join(str(item) for item in value) if isinstance(value, (list, tuple)) else value
        )

    df.to_parquet(silver_path, index=False)
    return str(silver_path)
