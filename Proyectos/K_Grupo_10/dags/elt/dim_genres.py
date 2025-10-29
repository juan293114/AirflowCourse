from pathlib import Path

def build_dim_genres(
    silver_path: Path | str,
    output_path: Path | str,
) -> str:
    import pandas as pd

    silver_path = Path(silver_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(silver_path)

    genres = set()

    if "show_genres" in df.columns:
        for item in df["show_genres"].dropna():
            if isinstance(item, (list, tuple)):
                genres.update(item)
            elif isinstance(item, str) and "," in item:
                genres.update([g.strip() for g in item.split(",")])
            else:
                genres.add(item)

    # Creamos DataFrame limpio
    dim = pd.DataFrame(
        [{"genre_id": i + 1, "genre_name": genre} for i, genre in enumerate(sorted(genres))]
    )

    dim.to_parquet(output_path, index=False)
    return str(output_path)
