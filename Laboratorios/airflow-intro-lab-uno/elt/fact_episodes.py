from pathlib import Path



def build_fact_episodes(
    silver_path: Path | str,
    output_path: Path | str,
) -> str:
    import pandas as pd

    silver_path = Path(silver_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(silver_path)

    if "episode_id" not in df.columns:
        fact = pd.DataFrame(columns=["episode_id"])
    else:
        fact = df.copy()

        if "airdate" in fact.columns:
            dates = pd.to_datetime(fact["airdate"], errors="coerce")
        elif "airstamp" in fact.columns:
            dates = pd.to_datetime(fact["airstamp"], errors="coerce")
        else:
            dates = pd.Series(pd.NaT, index=fact.index)

        fact["date_key"] = dates.dt.strftime("%Y%m%d").astype("Int64")

        fact["network_id"] = fact.get("show_network_id")
        if "show_webchannel_id" in fact.columns:
            fact.loc[fact["network_id"].isna(), "network_id"] = fact.get("show_webchannel_id")

        selected_columns = [col for col in [
            "episode_id",
            "show_id",
            "episode_name",
            "season",
            "episode_number",
            "episode_runtime",
            "episode_rating",
            "date_key",
            "network_id",
        ] if col in fact.columns]

        fact = fact[selected_columns].drop_duplicates(subset=["episode_id"])

    fact.to_parquet(output_path, index=False)
    return str(output_path)
