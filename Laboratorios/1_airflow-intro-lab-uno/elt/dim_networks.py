from pathlib import Path



def build_dim_networks(
    silver_path: Path | str,
    output_path: Path | str,
) -> str:
    import pandas as pd

    silver_path = Path(silver_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(silver_path)

    records: list[dict] = []

    if "show_network_id" in df.columns:
        cols = ["show_network_id", "show_network_name", "show_network_country_code", "show_network_country_name"]
        available = [col for col in cols if col in df.columns]
        subset = df.loc[df["show_network_id"].notna(), available].drop_duplicates()
        for row in subset.to_dict(orient="records"):
            records.append(
                {
                    "network_id": row.get("show_network_id"),
                    "network_name": row.get("show_network_name"),
                    "country_code": row.get("show_network_country_code"),
                    "country_name": row.get("show_network_country_name"),
                    "network_type": "broadcast",
                }
            )

    if "show_webchannel_id" in df.columns:
        cols = ["show_webchannel_id", "show_webchannel_name", "show_webchannel_country_code", "show_webchannel_country_name"]
        available = [col for col in cols if col in df.columns]
        subset = df.loc[df["show_webchannel_id"].notna(), available].drop_duplicates()
        for row in subset.to_dict(orient="records"):
            records.append(
                {
                    "network_id": row.get("show_webchannel_id"),
                    "network_name": row.get("show_webchannel_name"),
                    "country_code": row.get("show_webchannel_country_code"),
                    "country_name": row.get("show_webchannel_country_name"),
                    "network_type": "web",
                }
            )

    dim = pd.DataFrame(records, columns=["network_id", "network_name", "country_code", "country_name", "network_type"])
    dim.drop_duplicates(subset=["network_id"], inplace=True)
    dim.sort_values("network_name", inplace=True, ignore_index=True)
    dim.to_parquet(output_path, index=False)
    return str(output_path)
