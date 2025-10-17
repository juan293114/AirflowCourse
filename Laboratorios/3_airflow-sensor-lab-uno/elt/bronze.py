import json
from pathlib import Path

import pandas as pd


def copy_raw_to_bronze(
    raw_root: Path | str,
    bronze_path: Path | str,
) -> str:
    raw_root = Path(raw_root)
    bronze_path = Path(bronze_path)
    bronze_path.parent.mkdir(parents=True, exist_ok=True)

    candidate_files = sorted(raw_root.glob("**/tvmaze.json"))

    records: list[dict] = []
    for json_file in candidate_files:
        payload = json.loads(json_file.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            records.extend(payload)
        else:
            records.append(payload)

    df = pd.json_normalize(records) if records else pd.DataFrame()
    df.to_parquet(bronze_path, index=False)
    return str(bronze_path)
