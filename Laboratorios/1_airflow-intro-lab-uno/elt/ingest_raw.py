import json
import logging
from pathlib import Path
from typing import List, Sequence
import pendulum
import requests

API_URL = "http://api.tvmaze.com/schedule/web"

logger = logging.getLogger(__name__)


def ingest_to_raw(
    start_date: pendulum.Date,
    end_date: pendulum.Date,
    output_dir: Path | str,
    timeout: int,
) -> Sequence[str]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    saved_files: List[str] = []

    current_date = start_date
    while current_date <= end_date:
        params = {"date": current_date.to_date_string()}
        try:
            response = requests.get(API_URL, params=params, timeout=timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("TVMaze API unavailable for %s: %s", current_date.to_date_string(), exc)
            current_date = current_date.add(days=1)
            continue

        daily_output_dir = (
            output_dir / f"{current_date.year:04d}" / f"{current_date.month:02d}" / f"{current_date.day:02d}"
        )
        daily_output_dir.mkdir(parents=True, exist_ok=True)

        file_path = daily_output_dir / "tvmaze.json"
        file_path.write_text(json.dumps(response.json(), indent=2), encoding="utf-8")
        saved_files.append(str(file_path))

        current_date = current_date.add(days=1)

    return saved_files
