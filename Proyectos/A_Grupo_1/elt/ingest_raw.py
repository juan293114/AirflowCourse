# Contenido para: elt/ingest_raw.py
import json
import logging
from pathlib import Path
from typing import List, Sequence
import pendulum
import requests

# URL de la API de OpenSky para estados en vivo (LIVE)
API_URL = "https://opensky-network.org/api/states/all"

logger = logging.getLogger(__name__)


def ingest_to_raw(
    output_dir: Path | str,
    timeout: int,
) -> Sequence[str]:
    """
    Ingesta una "foto" en vivo del estado de todos los vuelos desde OpenSky.

    Args:
        output_dir: Directorio raíz donde se guardarán los datos.
        timeout: Timeout para la petición HTTP.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    saved_files: List[str] = []

    # Obtenemos la fecha y hora actual en UTC para la ingesta
    now_utc = pendulum.now("UTC")

    try:
        response = requests.get(API_URL, timeout=timeout)
        response.raise_for_status()
        
        # Manejar respuesta vacía
        if not response.content or not response.json():
             logger.warning("No data returned from OpenSky (live states).")
             return []
             
    except requests.RequestException as exc:
        logger.warning(
            "OpenSky API unavailable (live states): %s", exc
        )
        return [] # Retorna lista vacía si falla la ingesta

    # Crear la estructura de carpetas YYYY/MM/DD
    daily_output_dir = (
        output_dir / f"{now_utc.year:04d}" / f"{now_utc.month:02d}" / f"{now_utc.day:02d}"
    )
    daily_output_dir.mkdir(parents=True, exist_ok=True)

    # Guardar el archivo JSON con un nombre único
    file_name = f"states_{now_utc.format('HHmmss')}.json"
    file_path = daily_output_dir / file_name
    
    file_path.write_text(json.dumps(response.json(), indent=2), encoding="utf-8")
    saved_files.append(str(file_path))

    return saved_files