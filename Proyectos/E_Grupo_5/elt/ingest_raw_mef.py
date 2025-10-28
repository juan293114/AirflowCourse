import json
import logging
from pathlib import Path
from typing import List, Sequence
import requests

# URL del endpoint correcto del MEF
API_URL = "https://api.datosabiertos.mef.gob.pe/DatosAbiertos/v1/datastore_search"
RESOURCE_ID = "f9cc4ba0-931a-4b70-86c9-eacbd8c68596"
LIMIT = 5

# Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ingest_to_raw(
    output_dir: Path | str,
    timeout: int = 30,
    limit: int = LIMIT
) -> Sequence[str]:
    """
    Descarga datos del MEF (API de Datos Abiertos) y los guarda en JSON por páginas.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    saved_files: List[str] = []

    offset = 0
    page = 1

    while True:
        params = {
            "resource_id": RESOURCE_ID,
            "limit": limit,
            "offset": offset,
        }

        try:
            response = requests.get(API_URL, params=params, timeout=timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("❌ API del MEF no disponible en offset=%s: %s", offset, exc)
            break

        data = response.json()

        # ✅ Compatibilidad con estructura real del MEF
        if "records" in data:
            records = data["records"]
        else:
            records = data.get("result", {}).get("records", [])

        if not records:
            logger.info("✅ No se encontraron más registros (offset=%s).", offset)
            break

        # Guardar página descargada
        file_path = output_dir / f"mef_data_page_{page:03d}.json"
        file_path.write_text(json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8")
        saved_files.append(str(file_path))

        logger.info("📄 Página %s guardada con %s registros.", page, len(records))

        if len(records) < limit:
            logger.info("✅ Descarga completada (última página: %s).", page)
            break

        offset += limit
        page += 1

    return saved_files


if __name__ == "__main__":
    # Carpeta de salida
    output_path = Path("data_raw/mef")

    logger.info("🚀 Iniciando descarga desde la API del MEF...")
    archivos_guardados = ingest_to_raw(output_path)
    logger.info("✅ Archivos guardados: %s", archivos_guardados)

