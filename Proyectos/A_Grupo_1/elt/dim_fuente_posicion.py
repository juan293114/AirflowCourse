from pathlib import Path
import pandas as pd

def build_dim_fuente_posicion(
    silver_path: Path | str,
    output_path: Path | str,
) -> str:
    """
    Construye la dim_fuente_posicion desde la capa Silver.
    Extrae las categorías únicas de 'fuente_posicion' y añade
    una descripción para cada una.
    """
    silver_path = Path(silver_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Columnas finales de la dimensión
    DIM_COLUMNS = ["id_fuente", "fuente_posicion", "descripcion"]

    try:
        df = pd.read_parquet(silver_path)
    except Exception:
        # Si el archivo Silver no existe o está vacío, crea una dimensión vacía
        dim = pd.DataFrame(columns=DIM_COLUMNS)
        dim.to_parquet(output_path, index=False)
        return str(output_path)

    # Validamos que la columna necesaria exista
    if "fuente_posicion" not in df.columns:
        dim = pd.DataFrame(columns=DIM_COLUMNS)
        dim.to_parquet(output_path, index=False)
        return str(output_path)

    records: list[dict] = []

    # Definimos el "traductor" de descripciones
    desc_map = {
        "ADS-B": "Reporte directo automático del avión (GPS).",
        "MLAT": "Triangulación por antenas en tierra.",
        "ASTERIX": "Formato de radar de control aéreo.",
        "FLARM": "Sistema anticolisión para aeronaves ligeras.",
        "Desconocido": "Fuente de datos no identificada."
    }

    # Obtenemos las categorías únicas de la columna
    subset = df.loc[df["fuente_posicion"].notna(), ["fuente_posicion"]].drop_duplicates()

    # Iteramos sobre las categorías y las enriquecemos
    for row in subset.to_dict(orient="records"):
        fuente = row.get("fuente_posicion")
        records.append(
            {
                "fuente_posicion": fuente,
                # Usamos el .get() para asignar la descripción o "N/A" si no se encuentra
                "descripcion": desc_map.get(fuente, "N/A"), 
            }
        )

    # Creamos la dimensión final desde los registros
    dim = pd.DataFrame(records, columns=["fuente_posicion", "descripcion"])

    # Hacemos una limpieza final
    dim.drop_duplicates(subset=["fuente_posicion"], inplace=True)
    dim.sort_values("fuente_posicion", inplace=True, ignore_index=True)

    # Añadimos la clave subrogada (id_fuente)
    dim.reset_index(inplace=True)
    dim.rename(columns={"index": "id_fuente"}, inplace=True)
    
    # Reordenamos las columnas y guardamos
    dim = dim[DIM_COLUMNS]
    dim.to_parquet(output_path, index=False)
    
    return str(output_path)