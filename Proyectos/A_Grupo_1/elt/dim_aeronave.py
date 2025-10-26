from pathlib import Path
import pandas as pd

def build_dim_aeronave(
    silver_path: Path | str,
    output_path: Path | str,
) -> str:
    """
    Construye la dim_aeronave desde la capa Silver.
    Extrae aeronaves únicas (codigo_aeronave) y su país de origen.
    """
    silver_path = Path(silver_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        df = pd.read_parquet(silver_path)
    except Exception:
        # Si el archivo Silver no existe o está vacío, crea una dimensión vacía
        dim = pd.DataFrame(columns=["id_aeronave", "codigo_aeronave", "pais_origen"])
        dim.to_parquet(output_path, index=False)
        return str(output_path)

    records: list[dict] = []

    # Definimos las columnas que identifican una aeronave
    cols = ["codigo_aeronave", "pais_origen"]
    available = [col for col in cols if col in df.columns]

    # Nos aseguramos de que la columna clave 'codigo_aeronave' exista
    if "codigo_aeronave" in available:
        # Obtenemos el subconjunto de aeronaves únicas
        subset = df.loc[df["codigo_aeronave"].notna(), available].drop_duplicates()

        # Iteramos sobre las aeronaves únicas y las añadimos a 'records'
        for row in subset.to_dict(orient="records"):
            records.append(
                {
                    "codigo_aeronave": row.get("codigo_aeronave"),
                    "pais_origen": row.get("pais_origen"),
                }
            )

    # Creamos la dimensión final desde los registros
    dim = pd.DataFrame(records, columns=["codigo_aeronave", "pais_origen"])

    # Hacemos una limpieza final
    dim.drop_duplicates(subset=["codigo_aeronave"], inplace=True)
    dim.sort_values("codigo_aeronave", inplace=True, ignore_index=True)

    # Añadimos la clave subrogada (id_aeronave)
    dim.reset_index(inplace=True)
    dim.rename(columns={"index": "id_aeronave"}, inplace=True)
    
    # Reordenamos las columnas para que la clave quede primero
    dim = dim[["id_aeronave", "codigo_aeronave", "pais_origen"]]

    # Guardamos el archivo parquet
    dim.to_parquet(output_path, index=False)
    return str(output_path)