# extraccion_restcountries_v3_csv.py
import requests
import pandas as pd
from pathlib import Path


BASE_URL = "https://restcountries.com/v3.1"


FIELDS = [
    "name",
    "capital",
    "region",
    "subregion",
    "population",
    "area",
    "languages",
    "currencies",
    "flags",
    "timezones"
]

FIELDS_QUERY = ",".join(FIELDS)

# Carpeta de salida
OUTPUT_DIR = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_PATH = OUTPUT_DIR / "paises.csv"

# ================= FUNCIONES =================
def obtener_todos_los_paises():
    """Obtiene todos los países con los campos especificados."""
    url = f"{BASE_URL}/all?fields={FIELDS_QUERY}"
    print(f"🌍 Iniciando extracción desde: {url}")
    r = requests.get(url)

    if r.status_code != 200:
        print(f"⚠️ Error al conectar con la API: {r.status_code}")
        print(r.text)
        return None

    return r.json()

def generar_csv(datos):
    """Transforma los datos JSON en un CSV plano."""
    registros = []
    for pais in datos:
        name = pais.get("name", {}).get("common", "Desconocido")
        capital = ", ".join(pais.get("capital", [])) if pais.get("capital") else None
        region = pais.get("region")
        subregion = pais.get("subregion")
        population = pais.get("population")
        area = pais.get("area")
        languages = ", ".join(pais.get("languages", {}).values()) if pais.get("languages") else None
        currencies = ", ".join(pais.get("currencies", {}).keys()) if pais.get("currencies") else None
        flag = pais.get("flags", {}).get("png")
        timezones = ", ".join(pais.get("timezones", []))

        registros.append({
            "name": name,
            "capital": capital,
            "region": region,
            "subregion": subregion,
            "population": population,
            "area": area,
            "languages": languages,
            "currencies": currencies,
            "flag_url": flag,
            "timezones": timezones
        })

    df = pd.DataFrame(registros)
    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
    print(f"✅ CSV generado correctamente en: {OUTPUT_PATH.absolute()}")

# ================= MAIN =================
if __name__ == "__main__":
    print("🌍 Iniciando extracción de datos de países...")

    data = obtener_todos_los_paises()
    if data:
        generar_csv(data)
        print("🎉 Extracción completada correctamente.")
    else:
        print("❌ No se pudieron obtener los datos.")
