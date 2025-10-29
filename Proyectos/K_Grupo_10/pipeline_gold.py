from dim_region import build_dim_region
from dim_country import build_dim_country
from dim_language import build_dim_language
from dim_currency import build_dim_currency
from fact_country import build_fact_country

if __name__ == "__main__":
    
    bronze_path = "data/bronze/paises_bronze.parquet"

    print("🧩 Creando dimensiones y hechos desde capa BRONZE...")
    build_dim_region(bronze_path, "data/gold/dim_region.parquet")
    build_dim_country(bronze_path, "data/gold/dim_country.parquet")
    build_dim_language(bronze_path, "data/gold/dim_language.parquet")
    build_dim_currency(bronze_path, "data/gold/dim_currency.parquet")
    build_fact_country(bronze_path, "data/gold/fact_country.parquet")

    print("✅ ¡Proceso GOLD finalizado correctamente!")
