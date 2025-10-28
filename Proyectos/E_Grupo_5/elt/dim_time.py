from pathlib import Path

def build_dim_time(
    silver_path: Path | str,
    output_path: Path | str,
    date_column: str = "fecha",
) -> str:
    """
    Construye la dimensión de tiempo para datasets tipo MEF.
    Se puede especificar la columna de fecha relevante con `date_column`.
    """
    import pandas as pd

    silver_path = Path(silver_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(silver_path)

    if date_column in df.columns:
        date_series = pd.to_datetime(df[date_column], errors="coerce")
    else:
        date_series = pd.Series([], dtype="datetime64[ns]")

    date_series = date_series.dropna().drop_duplicates().sort_values()

    dim = pd.DataFrame({"date": date_series})
    dim["date_key"] = dim["date"].dt.strftime("%Y%m%d").astype(int)
    dim["year"] = dim["date"].dt.year
    dim["month"] = dim["date"].dt.month
    dim["day"] = dim["date"].dt.day
    dim["day_name"] = dim["date"].dt.day_name()
    dim["week_of_year"] = dim["date"].dt.isocalendar().week.astype(int)
    dim["month_name"] = dim["date"].dt.month_name()
    dim["quarter"] = dim["date"].dt.quarter
    dim["is_weekend"] = dim["day_name"].isin(["Saturday", "Sunday"])

    dim = dim[["date_key", "date", "year", "quarter", "month", "month_name", "day", "day_name", "week_of_year", "is_weekend"]]
    dim.to_parquet(output_path, index=False)
    return str(output_path)
