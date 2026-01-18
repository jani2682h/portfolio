from pathlib import Path
import pandas as pd

# Convert Fahrenheit to Celsius
def f_to_c(series: pd.Series) -> pd.Series:
    return ((series - 32) * 5.0 / 9.0).round(1)

def read_csv_safe(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"Unable to locate {path}")
    print(f"✔ Successfully loaded {path.name}")
    return pd.read_csv(path)

# Clean Iowa weather data
def clean_iowa(file_path: str) -> pd.DataFrame:
    df = read_csv_safe(Path(file_path))
    df["day"] = pd.to_datetime(df["day"], errors="coerce")
    df["year"] = df["day"].dt.year
    df["month"] = df["day"].dt.month
    df["max_temp_c"] = f_to_c(df["max_temp_f"])
    df["min_temp_c"] = f_to_c(df["min_temp_f"])

    grouped = df.groupby(["year", "month"]).agg(
        {"max_temp_c": "mean", "min_temp_c": "mean", "snow_in": "mean"}
    ).reset_index()
    grouped["avg_temp"] = (grouped["max_temp_c"] + grouped["min_temp_c"]) / 2.0
    grouped = grouped.rename(columns={
        "max_temp_c": "max_temp",
        "min_temp_c": "min_temp",
        "snow_in": "snow"
    })
    grouped.insert(0, "State_Name", "Iowa")

    # Use nullable Float64 for numeric columns
    for col in ["max_temp", "min_temp", "avg_temp", "snow"]:
        grouped[col] = grouped[col].astype("Float64")
    grouped["year"] = grouped["year"].astype("Int64")
    grouped["month"] = grouped["month"].astype("Int64")

    return grouped

# Clean Wisconsin and Minnesota weather data
def clean_wi_mn(file_path: str, state_name: str) -> pd.DataFrame:
    df = read_csv_safe(Path(file_path))
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    df["year"] = df["DATE"].dt.year
    df["month"] = df["DATE"].dt.month
    df["TAVG_C"] = f_to_c(df["TAVG"])
    df["TMAX_C"] = f_to_c(df["TMAX"])
    df["TMIN_C"] = f_to_c(df["TMIN"])

    grouped = df.groupby(["year", "month"]).agg(
        {"TAVG_C": "mean", "TMAX_C": "mean", "TMIN_C": "mean", "SNOW": "mean"}
    ).reset_index()
    grouped = grouped.rename(columns={
        "TAVG_C": "avg_temp",
        "TMAX_C": "max_temp",
        "TMIN_C": "min_temp",
        "SNOW": "snow"
    })
    grouped.insert(0, "State_Name", state_name)

    for col in ["max_temp", "min_temp", "avg_temp", "snow"]:
        grouped[col] = grouped[col].astype("Float64")
    grouped["year"] = grouped["year"].astype("Int64")
    grouped["month"] = grouped["month"].astype("Int64")

    return grouped

# Merge all states
def merge_states(iowa_df: pd.DataFrame,
                 wisconsin_df: pd.DataFrame,
                 minnesota_df: pd.DataFrame) -> pd.DataFrame:
    merged = pd.concat([iowa_df, wisconsin_df, minnesota_df], ignore_index=True)
    merged = merged.sort_values(by=["State_Name", "year", "month"]).reset_index(drop=True)
    return merged

# Main function to process weather data
def run_weather_dashboard_data(input_dir: str, output_file: str) -> pd.DataFrame:
    iowa_file = Path(input_dir) / "raw_IOWA_weather.csv"
    wisconsin_file = Path(input_dir) / "wisconsin" / "raw_wisconsin_weather.csv"
    minnesota_file = Path(input_dir) / "minnesota" / "raw_minnesota_weather.csv"

    iowa_clean = clean_iowa(str(iowa_file))
    wisconsin_clean = clean_wi_mn(str(wisconsin_file), "Wisconsin")
    minnesota_clean = clean_wi_mn(str(minnesota_file), "Minnesota")

    merged_df = merge_states(iowa_clean, wisconsin_clean, minnesota_clean)

    merged_df = merged_df.rename(columns={
        "year": "Year",
        "month": "Month",
        "max_temp": "Max_Temp",
        "min_temp": "Min_Temp",
        "snow": "Snow",
        "avg_temp": "Avg_Temp"
    })

    merged_df = merged_df[["State_Name", "Year", "Month", "Max_Temp", "Min_Temp", "Snow", "Avg_Temp"]]

    # Save CSV with 'null' for missing values
    out_path = Path(output_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged_df.to_csv(out_path, index=False, na_rep="null")
    print(f" Weather dashboard saved to {out_path}")

    return merged_df






