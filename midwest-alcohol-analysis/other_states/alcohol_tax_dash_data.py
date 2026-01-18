from pathlib import Path
from typing import Union
import pandas as pd
import logging

GALLON_TO_LITER = 3.78541
log = logging.getLogger(__name__)

def _to_path(p: Union[str, Path]) -> Path:
    """Convert a string or Path to a Path object."""
    return p if isinstance(p, Path) else Path(p)

def read_excel_flex(xlsx_path: Path) -> pd.DataFrame:
    """Read an Excel file into a DataFrame."""
    if not xlsx_path.is_file():
        raise FileNotFoundError(f"Unable to locate {xlsx_path}")
    return pd.read_excel(xlsx_path, dtype=str, engine="openpyxl")

def clean_alcohol_tax(xlsx_path: Path, convert_to_liter: bool = True) -> pd.DataFrame:
    """Clean a single alcohol tax sheet, convert rates to liters if needed, fill missing values as NaN."""
    df = read_excel_flex(xlsx_path)

    # Remove $ and , then convert to float; missing values become NaN
    df["Rate"] = pd.to_numeric(df["Rate"].str.replace(r"[$,]", "", regex=True), errors="coerce")

    if convert_to_liter:
        df["Rate_Per_Liter"] = df["Rate"] / GALLON_TO_LITER

    df["State_code"] = df["States"]
    df["State_Name"] = df["State"]

    # Ensure missing values exist as NaN
    for col in ["Rate", "Rate_Per_Liter"]:
        if col not in df.columns:
            df[col] = pd.NA

    return df[["State_code", "State_Name", "Rate", "Rate_Per_Liter"]]

def merge_alcohol_tax(beer_df: pd.DataFrame, wine_df: pd.DataFrame, liquor_df: pd.DataFrame) -> pd.DataFrame:
    """Merge beer, wine, and liquor dataframes into one. Missing values remain as NaN."""
    merged = beer_df.merge(wine_df, on="State_code", suffixes=("_Beer", "_Wine"))
    merged = merged.merge(liquor_df, on="State_code")
    merged = merged.rename(columns={"Rate": "Rate_Liquor", "Rate_Per_Liter": "Rate_Per_Liter_Liquor"})

    # Ensure all expected columns exist
    for col in ["Rate_Beer", "Rate_Per_Liter_Beer", "Rate_Wine", "Rate_Per_Liter_Wine",
                "Rate_Liquor", "Rate_Per_Liter_Liquor"]:
        if col not in merged.columns:
            merged[col] = pd.NA

    cols_order = [
        "State_code", "State_Name",
        "Rate_Beer", "Rate_Per_Liter_Beer",
        "Rate_Wine", "Rate_Per_Liter_Wine",
        "Rate_Liquor", "Rate_Per_Liter_Liquor",
    ]
    return merged[cols_order]

def run_alcohol_tax_dashboard(input_dir: Union[str, Path], output_path: Union[str, Path]) -> pd.DataFrame:
    """Read, clean, merge alcohol tax data, fill missing values as null, and save CSV."""
    input_dir = _to_path(input_dir)
    output_path = _to_path(output_path)

    # Clean each alcohol type
    beer_df = clean_alcohol_tax(input_dir / "beer.xlsx")
    wine_df = clean_alcohol_tax(input_dir / "wine.xlsx")
    liquor_df = clean_alcohol_tax(input_dir / "liquor.xlsx")

    # Merge dataframes
    merged_df = merge_alcohol_tax(beer_df, wine_df, liquor_df)

    # Convert numeric columns to nullable Float64 type
    merged_df["Rate_Beer"] = merged_df["Rate_Beer"].astype("Float64")
    merged_df["Rate_Per_Liter_Beer"] = merged_df["Rate_Per_Liter_Beer"].astype("Float64")
    merged_df["Rate_Wine"] = merged_df["Rate_Wine"].astype("Float64")
    merged_df["Rate_Per_Liter_Wine"] = merged_df["Rate_Per_Liter_Wine"].astype("Float64")
    merged_df["Rate_Liquor"] = merged_df["Rate_Liquor"].astype("Float64")
    merged_df["Rate_Per_Liter_Liquor"] = merged_df["Rate_Per_Liter_Liquor"].astype("Float64")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Save CSV with null for missing values
    merged_df.to_csv(output_path, index=False, na_rep="null")
    log.info("Alcohol tax summary saved to %s", output_path)

    return merged_df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    run_alcohol_tax_dashboard(
        input_dir="../input/alcohol_tax",
        output_path="../output/dashboard/alcohol_tax_summary.csv"
    )








