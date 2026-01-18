from pathlib import Path
import pandas as pd


GALLON_TO_LITER = 3.78541

# Convert fiscal year to calendar year
def transform_fy_to_calendar(df: pd.DataFrame) -> pd.DataFrame:
    df = df[~df["Month"].astype(str).str.lower().isin(["total", "grand total", ""])].copy()

    # Fill missing FY values and extract year
    df["FY_Year"] = df["Year"].ffill()
    df["FY_Year"] = df["FY_Year"].astype(str).str.extract(r'(\d{4})')[0].astype("Int64")

    # Keep original month name
    df["Month_Name"] = df["Month"]

    # Map month names to numbers
    month_map = {
        "January": 1, "February": 2, "March": 3, "April": 4,
        "May": 5, "June": 6, "July": 7, "August": 8,
        "September": 9, "October": 10, "November": 11, "December": 12
    }
    df["Month"] = df["Month_Name"].map(month_map).astype("Int64")

    # Compute calendar year: Jan–Jun → FY_Year, Jul–Dec → FY_Year - 1
    df["Year"] = df.apply(lambda r: r["FY_Year"] if r["Month"] <= 6 else r["FY_Year"] - 1, axis=1).astype("Int64")

    # Convert all numeric columns to nullable float
    for col in df.select_dtypes(include=[float, int]).columns:
        df[col] = df[col].astype("Float64")
    return df

# Summarize Wisconsin revenue & volume
def summarize_wisconsin(input_folder: str, output_csv: str) -> None:
    input_path = Path(input_folder)
    revenue_file = input_path / "Table_Revenues.xlsx"
    gallons_file = input_path / "Table_Taxable_Gallons.xlsx"

    df_rev = pd.read_excel(revenue_file, header=1)
    df_gal = pd.read_excel(gallons_file, header=1)

    df_rev = transform_fy_to_calendar(df_rev)
    df_gal = transform_fy_to_calendar(df_gal)

    alcohol_cols = ["Cider", "Wine", "Beer", "Liquor"]
    for col in alcohol_cols:
        if col not in df_rev:
            df_rev[col] = pd.NA
        if col not in df_gal:
            df_gal[col] = pd.NA

    # Compute totals and use nullable Float64
    df_rev["Total_Revenue"] = df_rev[alcohol_cols].sum(axis=1, skipna=True).astype("Float64")
    df_gal["Total_Gallons"] = df_gal[alcohol_cols].sum(axis=1, skipna=True).astype("Float64")
    df_gal["Total_Volume_Liters"] = (df_gal["Total_Gallons"] * GALLON_TO_LITER).astype("Float64")

    merged = pd.merge(
        df_rev[["FY_Year", "Year", "Month_Name", "Month", "Total_Revenue"]],
        df_gal[["FY_Year", "Year", "Month_Name", "Month", "Total_Gallons", "Total_Volume_Liters"]],
        on=["FY_Year", "Year", "Month_Name", "Month"],
        how="inner"
    )

    merged["State_Name"] = "Wisconsin"

    summary = merged.sort_values(["Year", "Month"])
    summary = summary[
        ["State_Name","Year", "FY_Year", "Month_Name", "Month",
         "Total_Revenue", "Total_Volume_Liters", "Total_Gallons"]
    ]

    # Ensure numeric columns are nullable
    for col in ["Total_Revenue", "Total_Volume_Liters", "Total_Gallons"]:
        summary[col] = summary[col].astype("Float64")

    # Save CSV with 'null' for missing values
    out_path = Path(output_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(out_path, index=False, na_rep="null")
    print(f"✔ Wisconsin summary written to: {out_path}")

def run_wisconsin_dashboard_job(input_folder: str, output_csv: str) -> None:
    summarize_wisconsin(input_folder=input_folder, output_csv=output_csv)

if __name__ == "__main__":
    run_wisconsin_dashboard_job(
        input_folder="../input/wisconsin",
        output_csv="../output/dashboard/wisconsin_summary.csv",
    )








