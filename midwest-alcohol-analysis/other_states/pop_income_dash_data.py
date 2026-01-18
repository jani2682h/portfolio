import pandas as pd
from pathlib import Path
import logging
import re
from typing import List
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# Read SQINC1 CSV
def _read_sqinc1(file_path: Path) -> pd.DataFrame:
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()
    header_idx = None
    for i, line in enumerate(lines):
        if ("GeoFips" in line) and ("Description" in line) and re.search(r"\d{4}:Q[1-4]", line):
            header_idx = i
            break
    if header_idx is None:
        raise ValueError(f"Header not found in {file_path}")
    return pd.read_csv(file_path, dtype=str, sep=None, engine="python", skiprows=header_idx, header=0)


# Detect description column
def _detect_description_col(df: pd.DataFrame) -> str:
    for c in df.columns:
        if "description" in c.lower():
            return c
    raise ValueError("Description column not found")


# Extract personal income
def _extract_income(df_raw: pd.DataFrame) -> pd.DataFrame:
    desc_col = _detect_description_col(df_raw)
    mask = df_raw[desc_col].str.contains("personal income", case=False, na=False)
    df_inc = df_raw[mask].copy()
    df_inc["Description"] = df_inc[desc_col]

    quarter_cols = [c for c in df_inc.columns if re.match(r"\d{4}:Q[1-4]", c)]
    df_long = df_inc.melt(id_vars=[desc_col], value_vars=quarter_cols,
                          var_name="year_quarter", value_name="Personal_Income")
    df_long[["Year", "Quarter"]] = df_long["year_quarter"].str.split(":", expand=True)
    df_long["Year"] = pd.to_numeric(df_long["Year"], errors="coerce").astype("Int64")
    df_long["Quarter"] = pd.to_numeric(df_long["Quarter"].str.replace("Q", ""), errors="coerce").astype("Int64")
    df_long["Personal_Income"] = (
        df_long["Personal_Income"].astype(str)
        .str.replace(r"[,\s]", "", regex=True)
        .str.replace(r"\(NA\)", "", regex=True)
        .replace({"": pd.NA})
    )
    df_long["Personal_Income"] = pd.to_numeric(df_long["Personal_Income"], errors="coerce") * 1_000_000
    df_long["Personal_Income"] = df_long["Personal_Income"].astype("Float64")
    return df_long[["Year", "Quarter", "Description", "Personal_Income"]]


# Extract population
def _extract_population(df_raw: pd.DataFrame) -> pd.DataFrame:
    desc_col = _detect_description_col(df_raw)
    mask = df_raw[desc_col].str.contains("population", case=False, na=False)
    df_pop = df_raw[mask].copy()
    df_pop["Description"] = df_pop[desc_col]

    quarter_cols = [c for c in df_pop.columns if re.match(r"\d{4}:Q[1-4]", c)]
    df_long = df_pop.melt(id_vars=[desc_col], value_vars=quarter_cols,
                          var_name="year_quarter", value_name="Population")
    df_long[["Year", "Quarter"]] = df_long["year_quarter"].str.split(":", expand=True)
    df_long["Year"] = pd.to_numeric(df_long["Year"], errors="coerce").astype("Int64")
    df_long["Quarter"] = pd.to_numeric(df_long["Quarter"].str.replace("Q", ""), errors="coerce").astype("Int64")
    df_long["Population"] = (
        df_long["Population"].astype(str)
        .str.replace(r"[,\s]", "", regex=True)
        .str.replace(r"\(NA\)", "", regex=True)
        .replace({"": pd.NA})
    )
    df_long["Population"] = pd.to_numeric(df_long["Population"], errors="coerce").astype("Int64")
    return df_long[["Year", "Quarter", "Description", "Population"]]


# Merge income and population per quarter
def _combine_income_population(df_raw: pd.DataFrame) -> pd.DataFrame:
    inc = _extract_income(df_raw)
    pop = _extract_population(df_raw)

    inc = inc.groupby(["Year", "Quarter"], as_index=False).agg({
        "Personal_Income": "mean",
        "Description": "first"
    })
    pop = pop.groupby(["Year", "Quarter"], as_index=False).agg({
        "Population": "mean",
        "Description": "first"
    })

    merged = pd.merge(inc, pop, on=["Year", "Quarter"], how="outer")
    return merged.sort_values(["Year", "Quarter"]).reset_index(drop=True)


# Expand quarterly data to monthly
def _expand_quarter_to_month(df_quarter: pd.DataFrame) -> pd.DataFrame:
    q2m = {1: [1,2,3], 2: [4,5,6], 3: [7,8,9], 4: [10,11,12]}
    df_month = df_quarter.assign(Month=lambda d: d["Quarter"].map(q2m)).explode("Month").astype({"Month":"Int64"})
    df_month["Per_Capita_Personal_Income"] = df_month["Personal_Income"] / df_month["Population"]
    df_month["Per_Capita_Personal_Income"] = df_month["Per_Capita_Personal_Income"].astype("Float64")
    return df_month[["Year","Month","Population","Personal_Income","Per_Capita_Personal_Income"]]


# Main preparation function
def prepare_population(pop_folder: str, output_path: str) -> pd.DataFrame:
    folder = Path(pop_folder).expanduser().resolve()
    files = sorted(folder.glob("raw_*_population.*"))
    all_states: List[pd.DataFrame] = []

    for fp in tqdm(files, desc="Processing population files"):
        state_name = fp.stem.split("_")[1].capitalize()
        try:
            raw_df = _read_sqinc1(fp)
        except Exception:
            raw_df = pd.read_csv(fp, dtype=str, sep=None, engine="python")

        combined_q = _combine_income_population(raw_df)
        combined_m = _expand_quarter_to_month(combined_q)
        combined_m["State_Name"] = state_name
        all_states.append(combined_m)

    result = pd.concat(all_states, ignore_index=True)
    result = result[["State_Name","Year","Month","Population","Personal_Income","Per_Capita_Personal_Income"]]
    result.to_csv(output_path, index=False, na_rep="null")
    logging.info(f"✔ CSV written to {output_path}")
    return result


# Simple callable entry
def run_pop_income_dashboard_data(pop_folder: str, output_path: str):
    prepare_population(pop_folder=pop_folder, output_path=output_path)


# Standalone test
if __name__ == "__main__":
    import argparse

#    parser = argparse.ArgumentParser(description="Clean SQINC1 population & income data")
#    parser.add_argument("--input_folder", type=str, required=True)
#    parser.add_argument("--output_csv", type=str, required=True)
#    args = parser.parse_args()

    input_folder = "../input/population"
    output_csv = "../output/pop_income.csv"

    run_pop_income_dashboard_data(pop_folder=input_folder, output_path=output_csv)
    print(f"\n CSV generated: {output_csv}")






