import os
import re
from pathlib import Path
import pdfplumber
import pandas as pd
import numpy as np
from tqdm import tqdm

BARREL_TO_LITERS = 31 * 3.78541
BOTTLE_TO_LITERS = 0.75

month_map = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12,
}

def run_minnesota_dashboard_job(pdf_folder: str, csv_out: str) -> pd.DataFrame:
    """Parse Minnesota PDFs, summarize revenue and volume, save CSV with nullable numeric types and 'null' for missing."""
    pdf_folder = Path(pdf_folder)
    pdf_files = sorted([f for f in pdf_folder.glob("*.pdf")])

    all_rows = []

    for pdf_file in tqdm(pdf_files, desc="Processing PDFs"):
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text or "Period Summary" not in text:
                    continue

                pages_split = text.split('--- Page')
                for page_text in pages_split:
                    m_date = re.search(
                        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})",
                        page_text,
                    )
                    if not m_date:
                        continue
                    month_str, year = m_date.groups()
                    month = month_map[month_str]

                    # Sum revenues
                    net_balances = re.findall(r"Net Balance\s*[\d\.,-]*\s*\$([\d,\.]+)", page_text)
                    total_revenue = sum(float(v.replace(",", "")) for v in net_balances) if net_balances else np.nan

                    # Sum volumes
                    volume_liters = 0.0
                    found_volume = False
                    for line in page_text.splitlines():
                        line = line.strip()
                        if not line or not re.search(r"\d", line):
                            continue
                        m_liters = re.search(r"([\d,]+\.\d+)\s*Liters", line)
                        m_barrel = re.search(r"([\d,]+\.\d+)\s*Barrels\*", line)
                        m_bottle = re.search(r"Bottles\s*([\d,]+\.\d+)", line)

                        if m_liters:
                            volume_liters += float(m_liters.group(1).replace(",", ""))
                            found_volume = True
                        elif m_barrel:
                            volume_liters += float(m_barrel.group(1).replace(",", "")) * BARREL_TO_LITERS
                            found_volume = True
                        elif m_bottle:
                            volume_liters += float(m_bottle.group(1).replace(",", "")) * BOTTLE_TO_LITERS
                            found_volume = True

                    if not found_volume:
                        volume_liters = np.nan

                    all_rows.append({
                        "State_Name": "Minnesota",
                        "Year": int(year),
                        "Month": int(month),
                        "Total_Revenue": total_revenue,
                        "Total_Volume_Liters": round(volume_liters, 2) if not np.isnan(volume_liters) else np.nan,
                    })

    # Create DataFrame and enforce nullable types
    df = pd.DataFrame(all_rows)
    df["Year"] = df["Year"].astype("Int64")
    df["Month"] = df["Month"].astype("Int64")
    df["Total_Revenue"] = df["Total_Revenue"].astype("Float64")
    df["Total_Volume_Liters"] = df["Total_Volume_Liters"].astype("Float64")

    # Save CSV with 'null' for missing
    out_path = Path(csv_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, na_rep="null")
    print(f"✔ Minnesota dashboard saved to {out_path}")

    return df

def run_minnesota_dashboard_main(input_folder: str, output_csv: str) -> None:
    run_minnesota_dashboard_job(pdf_folder=input_folder, csv_out=output_csv)

if __name__ == "__main__":
    input_folder = "../input/minnesota_pdfs"
    output_csv = "../output/dashboard/minnesota_2023_2025_summary.csv"
    run_minnesota_dashboard_main(input_folder, output_csv)




