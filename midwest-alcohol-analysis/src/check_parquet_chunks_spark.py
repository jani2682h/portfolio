import os
from pyspark.sql import SparkSession
from tabulate import tabulate

spark = SparkSession.builder.appName("CheckParquet").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

parent_dir = input("Enter parent folder path: ").strip()

# Find CSV / CSV.GZ
csv_files = [f for f in os.listdir(parent_dir)
             if f.lower().endswith(".csv") or f.lower().endswith(".csv.gz")]

if not csv_files:
    print("No CSV/CSV.GZ found!")
    exit()

csv_path = os.path.join(parent_dir, csv_files[0])
print(f"Original CSV: {csv_path}")

csv_df = spark.read.csv(csv_path, header=True)
csv_rows = csv_df.count()
print(f"Original CSV rows: {csv_rows:,}")

# Find parquet folder
parquet_dirs = [os.path.join(parent_dir, d) for d in os.listdir(parent_dir)
                if os.path.isdir(os.path.join(parent_dir, d)) and "parquet" in d.lower()]

table_rows = []
for pq in parquet_dirs:
    parquet_name = os.path.basename(pq)
    cleaned_df = spark.read.parquet(pq)
    parquet_rows = cleaned_df.count()
    diff = csv_rows - parquet_rows

    if "ZIPCODE" in cleaned_df.columns:
        missing_zip = cleaned_df.filter(cleaned_df["ZIPCODE"].isNull()).count()
    else:
        missing_zip = "N/A"

    table_rows.append([
        parquet_name,
        f"{parquet_rows:,}",
        f"{diff:,}" if diff != 0 else "0",
        f"{missing_zip:,}" if isinstance(missing_zip, int) else missing_zip
    ])

# Print table
print(tabulate(
    table_rows,
    headers=["Parquet Folder", "Parquet Rows", "Row Diff", "Missing ZIPCODE"],
    tablefmt="github"
))






