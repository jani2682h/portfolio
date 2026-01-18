import os
import pandas as pd

# Ask user for CSV file path
csv_file = input("Enter path to CSV file: ").strip()

# Ask user for Parquet chunks folder
chunks_dir = input("Enter path to Parquet chunks folder: ").strip()

# Step 1: check total rows in CSV
with open(csv_file) as f:
    total_csv_rows = sum(1 for _ in f) - 1  # subtract header
print(f"Total rows in CSV: {total_csv_rows}")

# Step 2: check total rows in all Parquet chunks
total_parquet_rows = 0
for f in os.listdir(chunks_dir):
    if f.endswith(".parquet"):
        df = pd.read_parquet(os.path.join(chunks_dir, f))
        total_parquet_rows += len(df)
print(f"Total rows in all Parquet chunks: {total_parquet_rows}")

if total_csv_rows == total_parquet_rows:
    print("Row counts match. Conversion looks correct.")
else:
    print("Row counts do NOT match! Please check your CSV and chunks.")

# Step 3: check column names
csv_cols = pd.read_csv(csv_file, nrows=0).columns.tolist()
first_chunk = pd.read_parquet(os.path.join(chunks_dir, os.listdir(chunks_dir)[0]))
parquet_cols = first_chunk.columns.tolist()

if csv_cols == parquet_cols:
    print("Columns match between CSV and Parquet.")
else:
    print("Columns do NOT match!")
    print("CSV columns:", csv_cols)
    print("Parquet columns:", parquet_cols)

# Step 4: check Zip Code column in each chunk
for f in os.listdir(chunks_dir):
    if f.endswith(".parquet"):
        df = pd.read_parquet(os.path.join(chunks_dir, f))

        # check for missing values
        if df['Zip Code'].isnull().any():
            print(f"{f} has missing Zip Code values!")
        else:
            print(f"{f} Zip Code column has no missing values.")

        # check format: 5 digit string, skip missing values
        valid_mask = df['Zip Code'].notnull() & df['Zip Code'].str.match(r'^\d{5}$')
        invalid_zip = df[~valid_mask]

        if not invalid_zip.empty:
            print(f"{f} has {len(invalid_zip)} rows with invalid Zip Code format.")


