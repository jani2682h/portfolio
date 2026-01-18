import os
import pandas as pd

# Ask user for CSV file path
csv_file = input("Enter path to CSV file: ").strip()

# Ask user for Parquet output folder
output_dir = input("Enter folder to save Parquet chunks: ").strip()
os.makedirs(output_dir, exist_ok=True)

# Chunk size (number of rows per Parquet file)
chunksize = 500_000
i = 0

# Track total invalid Zip Codes
total_invalid_zip = 0

# Calculate total rows in CSV (excluding header)
total_rows = sum(1 for _ in open(csv_file)) - 1

# Read CSV in chunks
for chunk in pd.read_csv(csv_file, chunksize=chunksize, dtype={'Zip Code': str}, low_memory=False):
    # Fill missing Zip Code values with 'NA'
    chunk['Zip Code'] = chunk['Zip Code'].fillna('NA')

    # Check Zip Code format: must be 5-digit number
    invalid_zip = chunk[(chunk['Zip Code'] != 'NA') & (~chunk['Zip Code'].str.match(r'^\d{5}$'))]
    num_invalid = len(invalid_zip)
    total_invalid_zip += num_invalid

    if num_invalid > 0:
        print(f"Chunk {i} has {num_invalid} rows with invalid Zip Code format!")

    # Print progress
    start_row = i * chunksize
    end_row = start_row + len(chunk)
    progress = end_row / total_rows * 100
    print(f"Processing chunk {i}, rows: {len(chunk)}, progress: {progress:.2f}%")

    # Save chunk as Parquet
    chunk.to_parquet(
        os.path.join(output_dir, f"chunk_{i}.parquet"),
        index=False
    )
    print(f"Chunk {i} saved.\n")
    i += 1

# Final summary
print("CSV split into Parquet chunks successfully.")
print(f"Total rows with invalid Zip Code across all chunks: {total_invalid_zip}")




