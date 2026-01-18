import os
import pandas as pd
from pyspark.sql import SparkSession
import osmium
from pathlib import Path
from tqdm import tqdm
from pyspark.sql.functions import (
    col, when, trim, upper, regexp_replace, lpad, length,
    to_date, year, month, dayofmonth, lit, count
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, IntegerType
)

#  Split CSV into Parquet chunks
def split_csv_to_parquet(csv_path: str, out_dir: str, chunksize: int = 1_000_000):
    os.makedirs(out_dir, exist_ok=True)

    for i, chunk in enumerate(pd.read_csv(
        csv_path,
        chunksize=chunksize,
        dtype={"Zip Code": str},  # keep ZIP code as string
        low_memory=False,
        encoding="utf-8"
    )):
        out_path = os.path.join(out_dir, f"iowa_chunk_{i}.parquet")
        chunk.to_parquet(out_path, index=False)
        print(f"[{i:03d}] saved {out_path}, rows={len(chunk):,}")

    print(f"CSV has been split into {i+1} Parquet chunks. Directory: {out_dir}")


# Clean Parquet chunks with Spark
def clean_parquet_chunks(parquet_dir: str, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)

    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("IA_LiquorSales_ChunkClean")
        .config("spark.executor.memory", "6g")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.sql.files.maxPartitionBytes", "48m")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

    # Define a flexible schema
    parquet_schema = StructType([
        StructField("invoice_item_number", StringType(), True),
        StructField("date", StringType(), True),
        StructField("store_number", StringType(), True),
        StructField("store_name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("store_location", StringType(), True),
        StructField("county_number", StringType(), True),
        StructField("county", StringType(), True),
        StructField("category", StringType(), True),
        StructField("category_name", StringType(), True),
        StructField("vendor_number", StringType(), True),
        StructField("vendor_name", StringType(), True),
        StructField("item_number", StringType(), True),
        StructField("item_description", StringType(), True),
        StructField("pack", StringType(), True),
        StructField("bottle_volume_ml", StringType(), True),
        StructField("state_bottle_cost", StringType(), True),
        StructField("state_bottle_retail", StringType(), True),
        StructField("bottles_sold", StringType(), True),
        StructField("sale_dollars", StringType(), True),
        StructField("volume_sold_liters", StringType(), True),
        StructField("volume_sold_gallons", StringType(), True),
    ])

    # Read all Parquet chunks
    df = spark.read.option("ignoreCorruptFiles", "true") \
                   .schema(parquet_schema) \
                   .parquet(parquet_dir)

    print("Original Schema:")
    df.printSchema()

    # Normalize column names to snake_case
    df = df.select([col(c).alias(c.strip().lower().replace(" ", "_").replace("/", "_")) for c in df.columns])
    print("Normalized column names:")
    print(df.columns)

    # Clean string columns
    string_cols = [c for c, t in df.dtypes if t == "string"]
    for c in string_cols:
        df = df.withColumn(c, upper(trim(col(c))))

    # Clean ZIP code
    if "zip_code" in df.columns:
        df = df.withColumn("zip_code", when(col("zip_code").isin("", "NA", "N/A", "-"), None)
                           .otherwise(trim(col("zip_code"))))
        df = df.withColumn("zip_code", regexp_replace(col("zip_code"), "[^0-9]", ""))
        df = df.withColumn("zip_code", when(length(col("zip_code")) < 5, lpad(col("zip_code"), 5, "0"))
                                         .otherwise(col("zip_code")))
        df = df.withColumn("zip_code", col("zip_code").cast(StringType()))
    else:
        df = df.withColumn("zip_code", lit(None).cast(StringType()))

    # Convert integer-like columns
    int_cols = ["store_number", "vendor_number", "item_number", "pack", "bottles_sold", "county_number"]
    for c in int_cols:
        if c in df.columns:
            df = df.withColumn(c, regexp_replace(col(c), "[^0-9\\-+]", ""))
            df = df.withColumn(c, when(col(c).cast(LongType()).isNotNull(), col(c).cast(LongType()))
                               .otherwise(lit(None).cast(LongType())))

    # Convert float-like columns
    float_cols = ["bottle_volume_ml", "state_bottle_cost", "state_bottle_retail",
                  "sale_dollars", "volume_sold_liters", "volume_sold_gallons"]
    for c in float_cols:
        if c in df.columns:
            df = df.withColumn(c, regexp_replace(col(c), "[^0-9eE\\.\\-+]", ""))
            df = df.withColumn(c, when(col(c).cast(DoubleType()).isNotNull(), col(c).cast(DoubleType()))
                               .otherwise(lit(None).cast(DoubleType())))

    # Extract year, month, day from date
    if "date" in df.columns:
        df = df.withColumn("date", to_date(col("date"), "MM/dd/yyyy"))
        df = df.withColumn("year", year(col("date")).cast(IntegerType()))
        df = df.withColumn("month", month(col("date")).cast(IntegerType()))
        df = df.withColumn("day", dayofmonth(col("date")).cast(IntegerType()))
    else:
        df = df.withColumn("year", lit(None).cast(IntegerType()))
        df = df.withColumn("month", lit(None).cast(IntegerType()))
        df = df.withColumn("day", lit(None).cast(IntegerType()))

    # Simple stats
    stats = df.agg(count("*").alias("total_rows"),
                   (count("*") - count(col("zip_code"))).alias("missing_zip")).collect()[0]
    print(f"Total rows: {stats['total_rows']:,}, Missing ZIP: {stats['missing_zip']:,}")

    # Parse Iowa OSM PBF file for address coordinates
    class AddressHandler(osmium.SimpleHandler):
        """Extract nodes with house number and street from OSM PBF."""

        def __init__(self):
            super().__init__()
            self.data = []

        def node(self, n):
            if 'addr:housenumber' in n.tags and 'addr:street' in n.tags:
                addr = f"{n.tags.get('addr:housenumber', '')} {n.tags.get('addr:street', '')}, {n.tags.get('addr:city', '')}"
                self.data.append({
                    "address": addr.strip(),
                    "lon": n.location.lon,
                    "lat": n.location.lat
                })

    pbf_file = input("Enter Iowa OSM PBF file path: ").strip()
    if not Path(pbf_file).is_file():
        raise FileNotFoundError(f"PBF file not found: {pbf_file}")

    handler = AddressHandler()
    print("Parsing Iowa PBF file ...")
    handler.apply_file(pbf_file)
    df_osm = pd.DataFrame(handler.data)
    print(f"Loaded {len(df_osm)} addresses from OSM")

    # Fill missing lon/lat from OSM data
    df_osm_spark = spark.createDataFrame(df_osm).select(
        col("address"),
        col("lon").alias("lon_osm"),
        col("lat").alias("lat_osm")
    )

    to_geocode = df.filter(col("lon").isNull() | col("lat").isNull())

    if to_geocode.rdd.isEmpty():
        print("All rows already have lon/lat. No geocoding needed.")
        filled = df
    else:
        geocoded_df = to_geocode.join(
            df_osm_spark,
            to_geocode.Address == df_osm_spark.address,
            how="left"
        ).drop("address")

        geocoded_df = geocoded_df.withColumn(
            "lon",
            when(col("lon").isNull(), col("lon_osm")).otherwise(col("lon"))
        ).withColumn(
            "lat",
            when(col("lat").isNull(), col("lat_osm")).otherwise(col("lat"))
        ).drop("lon_osm", "lat_osm")

        already_have = df.filter(col("lon").isNotNull() & col("lat").isNotNull())
        filled = already_have.unionByName(geocoded_df, allowMissingColumns=True)

    # Write single cleaned Parquet file
    out_path = os.path.join(out_dir, "iowa_cleaned.parquet")
    df.write.mode("overwrite").parquet(out_path)
    print(f"Cleaning done. Output saved at: {out_path}")

    spark.stop()


# Main program
if __name__ == "__main__":
    csv_path = input("CSV file path: ").strip()
    chunk_dir = input("Parquet chunks directory (default: iowa_parquet_chunks): ").strip() or "iowa_parquet_chunks"
    try:
        chunk_size = int(input("Rows per chunk (default: 1_000_000): ").strip() or "1000000")
    except ValueError:
        print("Invalid input, using default 1_000_000 rows per chunk")
        chunk_size = 1_000_000

    split_csv_to_parquet(csv_path, chunk_dir, chunksize=chunk_size)
    clean_output_dir = "../output/iowa_cleaned_output"
    clean_parquet_chunks(chunk_dir, clean_output_dir)

    print("All done!")
