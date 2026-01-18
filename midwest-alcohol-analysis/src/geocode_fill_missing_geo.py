import pandas as pd
import osmium
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import (
    DoubleType, StringType, StructType, StructField
)

spark = (
    SparkSession.builder.appName("FillMissingGeo")
    .config("spark.executor.memory", "6g")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.sql.shuffle.partitions", "64")
    .config("spark.sql.files.maxPartitionBytes", "48m")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")


geo_schema = StructType(
    [
        StructField("Invoice/Item Number", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("City", StringType(), True),
        StructField("Zip Code", StringType(), True),
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True),
    ]
)

parquet_dir = input("Enter folder containing Parquet: ").strip()
df = spark.read.schema(geo_schema).parquet(parquet_dir)

# Add missing lon/lat columns if absent
for c in ("lon", "lat"):
    if c not in df.columns:
        df = df.withColumn(c, lit(None).cast(DoubleType()))

print("Columns after adding missing ones:", df.columns)
df.show(5, truncate=False)


# Rows that need geocoding
to_geocode = df.filter(
    col("lon").isNull() | col("lat").isNull()
)

if to_geocode.rdd.isEmpty():
    out_dir = input("Enter output folder (default 'parquet_filled_geo'): ").strip() or "parquet_filled_geo"
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(out_dir)
    print(f"All rows already have valid lon/lat. Written to {out_dir}")
    spark.stop()
    raise SystemExit(0)


# Read Iowa PBF offline using pyosmium

class AddressHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.data = []

    def node(self, n):
        if 'addr:housenumber' in n.tags and 'addr:street' in n.tags:
            addr = f"{n.tags.get('addr:housenumber','')} {n.tags.get('addr:street','')}, {n.tags.get('addr:city','')}"
            self.data.append({
                "address": addr.strip(),
                "lon": n.location.lon,
                "lat": n.location.lat
            })

pbf_file = "../data/iowa.osm.pbf"
handler = AddressHandler()
print("Parsing iowa.osm.pbf ...")
handler.apply_file(pbf_file)
df_osm = pd.DataFrame(handler.data)
print(f"Loaded {len(df_osm)} OSM addresses")


# Rename OSM columns to avoid ambiguity after join
df_osm_spark = spark.createDataFrame(df_osm).select(
    col("address"),
    col("lon").alias("lon_osm"),
    col("lat").alias("lat_osm")
)

# Perform left join on Address
geocoded_df = to_geocode.join(
    df_osm_spark,
    to_geocode.Address == df_osm_spark.address,
    how="left"
).drop("address")

# Fill missing lon/lat with OSM values
geocoded_df = geocoded_df.withColumn(
    "lon",
    when(col("lon").isNull(), col("lon_osm")).otherwise(col("lon"))
).withColumn(
    "lat",
    when(col("lat").isNull(), col("lat_osm")).otherwise(col("lat"))
).drop("lon_osm", "lat_osm")


already_have = df.filter(
    col("lon").isNotNull() & col("lat").isNotNull()
)

filled = already_have.unionByName(geocoded_df, allowMissingColumns=True)

out_dir = input("Enter output folder (default 'parquet_filled_geo'): ").strip() or "parquet_filled_geo"
Path(out_dir).mkdir(parents=True, exist_ok=True)
filled.write.mode("overwrite").parquet(out_dir)
print(f"Missing/invalid lon/lat filled. Result written to: {Path(out_dir).resolve()}")

from pyspark.sql.functions import col

df = spark.read.parquet("../output/addresses_needing_geocode")

df.show(20, truncate=False)

df.filter(col("lon").isNull() | col("lat").isNull()).show(50, truncate=False)

print("Null count:", df.filter(col("lon").isNull() | col("lat").isNull()).count())

spark.stop()