from pathlib import Path
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, to_date, sum as _sum, round, regexp_replace, lit
from pyspark.sql.types import DoubleType

# Process Iowa data folder (CSV or Parquet) and save monthly summary
def run_iowa_dashboard_job(input_folder: str, output_csv: str) -> None:
    input_path = Path(input_folder)
    if not input_path.is_dir():
        raise FileNotFoundError(f"Input folder not found: {input_path}")

    # Start Spark session
    spark = SparkSession.builder.appName("IowaLiquorDashboard").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # List all CSV or Parquet files in folder
    files = sorted(input_path.glob("*.csv")) + sorted(input_path.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No CSV or Parquet files found in {input_path}")

    # Read all files and union
    df_list = []
    for f in files:
        if f.suffix.lower() == ".csv":
            df_list.append(spark.read.option("header", True).csv(str(f)))
        else:
            df_list.append(spark.read.parquet(str(f)))

    df = df_list[0]
    for other in df_list[1:]:
        df = df.unionByName(other)

    df = df.withColumn("State_Name", lit("Iowa"))

    # Rename columns for consistency
    rename_map = {
        "Date": "date",
        "Sale (Dollars)": "sale_dollars",
        "Volume Sold (Liters)": "volume_sold_liter",
    }
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    # Clean numeric columns
    df = df.withColumn("sale_dollars", regexp_replace(col("sale_dollars"), r"[\$,]", "").cast(DoubleType())) \
           .withColumn("volume_sold_liter", regexp_replace(col("volume_sold_liter"), ",", "").cast(DoubleType()))

    # Convert to date and extract year/month
    df = df.withColumn("date", to_date(col("date"), "MM/dd/yyyy"))
    df = df.withColumn("Year", year(col("date"))).withColumn("Month", month(col("date")))

    # Aggregate monthly totals
    agg = df.groupBy("State_Name", "Year", "Month") \
            .agg(
                round(_sum("sale_dollars"), 2).alias("Total_Revenue"),
                round(_sum("volume_sold_liter"), 2).alias("Total_Volume_Liters")
            ).orderBy("Year", "Month")

    # Write to CSV via temporary folder
    tmp_dir = Path(output_csv).with_suffix(".tmp")
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)

    agg.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(tmp_dir))

    part_file = next(tmp_dir.glob("part-*.csv"))
    final_path = Path(output_csv)
    final_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(part_file), str(final_path))
    shutil.rmtree(tmp_dir)

    spark.stop()
    print(f" CSV written to {final_path}")






