# Midwestern U.S. New Alcohol Introduction Analysis

## Overview
This project builds an interactive dashboard to analyze alcohol sales trends across Midwest states (Iowa + other states). It includes a complete data cleaning and validation workflow to transform datasets into dashboard-ready outputs.

## What I Did (My Contribution)
1. Cleaned and standardized datasets (type casting, ZIP code formatting, date parsing)
2. Converted large CSV files into Parquet chunks for faster processing
3. Validated data integrity using both Pandas and Spark (row consistency / missing ZIP checks)
4. Ran and tested an interactive multi-tab dashboard for exploratory analysis

## Tech Stack
- Python
- Pandas
- PySpark
- Parquet
- Dash / Plotly

## Demo Video
YouTube (Unlisted): https://www.youtube.com/watch?v=zUL7CU3gCVo

## Run the Dashboard
From the project root:

    pip install -r requirements.txt
    python -m dashboard.main_dashboard
    
Then open:
http://127.0.0.1:8050

## Data Source
Raw datasets are not included in this repository due to size/licensing constraints. This repo provides cleaned dashboard-ready CSV outputs used directly by the dashboard.

Folder: `output/dashboard/`
- `alcohol_tax.csv`
- `iowa_dashboard_data.csv`
- `minnesota_dashboard_data.csv`
- `wisconsin_dashboard_data.csv`
- `pop_income_dashboard_data.csv`
- `weather_dashboard_data.csv`

## Dataset Cleaning
Folder: `src/`

### check_parquet_chunks_spark.py
Checks row consistency in dashboard-ready Parquet chunks using Spark.

### iowa_check_parquet_chunks_pandas.py
Checks CSV vs Parquet consistency and validates Zip Code formatting using Pandas.

### csv_to_parquet_chunks.py
Splits a large CSV file into Parquet chunks to improve read performance.

### iowa_data_clean.py
Cleans Iowa liquor data and prepares dashboard-ready monthly aggregates.

### geocode_fill_missing_geo.py
Optional script to fill missing longitude/latitude using Iowa OSM PBF data (offline geocoding).  
Note: `iowa.osm.pbf` is not included in the repository due to file size.

## Dashboard Data Cleaning Package
Folder: `other_midwest_states/`  
Output folder: `output/dashboard/`

### main_clean.py
Runs all cleaning modules and produces dashboard-ready CSV outputs.

### iowa_dashboard_data.py
Cleans Iowa data and aggregates monthly revenue and volume.

### wisconsin_dashboard_data.py
Cleans Wisconsin Excel data, converts fiscal-year data to calendar-year format, and aggregates monthly values.

### minnesota_dashboard_data.py
Parses Minnesota PDF reports and aggregates monthly totals.

### pop_income_dashboard_data.py
Processes population and personal income data and expands quarterly income to monthly per-capita income.

### alcohol_tax_dashboard_data.py
Cleans alcohol tax datasets and standardizes units to per liter.

### weather_dashboard_data.py
Cleans weather data for multiple states and aggregates monthly averages.

### __init__.py
Marks this directory as a Python package.

## Dashboard Package
Folder: `dashboard/`

### main_dashboard.py
Main entry point for the interactive multi-tab dashboard. Loads all datasets once, builds all tab layouts, registers callbacks for each module, and provides a unified UI for navigating between dashboards.

### revenue_volume_weather.py
Dashboard 1 – Revenue / Volume / Weather

### revenue_volume_population.py
Dashboard 2 – Revenue / Volume / Population

### revenue_income.py
Dashboard 3 – Revenue vs Income

### alcohol_tax_rates.py
Dashboard 4 – Alcohol Tax Rates (Beer / Wine / Liquor)

## Notes
Raw datasets are not included in this repository due to file size and licensing constraints.  
This repository includes cleaned, dashboard-ready CSV files under `output/dashboard/` so the dashboard can run directly after cloning.
