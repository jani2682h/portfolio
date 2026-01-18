from iowa_dash_data import run_iowa_dashboard_job
from wisconsin_dash_data import run_wisconsin_dashboard_job
from minnesota_dash_data import run_minnesota_dashboard_main
from pop_income_dash_data import run_pop_income_dashboard_data
from alcohol_tax_dash_data import run_alcohol_tax_dashboard
from weather_dash_data import run_weather_dashboard_data


def main():
    # Iowa
    # Note: Input folder is not included in Git due to large CSV/Parquet files.
    #       Replace the path below with your local folder path.
    run_iowa_dashboard_job(
        input_folder="../output/iowa_cleaned",  # Path to folder containing raw Iowa CSV or Parquet files
        output_csv="../output/dashboard/iowa_dashboard_data.csv"  # Path to save processed CSV
    )

    #  Wisconsin
    run_wisconsin_dashboard_job(
        input_folder="../input/wisconsin",
        output_csv="../output/dashboard/wisconsin_dashboard_data.csv"
    )

    #  Minnesota
    run_minnesota_dashboard_main(
        input_folder="../input/minnesota",
        output_csv="../output/dashboard/minnesota_dashboard_data.csv"
    )

    #  Population & Income
    run_pop_income_dashboard_data(
        pop_folder="../input/population",
        output_path="../output/dashboard/pop_income_dashboard_data.csv"
    )

    #  Alcohol Tax (Midwest)
    run_alcohol_tax_dashboard(
        input_dir="../input/tax",
        output_path="../output/dashboard/alcohol_tax.csv"
    )

    # Weather
    run_weather_dashboard_data(
        input_dir="../input",
        output_file="../output/dashboard/weather_dashboard_data.csv"
    )

    print("\n All dashboard data generated successfully.\n")


if __name__ == "__main__":
    main()




