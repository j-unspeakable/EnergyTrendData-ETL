from energytrend_etl.main import main

if __name__ == "__main__":
    # Define the output path for the deployment
    output_path = "./output"

    # Create a scheduled deployment using serve with the parameters
    main.serve(
        name="Daily Energy Trend Data ETL Deployment",
        cron="0 0 * * *",  # Run every day at midnight
        parameters={"output_path": output_path}  # Pass the output path parameter
    )