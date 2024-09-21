import logging
import argparse
from prefect import flow
from energytrend_etl.logger_config import setup_logger
from energytrend_etl.validation import validate_data
from energytrend_etl.save_to_csv import save_data_to_csv
from energytrend_etl.ingest_data import ingest_excel_files
from energytrend_etl.preprocess_data import process_excel_data
from energytrend_etl.validation_report import generate_data_profiling_report, generate_data_consistency_report


# Initialize the logger for main.py
logger = setup_logger(
    name='main',
    log_file='./logs/main.log',
    level=logging.INFO
)


# Prefect flow
@flow(name="Energy Trend Data ETL")
def main(output_path: str) -> None:
    """
    Main function for the data pipeline.

    Args:
        output_path (str): The directory path where the output file will be saved.
    """
    
    sheet_name = 'Quarter'
    header = 4
    url = 'https://www.gov.uk/government/statistics/oil-and-oil-products-section-3-energy-trends'
    html_name = "Supply and use of crude oil, natural gas liquids and feedstocks (ET 3.1 - quarterly)"
    
    # Ingest data
    filename = ingest_excel_files(url, html_name)
    if not filename:
        logger.error("Failed to ingest data. Exiting pipeline.")
        return

    # Preprocess data
    df = process_excel_data(filename, sheet_name, header)
    if df.empty:
        logger.error("Failed to process data. Exiting pipeline.")
        return

    # Validate data
    previous_df = validate_data(filename, df, sheet_name, header)
    if previous_df.empty:
        logger.error("Data validation failed. Exiting pipeline.")
        return

    # Save processed data as CSV file
    csv_filename = save_data_to_csv(df, filename, output_path)
    if not csv_filename:
        logger.error("Failed to save data to CSV. Exiting pipeline.")
        return

    # Generate data profiling report and consistency report
    if not generate_data_profiling_report(df, csv_filename):
        logger.error("Failed to generate data profiling report. Exiting pipeline.")
        return

    if not generate_data_consistency_report(df, previous_df, csv_filename):
        logger.error("Failed to generate data consistency report. Exiting pipeline.")
        return

if __name__ == '__main__':
    # Set up argument parsing
    parser = argparse.ArgumentParser(description='Process and analyze energy trend data.')
    parser.add_argument('--output-path', type=str, default='./output', help='The directory to save output files to.')
    
    args = parser.parse_args()

    # Run main with the provided output path
    main(args.output_path)
