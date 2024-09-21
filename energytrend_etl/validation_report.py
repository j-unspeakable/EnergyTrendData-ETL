import os
import logging
import pandas as pd
from prefect import task
from ydata_profiling import ProfileReport
from energytrend_etl.logger_config import setup_logger


# Set up logging
logger = setup_logger(
    name=__name__,
    log_file='./logs/validation_report.log',
    level=logging.INFO,
    log_format='%(asctime)s - %(levelname)s - %(message)s'
)


# Prefect task
@task(log_prints=True, tags=["profiling_report"])
def generate_data_profiling_report(
        df: pd.DataFrame, 
        save_filename: str, 
        report_dir: str = './report'
) -> str:
    """
    Function to generate a data profiling report.

    Args:
        df (pd.DataFrame): The preprocessed DataFrame to profile.
        save_filename (str): The base filename to use for the saved report.
        report_dir (str): The directory where reports will be saved (default is './report').

    Returns:
        str: Path to the generated data profiling report.
    """
    try:
        # Ensure the report directory exists
        os.makedirs(report_dir, exist_ok=True)
        
        # Generate standard HTML profiling report with pandas_profiling
        profile = ProfileReport(df, minimal=True)
        save_path_html = os.path.join(report_dir, f"{save_filename}_data_profiling.html")
        profile.to_file(save_path_html)

        # Generate additional profiling statistics in CSV format
        missing_values = df.isna().sum()
        row_count = len(df)
        column_count = len(df.columns)
        description = df.describe(include='all')
        description.loc['missing_values'] = missing_values
        description.loc['row_count'] = row_count
        description.loc['column_count'] = column_count

        save_path_csv = os.path.join(report_dir, f"{save_filename}_data_profiling.csv")
        description.to_csv(save_path_csv)

        logger.info(f"Data profiling report generated at {save_path_html} and {save_path_csv}")
        return save_path_html

    except Exception as e:
        logger.error(f"Error generating data profiling report: {str(e)}")
        return ""


# Prefect task
@task(log_prints=True, tags=["consistency_report"])
def generate_data_consistency_report(
        df: pd.DataFrame, 
        previous_df: pd.DataFrame, 
        save_filename: str, 
        report_dir: str = './report'
) -> str:
    """
    Function to generate a data consistency report.

    Args:
        df (pd.DataFrame): The current preprocessed DataFrame.
        previous_df (pd.DataFrame): The previous unprocessed DataFrame to compare against.
        save_filename (str): The base filename to use for the saved report.
        report_dir (str): The directory where reports will be saved (default is './report').

    Returns:
        str: Path to the generated data consistency report.
    """
    try:
        report = {}

        # Ensure the report directory exists
        os.makedirs(report_dir, exist_ok=True)

        # Check for correct time format
        time_cols = ['processed_date']
        for col in time_cols:
            if col in df.columns and not pd.api.types.is_datetime64_dtype(df[col]):
                report[col] = f"Column {col} has incorrect time format"
            else:
                report[col] = f"Column {col} has correct time format"

        # Check for missing values
        for col in df.columns:
            if df[col].isnull().sum() > 0:
                report[col] = f"Column {col} has {df[col].isnull().sum()} missing value(s)"
            else:
                report[col] = "No missing values"

        # Check for new and missing columns
        new_cols = set(df.columns) - set(previous_df.columns)
        if new_cols:
            report['new_columns'] = f"New column(s) detected: {new_cols}"
        else:
            report['new_columns'] = "No new column(s) detected"
        
        missing_cols = set(previous_df.columns) - set(df.columns)
        if missing_cols:
            report['missing_columns'] = f"Missing column(s) detected: {missing_cols}"
        else:
            report['missing_columns'] = "No missing column(s) detected"
        
        # Save report to file
        report_df = pd.DataFrame(report.items(), columns=['Check', 'Result'])
        save_path_csv = os.path.join(report_dir, f"{save_filename}_data_consistency.csv")
        report_df.to_csv(save_path_csv, index=False)
        logger.info(f"Data consistency report generated at {save_path_csv}")

        return save_path_csv

    except Exception as e:
        logger.error(f"Error generating data consistency report: {str(e)}")
        return ""
