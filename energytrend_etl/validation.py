import os
import logging
import pandas as pd
from prefect import task
from energytrend_etl.logger_config import setup_logger


# Set up logging
logger = setup_logger(
    name=__name__,
    log_file='./logs/validate_data.log',
    level=logging.INFO
)


# Prefect task
@task(log_prints=True, tags=["validate_data"])
def validate_data(filename: str, df: pd.DataFrame, sheet_name: str, header: int) -> pd.DataFrame:
    """
    Function to validate the schema of the data.

    Args:
        filename (str): The name of the Excel file with previous data.
        df (pd.DataFrame): The processed DataFrame to validate.
        sheet_name (str): The name of the sheet in the Excel file.
        header (int): Row (0-indexed) to use for the column labels of the parsed DataFrame.

    Returns:
        pd.DataFrame: The previous unprocessed DataFrame for reference.
    """
    try:
        # Read the previous unprocessed dataset
        previous_df = pd.read_excel(f"./data/{filename}", sheet_name=sheet_name, header=header)

        # Basic cleaning on the previous DataFrame
        previous_df.rename(columns=lambda x: x.replace(' ', '_').replace('\n', '_'), inplace=True)
        previous_df.set_index('Column1', inplace=True)

        # Subset the columns of interest from the previous data (exclude engineered columns)
        previous_df = previous_df[df.columns[:-2]]
        new_df = df[df.columns[:-2]]

        # Validation checks
        if set(previous_df.columns) != set(new_df.columns):
            logger.warning("Columns in previous and new data don't match")
        else:
            logger.info("Columns in previous and new data match")

        if not all(previous_df.dtypes == new_df.dtypes):
            logger.warning("Data types in previous and new data don't match")
        else:
            logger.info("Data types in previous and new data match")

        if previous_df.shape[0] != new_df.shape[0]:
            logger.warning("Number of rows in previous and new data don't match")
        else:
            logger.info("Number of rows in previous and new data match")

        if not previous_df.equals(new_df):
            logger.warning("Values in previous and new data don't match")
        else:
            logger.info("Values in previous and new data match")

        return previous_df

    except Exception as e:
        logger.error(f"Error validating data: {str(e)}")

        # Return an empty DataFrame on error for consistency
        return pd.DataFrame()
