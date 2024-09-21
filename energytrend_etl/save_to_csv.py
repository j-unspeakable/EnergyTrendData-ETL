import os
import logging
import pandas as pd
from prefect import task
from energytrend_etl.logger_config import setup_logger


# Set up logging
logger = setup_logger(
    name=__name__,
    log_file='./logs/save_to_csv.log',
    level=logging.INFO,
    log_format='%(asctime)s - %(levelname)s - %(message)s'
)


# Prefect task
@task(log_prints=True, tags=["save_data"])
def save_data_to_csv(df: pd.DataFrame, filename: str, output_path: str = './output') -> str:
    """
    Function to save data to a CSV file.

    Args:
        df (pd.DataFrame): The preprocessed DataFrame to save.
        filename (str): The base filename (without extension) to use for the saved file.
        output_path (str): The directory path where the output CSV file will be saved (default is './output').

    Returns:
        str: The base filename of the saved data without extension.
    """
    try:
        # Ensure the target directory exists
        os.makedirs(output_path, exist_ok=True)
        
        # Construct the save path
        save_filename = os.path.splitext(filename)[0]
        save_csv = os.path.join(output_path, f'{save_filename}.csv')
        
        # Save the DataFrame to a CSV file
        df.to_csv(save_csv, index=True)
        logger.info(f"Data successfully saved in the file {save_csv}")
        
        return save_filename

    except Exception as e:
        logger.error(f"Error saving data to CSV file: {str(e)}")
        return ""
