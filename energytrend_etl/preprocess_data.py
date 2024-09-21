import logging
import pandas as pd
from prefect import task
from energytrend_etl.logger_config import setup_logger


# Set up logging
logger = setup_logger(
    name=__name__,
    log_file='./logs/preprocess_data.log',
    level=logging.INFO,
    log_format='%(asctime)s - %(levelname)s - %(message)s'
)


# Prefect task
@task(log_prints=True, tags=["preprocess_data"])
def process_excel_data(
        filename: str, 
        sheet_name: str, 
        header: int, 
        min_rows: int = 5, 
        max_missing_percentage: 
        float = 20.0
) -> pd.DataFrame:
    """
    Function to preprocess Excel data and perform integrity checks.
    
    Args:
        filename (str): The name of the Excel file.
        sheet_name (str): The name of the sheet to process.
        header (int): Row (0-indexed) to use for the column labels of the parsed DataFrame.
        min_rows (int): Minimum number of rows required in the DataFrame for integrity (default is 10).
        max_missing_percentage (float): Maximum allowed percentage of missing values (default is 20%).

    Returns:
        pd.DataFrame: Preprocessed DataFrame.
    """
    try:
        # Read and preprocess raw data
        df = pd.read_excel(f"./data/{filename}", sheet_name=sheet_name, header=header)
        
        # Basic cleaning: replace spaces and newline characters in column names
        df.rename(columns=lambda x: x.strip().replace(' ', '_').replace('\n', '_'), inplace=True)
        
        # Integrity Check 1: Ensure key columns are present
        # We can replace with actual key columns if more than 'Column1' is required.
        required_columns = ['Column1']
        if not all(column in df.columns for column in required_columns):
            logger.error(f"Missing one or more required columns: {required_columns}")
            return pd.DataFrame()
        
        # Integrity Check 2: Ensure the DataFrame has a minimum number of rows
        if len(df) < min_rows:
            logger.error(f"DataFrame has less than the required minimum number of rows ({min_rows}).")
            return pd.DataFrame()

        # Integrity Check 3: Ensure the percentage of missing values is within acceptable limits
        total_cells = df.size
        missing_cells = df.isna().sum().sum()
        missing_percentage = (missing_cells / total_cells) * 100
        if missing_percentage > max_missing_percentage:
            logger.error(f"DataFrame has {missing_percentage:.2f}% missing values, which exceeds the maximum allowed {max_missing_percentage}%.")
            return pd.DataFrame()
        
        # Set index and clean up the index name
        df.set_index('Column1', inplace=True)
        df.index.name = None
        
        # Handle missing values explicitly if needed.
        # Replace NaNs with empty strings
        df.fillna('', inplace=True)
        
        # Add metadata columns
        df['processed_date'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        df['filename'] = filename
        
        logger.info(f"Data processing complete. Processed DataFrame head:\n{df.head()}")
        return df

    except Exception as e:
        logger.error(f"Error processing Excel data from {filename}: {str(e)}")
        return pd.DataFrame()
