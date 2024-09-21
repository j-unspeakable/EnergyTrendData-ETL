import os
import time
import logging
import requests
from prefect import task
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from energytrend_etl.logger_config import setup_logger
from tenacity import retry, stop_after_attempt, wait_exponential


# Set up logging
logger = setup_logger(
    name=__name__,
    log_file='./logs/ingest_data.log',
    level=logging.INFO,
    log_format='%(asctime)s - %(levelname)s - %(message)s'
)


# Retry logic for download
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def download_file(url: str, save_path: str) -> None:
    """
    Downloads a file from the specified URL and saves it to the provided path.

    Args:
        url (str): The URL of the file to download.
        save_path (str): The local path to save the downloaded file.

    Returns:
        None
    """
    response = requests.get(url)
    response.raise_for_status()
    with open(save_path, 'wb') as file:
        file.write(response.content)
    logger.info(f'File {save_path} downloaded successfully.')


# Retry logic for initial HTML request
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_html(url: str) -> requests.Response:
    """
    Fetches the HTML content from the specified URL.

    Args:
        url (str): The URL of the webpage to fetch.

    Returns:
        requests.Response: The response object containing the HTML content.
    """
    response = requests.get(url)
    response.raise_for_status()
    return response


# Prefect task
@task(log_prints=True, tags=["ingest_data"])
def ingest_excel_files(url: str, html_name: str) -> str:
    """
    Ingests Excel data by scraping the provided URL for a specific Excel file, checking if it's updated, 
    and downloading it if necessary.

    Args:
        url (str): The URL of the webpage containing links to Excel files.
        html_name (str): The name or part of the name of the HTML element containing the target Excel file.

    Returns:
        str: The filename of the downloaded Excel file.
    """
    try:
        # Get the webpage HTML with retry logic
        response = fetch_html(url)

        # Parse the HTML using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all links that end with .xls or .xlsx
        file_links = [
            (urljoin(url, link.get('href')), link.text) 
            for link in soup.find_all('a') 
            if link.get('href') and link.get('href').endswith(('.xls', '.xlsx'))
        ]

        # Find the link to the Excel file with the HTML name on the site.
        target_link = None
        for link, text in file_links:
            if html_name in text:
                target_link = link
                break

        if target_link:
            filename = os.path.basename(target_link)
            file_path = os.path.join('./data', filename)

            # Create directory if it does not exist
            os.makedirs('./data', exist_ok=True)

            # Check if file exists and is up to date
            if os.path.exists(file_path):
                local_mod_time = os.path.getmtime(file_path)
                response = requests.head(target_link)
                response.raise_for_status()
                website_mod_time = time.mktime(time.strptime(response.headers['Last-Modified'], '%a, %d %b %Y %H:%M:%S %Z'))
                
                if website_mod_time <= local_mod_time:
                    logger.info(f'{filename} is already up-to-date.')
                    return filename
            
            # Download file if not up-to-date
            download_file(target_link, file_path)
            return filename

        else:
            logger.info('Excel file not found on the provided URL.')

    except requests.exceptions.RequestException as e:
        logger.error(f"Error during requests to {url}: {str(e)}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")
    return ""
