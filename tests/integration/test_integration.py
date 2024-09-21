import os
import pytest
import pandas as pd
from unittest import mock
from energytrend_etl.save_to_csv import save_data_to_csv
from energytrend_etl.ingest_data import ingest_excel_files
from energytrend_etl.preprocess_data import process_excel_data


# Constants for the test
MOCK_URL = "http://example.com"
MOCK_HTML_NAME = "Test File"
MOCK_EXCEL_FILENAME = "test_file.xlsx"
MOCK_SHEET_NAME = "Sheet1"
MOCK_HEADER = 0
MOCK_SAVE_PATH = "./data/test_file.xlsx"
MOCK_CSV_PATH = "./data/test_file.csv"

# Mock HTML content
HTML_CONTENT = """
<html>
<head><title>Test</title></head>
<body>
<a href="test_file.xlsx">Test File</a>
<a href="another_test_file.xls">Another Test File</a>
</body>
</html>
"""

# Test Data for DataFrame
MOCK_DF_DATA = {
    'Column1': ['A', 'B', 'C', 'D', 'E'],
    'Value1': [1, 2, 3, 4, 5],
    'Value2': [6, 7, 8, 9, 10]
}


@pytest.fixture
def mock_environment(monkeypatch):
    """Fixture to mock environment setup and external dependencies."""
    # Mocking os.path.exists to simulate file checking
    monkeypatch.setattr(os.path, 'exists', mock.Mock(return_value=False))

    # Patching os.makedirs to avoid actual directory creation
    monkeypatch.setattr(os, 'makedirs', mock.Mock())

    # Mocking requests.get to return a mock response
    response_mock = mock.Mock(status_code=200)
    response_mock.raise_for_status = mock.Mock()
    response_mock.content = HTML_CONTENT.encode('utf-8')
    monkeypatch.setattr('energytrend_etl.ingest_data.requests.get', mock.Mock(return_value=response_mock))

    # Mocking requests.head to simulate the 'Last-Modified' header in responses
    monkeypatch.setattr('energytrend_etl.ingest_data.requests.head', mock.Mock(return_value=response_mock))

    # Mocking pandas read_excel to return a mock DataFrame
    mock_df = pd.DataFrame(MOCK_DF_DATA)
    mock_read_excel = mock.Mock(return_value=mock_df)
    monkeypatch.setattr('energytrend_etl.preprocess_data.pd.read_excel', mock_read_excel)

    # Mocking the CSV save to avoid file I/O
    mock_to_csv = mock.Mock()
    monkeypatch.setattr('energytrend_etl.save_to_csv.pd.DataFrame.to_csv', mock_to_csv)

    return mock_read_excel, mock_to_csv


@pytest.mark.usefixtures("mock_environment")
def test_end_to_end_pipeline(mock_environment):
    """Integration test for the entire data pipeline."""
    # Unpack the mock objects
    mock_read_excel, mock_to_csv = mock_environment

    # Step 1: Ingest Data
    ingested_filename = ingest_excel_files.fn(MOCK_URL, MOCK_HTML_NAME)  # Use .fn to call directly without Prefect orchestration
    assert ingested_filename == MOCK_EXCEL_FILENAME, f"Failed to ingest data or incorrect filename returned. Received: {ingested_filename}"

    # Step 2: Preprocess Data
    processed_df = process_excel_data.fn(ingested_filename, MOCK_SHEET_NAME, MOCK_HEADER, min_rows=5)  # Use .fn here too
    assert not processed_df.empty, "Preprocessing failed or returned an empty DataFrame."
    assert 'processed_date' in processed_df.columns, "Processed DataFrame should have 'processed_date' column."
    assert 'filename' in processed_df.columns, "Processed DataFrame should have 'filename' column."

    # Step 3: Save Processed Data to CSV (include output_path)
    output_path = "./output"  # Define the output path
    saved_filename = save_data_to_csv.fn(processed_df, ingested_filename, output_path)  # And here
    expected_csv_filename = os.path.splitext(ingested_filename)[0]
    assert saved_filename == expected_csv_filename, "Saving to CSV failed or incorrect filename returned."

    # Verify mocks to ensure expected calls were made
    mock_read_excel.assert_called_once_with(f"./data/{MOCK_EXCEL_FILENAME}", sheet_name=MOCK_SHEET_NAME, header=MOCK_HEADER)
    
    # Update the path to match the actual output path
    expected_csv_path = os.path.join(output_path, f"{expected_csv_filename}.csv")
    mock_to_csv.assert_called_once_with(expected_csv_path, index=True)
