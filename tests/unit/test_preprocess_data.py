import pytest
import pandas as pd
from energytrend_etl.preprocess_data import process_excel_data


# Test Data for DataFrame
MOCK_DF_DATA_SUCCESS = {
    'Column1': ['A', 'B', 'C', 'D', 'E'],
    'Value1': [1, 2, 3, 4, 5],
    'Value2': [6, 7, 8, 9, 10]
}

MOCK_DF_DATA_MISSING_COLUMN = {
    'Value1': [1, 2, 3],
    'Value2': [4, 5, 6]
}

MOCK_DF_DATA_INSUFFICIENT_ROWS = {
    'Column1': ['A'],
    'Value1': [1],
    'Value2': [4]
}

MOCK_DF_DATA_TOO_MANY_MISSING_VALUES = {
    'Column1': ['A', 'B', None],
    'Value1': [1, None, None],
    'Value2': [None, None, 6]
}


@pytest.fixture
def mock_read_excel(monkeypatch):
    """Fixture to mock pandas read_excel function with various scenarios."""
    def mock_read_excel_function(file, sheet_name=None, header=0):
        if "success" in file:
            return pd.DataFrame(MOCK_DF_DATA_SUCCESS)
        elif "missing_column" in file:
            return pd.DataFrame(MOCK_DF_DATA_MISSING_COLUMN)
        elif "insufficient_rows" in file:
            return pd.DataFrame(MOCK_DF_DATA_INSUFFICIENT_ROWS)
        elif "too_many_missing" in file:
            return pd.DataFrame(MOCK_DF_DATA_TOO_MANY_MISSING_VALUES)
        else:
            return pd.DataFrame()  # Return empty DataFrame for unknown cases

    monkeypatch.setattr('energytrend_etl.preprocess_data.pd.read_excel', mock_read_excel_function)


@pytest.mark.usefixtures("mock_read_excel")
def test_process_excel_data_success():
    """Test successful preprocessing of Excel data."""
    # Test the function
    result = process_excel_data.fn('mockfile_success.xlsx', 'Sheet1', 0)
    
    # Expected transformations
    assert not result.empty, "DataFrame should not be empty after processing."
    assert 'processed_date' in result.columns, "DataFrame should have a 'processed_date' column."
    assert 'filename' in result.columns, "DataFrame should have a 'filename' column."
    assert result.index.name is None, "Index should have no name after processing."


@pytest.mark.usefixtures("mock_read_excel")
def test_process_excel_data_missing_key_column():
    """Test preprocessing when key column is missing."""
    # Test the function
    result = process_excel_data.fn('mockfile_missing_column.xlsx', 'Sheet1', 0)
    
    # Verify it returns an empty DataFrame due to missing key column
    assert result.empty, "DataFrame should be empty if 'Column1' is missing."


@pytest.mark.usefixtures("mock_read_excel")
def test_process_excel_data_minimum_row_check():
    """Test preprocessing when DataFrame has fewer rows than required."""
    # Test the function
    result = process_excel_data.fn('mockfile_insufficient_rows.xlsx', 'Sheet1', 0, min_rows=2)
    
    # Verify it returns an empty DataFrame due to insufficient rows
    assert result.empty, "DataFrame should be empty if it has fewer rows than required."


@pytest.mark.usefixtures("mock_read_excel")
def test_process_excel_data_max_missing_percentage():
    """Test preprocessing when DataFrame has too many missing values."""
    # Test the function
    result = process_excel_data.fn('mockfile_too_many_missing.xlsx', 'Sheet1', 0, max_missing_percentage=10.0)
    
    # Verify it returns an empty DataFrame due to too many missing values
    assert result.empty, "DataFrame should be empty if it has too many missing values."
