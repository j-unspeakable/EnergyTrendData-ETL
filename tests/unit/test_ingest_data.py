import os
import pytest
from unittest import mock
from energytrend_etl.ingest_data import ingest_excel_files


# Test data for the HTML page content with Excel links
HTML_CONTENT = """
<html>
<head><title>Test</title></head>
<body>
<a href="test_file.xlsx">Test File</a>
<a href="another_test_file.xls">Another Test File</a>
</body>
</html>
"""


@pytest.fixture
def mock_environment(monkeypatch):
    """Fixture to mock environment setup and external dependencies."""
    # Mocking os.path.exists to always return False to simulate non-existence of the file
    monkeypatch.setattr(os.path, 'exists', mock.Mock(return_value=False))

    # Patching os.makedirs to avoid actual directory creation
    monkeypatch.setattr(os, 'makedirs', mock.Mock())

    # Mocking requests.get to return a mock response
    response_mock = mock.Mock(status_code=200, headers={'Last-Modified': 'Wed, 21 Oct 2015 07:28:00 GMT'})
    response_mock.raise_for_status = mock.Mock()
    response_mock.content = HTML_CONTENT.encode('utf-8')
    monkeypatch.setattr('energytrend_etl.ingest_data.requests.get', mock.Mock(return_value=response_mock))

    # Mocking requests.head to simulate the 'Last-Modified' header in responses
    monkeypatch.setattr('energytrend_etl.ingest_data.requests.head', mock.Mock(return_value=response_mock))


@pytest.mark.usefixtures("mock_environment")
def test_ingest_excel_files():
    """Test that the function correctly downloads a new file when needed."""
    url = "http://example.com"
    html_name = "Test File"

    expected_file_name = "test_file.xlsx"
    expected_save_path = "./data/test_file.xlsx"

    with mock.patch('energytrend_etl.ingest_data.download_file') as mock_download:
        result = ingest_excel_files.fn(url, html_name)  # Use .fn to call directly without Prefect orchestration
        
        mock_download.assert_called_once_with("http://example.com/test_file.xlsx", expected_save_path)
        assert result == expected_file_name, "Filename should match the expected output."


@pytest.mark.usefixtures("mock_environment")
def test_ingest_excel_files_no_file_found():
    """Test that the function returns an empty string if no matching file is found."""
    url = "http://example.com"
    html_name = "Missing File"

    result = ingest_excel_files.fn(url, html_name)  # Use .fn here too
    
    assert result == "", "Should return an empty string when no Excel file is found."


@pytest.mark.usefixtures("mock_environment")
def test_ingest_excel_files_file_up_to_date(monkeypatch):
    """Test that the function does not download a file if it's up-to-date."""
    url = "http://example.com"
    html_name = "Test File"

    # Mocking os.path.exists to return True to simulate existence of the file
    monkeypatch.setattr(os.path, 'exists', mock.Mock(return_value=True))
    
    # Mocking os.path.getmtime to return a more recent modification time
    monkeypatch.setattr(os.path, 'getmtime', mock.Mock(return_value=2000000000))
    
    # Mocking requests.get to return the HTML content that includes the file link
    response_mock_get = mock.Mock(status_code=200)
    response_mock_get.content = HTML_CONTENT.encode('utf-8')
    monkeypatch.setattr('energytrend_etl.ingest_data.requests.get', mock.Mock(return_value=response_mock_get))

    # Mocking requests.head to return an older Last-Modified time
    response_mock_head = mock.Mock(status_code=200, headers={'Last-Modified': 'Wed, 21 Oct 2015 07:28:00 GMT'})
    response_mock_head.raise_for_status = mock.Mock()
    monkeypatch.setattr('energytrend_etl.ingest_data.requests.head', mock.Mock(return_value=response_mock_head))

    with mock.patch('energytrend_etl.ingest_data.download_file') as mock_download:
        result = ingest_excel_files.fn(url, html_name)  # Use .fn here as well
        
        # Assertions to verify that no download occurred
        mock_download.assert_not_called()
        assert result == "test_file.xlsx", "Should return the filename since it's already up-to-date."
