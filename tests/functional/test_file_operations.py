"""
Functional tests for file operations (upload, download, metadata, delete).
These tests are driven by test_config.yaml.
"""
import pytest
import os
from pathlib import Path
from fastapi.testclient import TestClient
from src.storage_service import app, files_metadata, files_content
from tests.framework.test_runner import TestRunner


# Get the path to test config
CONFIG_PATH = Path(__file__).parent.parent / "config" / "functional_test_config.yaml"


@pytest.fixture(scope="function")
def test_runner(client):
    """Create a test runner instance."""
    return TestRunner(str(CONFIG_PATH), client)


def load_test_cases_from_config(config_path, category):
    """Load test cases for a specific category from config."""
    import yaml
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    test_cases = config.get('test_cases', {}).get(category, [])
    return test_cases


class TestFileUpload:
    """Test cases for file upload operations."""
    
    @pytest.mark.parametrize("test_case", 
        load_test_cases_from_config(CONFIG_PATH, 'file_upload'),
        ids=lambda tc: tc.get('id', 'UNKNOWN'))
    def test_file_upload(self, test_runner, test_case):
        """Execute file upload test case from config."""
        result = test_runner.execute_test_case(test_case)
        assert result['status'] == 'PASSED', \
            f"Test {result['test_id']} failed: {', '.join(result.get('errors', []))}"


class TestFileDownload:
    """Test cases for file download operations."""
    
    @pytest.mark.parametrize("test_case",
        load_test_cases_from_config(CONFIG_PATH, 'file_download'),
        ids=lambda tc: tc.get('id', 'UNKNOWN'))
    def test_file_download(self, test_runner, test_case):
        """Execute file download test case from config."""
        result = test_runner.execute_test_case(test_case)
        assert result['status'] == 'PASSED', \
            f"Test {result['test_id']} failed: {', '.join(result.get('errors', []))}"


class TestFileMetadata:
    """Test cases for file metadata operations."""
    
    @pytest.mark.parametrize("test_case",
        load_test_cases_from_config(CONFIG_PATH, 'file_metadata'),
        ids=lambda tc: tc.get('id', 'UNKNOWN'))
    def test_file_metadata(self, test_runner, test_case):
        """Execute file metadata test case from config."""
        result = test_runner.execute_test_case(test_case)
        assert result['status'] == 'PASSED', \
            f"Test {result['test_id']} failed: {', '.join(result.get('errors', []))}"


class TestFileDelete:
    """Test cases for file delete operations."""
    
    @pytest.mark.parametrize("test_case",
        load_test_cases_from_config(CONFIG_PATH, 'file_delete'),
        ids=lambda tc: tc.get('id', 'UNKNOWN'))
    def test_file_delete(self, test_runner, test_case):
        """Execute file delete test case from config."""
        result = test_runner.execute_test_case(test_case)
        assert result['status'] == 'PASSED', \
            f"Test {result['test_id']} failed: {', '.join(result.get('errors', []))}"

