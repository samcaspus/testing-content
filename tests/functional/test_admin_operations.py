"""
Functional tests for admin operations (tiering, stats).
These tests are driven by test_config.yaml.
"""
import pytest
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


class TestTiering:
    """Test cases for tiering operations."""
    
    @pytest.mark.parametrize("test_case",
        load_test_cases_from_config(CONFIG_PATH, 'tiering'),
        ids=lambda tc: tc.get('id', 'UNKNOWN'))
    def test_tiering(self, test_runner, test_case):
        """Execute tiering test case from config."""
        result = test_runner.execute_test_case(test_case)
        assert result['status'] == 'PASSED', \
            f"Test {result['test_id']} failed: {', '.join(result.get('errors', []))}"


class TestAdminStats:
    """Test cases for admin statistics."""
    
    @pytest.mark.parametrize("test_case",
        load_test_cases_from_config(CONFIG_PATH, 'admin_stats'),
        ids=lambda tc: tc.get('id', 'UNKNOWN'))
    def test_admin_stats(self, test_runner, test_case):
        """Execute admin stats test case from config."""
        result = test_runner.execute_test_case(test_case)
        assert result['status'] == 'PASSED', \
            f"Test {result['test_id']} failed: {', '.join(result.get('errors', []))}"

