"""
Security tests for Cloud Storage Tiering System.
These tests are driven by security_test_config.yaml.
"""
import pytest
from pathlib import Path
from fastapi.testclient import TestClient
from src.storage_service import app, files_metadata, files_content
from tests.framework.security_runner import SecurityRunner


# Get the path to security test config
CONFIG_PATH = Path(__file__).parent.parent / "config" / "security_test_config.yaml"


@pytest.fixture(scope="function")
def security_runner(client):
    """Create a security test runner instance."""
    return SecurityRunner(str(CONFIG_PATH), client)


def load_test_cases_from_config(config_path, category):
    """Load test cases for a specific category from config."""
    import yaml
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    test_cases = config.get('test_cases', {}).get(category, [])
    return test_cases


class TestInputValidation:
    """Security tests for input validation."""
    
    @pytest.mark.parametrize("test_case",
        load_test_cases_from_config(CONFIG_PATH, 'input_validation'),
        ids=lambda tc: tc.get('id', 'UNKNOWN'))
    def test_input_validation(self, security_runner, test_case):
        """Execute input validation security test case from config."""
        result = security_runner.execute_test_case(test_case)
        assert result['status'] == 'PASSED', \
            f"Test {result['test_id']} failed: {', '.join(result.get('errors', []))}"
        
        # Log security results
        if 'security_results' in result:
            security_results = result['security_results']
            print(f"\nSecurity Results for {result['test_id']}:")
            if security_results.get('server_errors'):
                print(f"  Server Errors: {len(security_results['server_errors'])}")
            if security_results.get('stack_traces'):
                print(f"  Stack Traces Exposed: {len(security_results['stack_traces'])}")
            if security_results.get('rejected_requests'):
                print(f"  Rejected Requests: {len(security_results['rejected_requests'])}")


class TestContentValidation:
    """Security tests for content validation."""
    
    @pytest.mark.parametrize("test_case",
        load_test_cases_from_config(CONFIG_PATH, 'content_validation'),
        ids=lambda tc: tc.get('id', 'UNKNOWN'))
    def test_content_validation(self, security_runner, test_case):
        """Execute content validation security test case from config."""
        result = security_runner.execute_test_case(test_case)
        assert result['status'] == 'PASSED', \
            f"Test {result['test_id']} failed: {', '.join(result.get('errors', []))}"
        
        # Log security results
        if 'security_results' in result:
            security_results = result['security_results']
            print(f"\nContent Validation Results for {result['test_id']}:")
            if security_results.get('corruption_detected'):
                print("  WARNING: Data corruption detected!")


class TestConcurrencySecurity:
    """Security tests for concurrent access."""
    
    @pytest.mark.parametrize("test_case",
        load_test_cases_from_config(CONFIG_PATH, 'concurrency_security'),
        ids=lambda tc: tc.get('id', 'UNKNOWN'))
    def test_concurrency_security(self, security_runner, test_case):
        """Execute concurrency security test case from config."""
        result = security_runner.execute_test_case(test_case)
        assert result['status'] == 'PASSED', \
            f"Test {result['test_id']} failed: {', '.join(result.get('errors', []))}"
        
        # Log security results
        if 'security_results' in result:
            security_results = result['security_results']
            print(f"\nConcurrency Security Results for {result['test_id']}:")
            if security_results.get('corruption_detected'):
                print("  WARNING: Data corruption detected!")
            concurrent_results = security_runner.context.get('concurrent_security_results', [])
            if concurrent_results:
                print(f"  Concurrent Operations: {len(concurrent_results)}")
                success_count = sum(1 for r in concurrent_results if r.get('success'))
                print(f"  Successful: {success_count}/{len(concurrent_results)}")

