"""
Performance tests for Cloud Storage Tiering System.
These tests are driven by performance_test_config.yaml.
"""
import pytest
from pathlib import Path
from fastapi.testclient import TestClient
from src.storage_service import app, files_metadata, files_content
from tests.framework.performance_runner import PerformanceRunner


# Get the path to performance test config
CONFIG_PATH = Path(__file__).parent.parent / "config" / "performance_test_config.yaml"



@pytest.fixture(scope="function")
def performance_runner(client):
    """Create a performance test runner instance."""
    return PerformanceRunner(str(CONFIG_PATH), client)


def load_test_cases_from_config(config_path, category):
    """Load test cases for a specific category from config."""
    import yaml
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    test_cases = config.get('test_cases', {}).get(category, [])
    return test_cases


class TestUploadPerformance:
    """Performance tests for file upload operations."""
    
    @pytest.mark.parametrize("test_case",
        load_test_cases_from_config(CONFIG_PATH, 'upload_performance'),
        ids=lambda tc: tc.get('id', 'UNKNOWN'))
    def test_upload_performance(self, performance_runner, test_case):
        """Execute upload performance test case from config."""
        result = performance_runner.execute_test_case(test_case)
        assert result['status'] == 'PASSED', \
            f"Test {result['test_id']} failed: {', '.join(result.get('errors', []))}"
        
        # Log performance metrics
        if 'metrics' in result:
            metrics = result['metrics']
            print(f"\nPerformance Metrics for {result['test_id']}:")
            if 'statistics' in performance_runner.context:
                stats = performance_runner.context['statistics']
                print(f"  Average: {stats.get('average', 0):.2f}ms")
                print(f"  Min: {stats.get('min', 0):.2f}ms")
                print(f"  Max: {stats.get('max', 0):.2f}ms")
            if 'average_throughput' in performance_runner.context:
                print(f"  Throughput: {performance_runner.context['average_throughput']:.2f} MB/s")


class TestDownloadPerformance:
    """Performance tests for file download operations."""
    
    @pytest.mark.parametrize("test_case",
        load_test_cases_from_config(CONFIG_PATH, 'download_performance'),
        ids=lambda tc: tc.get('id', 'UNKNOWN'))
    def test_download_performance(self, performance_runner, test_case):
        """Execute download performance test case from config."""
        result = performance_runner.execute_test_case(test_case)
        assert result['status'] == 'PASSED', \
            f"Test {result['test_id']} failed: {', '.join(result.get('errors', []))}"
        
        # Log performance metrics
        if 'metrics' in result and 'latencies' in result['metrics']:
            print(f"\nDownload Performance Metrics for {result['test_id']}:")
            for size, latencies in result['metrics']['latencies'].items():
                if latencies:
                    print(f"  {size}: {max(latencies):.2f}ms (max)")


class TestTieringPerformance:
    """Performance tests for tiering operations."""
    
    @pytest.mark.parametrize("test_case",
        load_test_cases_from_config(CONFIG_PATH, 'tiering_performance'),
        ids=lambda tc: tc.get('id', 'UNKNOWN'))
    def test_tiering_performance(self, performance_runner, test_case):
        """Execute tiering performance test case from config."""
        result = performance_runner.execute_test_case(test_case)
        assert result['status'] == 'PASSED', \
            f"Test {result['test_id']} failed: {', '.join(result.get('errors', []))}"
        
        # Log performance metrics
        if 'tiering_time' in performance_runner.context:
            print(f"\nTiering Performance for {result['test_id']}:")
            print(f"  Time: {performance_runner.context['tiering_time']:.2f}s")


class TestConcurrentPerformance:
    """Performance tests for concurrent operations."""
    
    @pytest.mark.parametrize("test_case",
        load_test_cases_from_config(CONFIG_PATH, 'concurrent_performance'),
        ids=lambda tc: tc.get('id', 'UNKNOWN'))
    def test_concurrent_performance(self, performance_runner, test_case):
        """Execute concurrent performance test case from config."""
        result = performance_runner.execute_test_case(test_case)
        assert result['status'] == 'PASSED', \
            f"Test {result['test_id']} failed: {', '.join(result.get('errors', []))}"
        
        # Log performance metrics
        if 'error_rate' in performance_runner.context:
            print(f"\nConcurrent Performance Metrics for {result['test_id']}:")
            print(f"  Error Rate: {performance_runner.context['error_rate']:.2f}%")
        if 'latency_percentiles' in performance_runner.context:
            percentiles = performance_runner.context['latency_percentiles']
            print(f"  P99 Latency: {percentiles.get('p99', 0):.2f}ms")


class TestStatsPerformance:
    """Performance tests for stats calculation."""
    
    @pytest.mark.parametrize("test_case",
        load_test_cases_from_config(CONFIG_PATH, 'stats_performance'),
        ids=lambda tc: tc.get('id', 'UNKNOWN'))
    def test_stats_performance(self, performance_runner, test_case):
        """Execute stats performance test case from config."""
        result = performance_runner.execute_test_case(test_case)
        assert result['status'] == 'PASSED', \
            f"Test {result['test_id']} failed: {', '.join(result.get('errors', []))}"
        
        # Log performance metrics
        if 'stats_time' in performance_runner.context:
            print(f"\nStats Performance for {result['test_id']}:")
            print(f"  Response Time: {performance_runner.context['stats_time']:.2f}ms")

