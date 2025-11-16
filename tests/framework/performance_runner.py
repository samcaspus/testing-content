"""
Performance test runner for Cloud Storage Tiering System.
This runner extends the base test runner with performance-specific capabilities.
"""
import yaml
import io
import uuid
import time
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Optional
from pathlib import Path
from fastapi.testclient import TestClient
from tests.framework.test_runner import TestRunner


class PerformanceRunner(TestRunner):
    """Performance test runner that extends TestRunner with performance metrics."""
    
    def __init__(self, config_path: str, client: TestClient):
        """Initialize performance runner."""
        super().__init__(config_path, client)
        self.performance_thresholds = self.config.get('performance_thresholds', {})
        self.metrics = {
            'response_times': [],
            'throughput': [],
            'error_count': 0,
            'total_requests': 0,
            'latencies': {}
        }
    
    def _get_threshold(self, threshold_path: str) -> Any:
        """Get threshold value from config using dot notation."""
        parts = threshold_path.split('.')
        value = self.performance_thresholds
        for i, part in enumerate(parts):
            if isinstance(value, dict):
                value = value.get(part)
                if value is None:
                    return None
            else:
                # If we've reached a non-dict value before the end, return None
                return None
        return value
    
    def execute_test_case(self, test_case: Dict) -> Dict:
        """Execute a performance test case with metrics collection."""
        # Reset metrics for each test
        self.metrics = {
            'response_times': [],
            'throughput': [],
            'error_count': 0,
            'total_requests': 0,
            'latencies': {}
        }
        
        result = super().execute_test_case(test_case)
        
        # Add performance metrics to result
        result['metrics'] = self.metrics.copy()
        
        return result
    
    def _execute_action(self, action: str, step: Dict) -> Dict:
        """Execute a test action, with performance-specific actions."""
        # Handle performance-specific actions first
        if action == 'prepare_files':
            return self._action_prepare_files(step)
        elif action == 'warmup_system':
            return self._action_warmup_system(step)
        elif action == 'sequential_upload':
            return self._action_sequential_upload(step)
        elif action == 'upload_with_timing':
            return self._action_upload_with_timing(step)
        elif action == 'download_with_timing':
            return self._action_download_with_timing(step)
        elif action == 'calculate_statistics':
            return self._action_calculate_statistics(step)
        elif action == 'calculate_throughput':
            return self._action_calculate_throughput(step)
        elif action == 'verify_performance':
            return self._action_verify_performance(step)
        elif action == 'verify_download_performance':
            return self._action_verify_download_performance(step)
        elif action == 'seed_files_for_tiering':
            return self._action_seed_files_for_tiering(step)
        elif action == 'run_tiering_with_timing':
            return self._action_run_tiering_with_timing(step)
        elif action == 'prepare_concurrent_workload':
            return self._action_prepare_concurrent_workload(step)
        elif action == 'run_concurrent_workload':
            return self._action_run_concurrent_workload(step)
        elif action == 'calculate_error_rate':
            return self._action_calculate_error_rate(step)
        elif action == 'calculate_latency_metrics':
            return self._action_calculate_latency_metrics(step)
        elif action == 'verify_concurrent_performance':
            return self._action_verify_concurrent_performance(step)
        elif action == 'seed_files':
            return self._action_seed_files(step)
        elif action == 'get_stats_with_timing':
            return self._action_get_stats_with_timing(step)
        elif action == 'prepare_file':
            # Alias for create_file but store file_id by size
            result = self._action_create_file(step)
            if result.get('success'):
                size = step.get('size', 'sample')
                filename = step.get('filename', 'test.bin')
                # Store filename mapping
                self.context[f'{size}_filename'] = filename
            return result
        else:
            # Fall back to parent class actions
            return super()._execute_action(action, step)
    
    def _action_prepare_files(self, step: Dict) -> Dict:
        """Prepare multiple files for testing."""
        count = step.get('count', 10)
        size = self._get_file_size(step.get('size', 'sample'))
        files = []
        
        for i in range(count):
            content = self._generate_file_content(size)
            files.append({
                'content': content,
                'filename': f"perf_file_{i}.bin",
                'size': size
            })
        
        self.context['prepared_files'] = files
        return {'success': True, 'count': len(files)}
    
    def _action_warmup_system(self, step: Dict) -> Dict:
        """Warm up the system with a few requests."""
        # Perform a few warmup requests
        warmup_count = step.get('count', 3)
        for _ in range(warmup_count):
            content = self._generate_file_content(self._get_file_size('sample'))
            self.client.post(
                "/files",
                files={"file": ("warmup.bin", io.BytesIO(content), 'application/octet-stream')}
            )
        return {'success': True}
    
    def _action_sequential_upload(self, step: Dict) -> Dict:
        """Upload files sequentially and record metrics."""
        count = step.get('count', 10)
        size = self._get_file_size(step.get('size', 'sample'))
        file_ids = []
        response_times = []
        
        for i in range(count):
            filename = f"seq_upload_{i}.bin"
            content = self._generate_file_content(size)
            
            start_time = time.time()
            response = self.client.post(
                "/files",
                files={"file": (filename, io.BytesIO(content), 'application/octet-stream')}
            )
            end_time = time.time()
            
            elapsed_ms = (end_time - start_time) * 1000
            response_times.append(elapsed_ms)
            
            if response.status_code == 201:
                file_ids.append(response.json().get('file_id'))
                self.metrics['total_requests'] += 1
            else:
                self.metrics['error_count'] += 1
            
            self.metrics['response_times'].extend(response_times)
        
        self.context['sequential_upload_file_ids'] = file_ids
        return {'success': True, 'response_times': response_times}
    
    def _action_upload_with_timing(self, step: Dict) -> Dict:
        """Upload a file and record timing metrics."""
        size = self._get_file_size(step.get('size', 'sample'))
        filename = step.get('filename', 'large_file.bin')
        content = self._generate_file_content(size)
        
        start_time = time.time()
        response = self.client.post(
            "/files",
            files={"file": (filename, io.BytesIO(content), 'application/octet-stream')}
        )
        end_time = time.time()
        
        elapsed_seconds = end_time - start_time
        elapsed_ms = elapsed_seconds * 1000
        
        if response.status_code == 201:
            file_id = response.json().get('file_id')
            self.context['last_file_id'] = file_id
            
            # Calculate throughput in MB/s
            size_mb = size / (1024 * 1024)
            throughput_mbps = size_mb / elapsed_seconds if elapsed_seconds > 0 else 0
            
            self.metrics['response_times'].append(elapsed_ms)
            self.metrics['throughput'].append(throughput_mbps)
            self.context['upload_time'] = elapsed_seconds
            self.context['upload_throughput'] = throughput_mbps
            
            return {
                'success': True,
                'elapsed_seconds': elapsed_seconds,
                'throughput_mbps': throughput_mbps
            }
        else:
            self.metrics['error_count'] += 1
            return {'success': False, 'error': f'Upload failed with status {response.status_code}'}
    
    def _action_download_with_timing(self, step: Dict) -> Dict:
        """Download a file and record timing metrics."""
        file_size = step.get('file_size', 'small')
        file_id = self.context.get(f'{file_size}_file_id')
        
        if not file_id:
            # Try to find file by size
            file_id = self.context.get('last_file_id')
        
        if not file_id:
            return {'success': False, 'error': 'No file_id available'}
        
        start_time = time.time()
        response = self.client.get(f"/files/{file_id}")
        end_time = time.time()
        
        elapsed_ms = (end_time - start_time) * 1000
        
        if response.status_code == 200:
            size_bytes = len(response.content)
            size_mb = size_bytes / (1024 * 1024)
            throughput_mbps = size_mb / (elapsed_ms / 1000) if elapsed_ms > 0 else 0
            
            self.metrics['response_times'].append(elapsed_ms)
            self.metrics['throughput'].append(throughput_mbps)
            
            # Store metrics by file size
            if file_size not in self.metrics['latencies']:
                self.metrics['latencies'][file_size] = []
            self.metrics['latencies'][file_size].append(elapsed_ms)
            
            return {
                'success': True,
                'elapsed_ms': elapsed_ms,
                'throughput_mbps': throughput_mbps
            }
        else:
            self.metrics['error_count'] += 1
            return {'success': False, 'error': f'Download failed with status {response.status_code}'}
    
    def _action_calculate_statistics(self, step: Dict) -> Dict:
        """Calculate statistics from collected metrics."""
        response_times = self.metrics.get('response_times', [])
        
        if not response_times:
            return {'success': False, 'error': 'No response times collected'}
        
        stats = {
            'average': statistics.mean(response_times),
            'min': min(response_times),
            'max': max(response_times),
            'median': statistics.median(response_times),
            'count': len(response_times)
        }
        
        if len(response_times) > 1:
            stats['stdev'] = statistics.stdev(response_times)
        
        self.context['statistics'] = stats
        return {'success': True, 'statistics': stats}
    
    def _action_calculate_throughput(self, step: Dict) -> Dict:
        """Calculate throughput metrics."""
        throughput_values = self.metrics.get('throughput', [])
        
        if not throughput_values:
            return {'success': False, 'error': 'No throughput data collected'}
        
        avg_throughput = statistics.mean(throughput_values)
        self.context['average_throughput'] = avg_throughput
        
        return {'success': True, 'average_throughput_mbps': avg_throughput}
    
    def _action_verify_performance(self, step: Dict) -> Dict:
        """Verify performance meets threshold."""
        threshold_path = step.get('threshold')
        operator = step.get('operator', '<')
        
        if not threshold_path:
            return {'success': False, 'error': 'No threshold specified'}
        
        threshold_value = self._get_threshold(threshold_path)
        
        if threshold_value is None:
            return {'success': False, 'error': f'Threshold not found: {threshold_path}'}
        
        # Get the value to compare
        if 'statistics' in self.context:
            # Compare against statistics
            stats = self.context['statistics']
            actual_value = stats.get('average', 0)
        elif 'upload_throughput' in self.context:
            actual_value = self.context['upload_throughput']
        elif 'upload_time' in self.context:
            actual_value = self.context['upload_time'] * 1000  # Convert to ms
        else:
            return {'success': False, 'error': 'No value to compare against threshold'}
        
        # Apply operator
        if operator == '<':
            passed = actual_value < threshold_value
        elif operator == '<=':
            passed = actual_value <= threshold_value
        elif operator == '>':
            passed = actual_value > threshold_value
        elif operator == '>=':
            passed = actual_value >= threshold_value
        elif operator == '==':
            passed = actual_value == threshold_value
        else:
            return {'success': False, 'error': f'Unknown operator: {operator}'}
        
        if not passed:
            return {
                'success': False,
                'error': f'Performance threshold not met: {actual_value} {operator} {threshold_value}'
            }
        
        return {'success': True}
    
    def _action_verify_download_performance(self, step: Dict) -> Dict:
        """Verify download performance for different file sizes."""
        thresholds = step.get('thresholds', [])
        
        for threshold_spec in thresholds:
            file_size = threshold_spec.get('size')
            threshold_path = threshold_spec.get('threshold')
            
            if file_size not in self.metrics.get('latencies', {}):
                return {'success': False, 'error': f'No metrics for file size: {file_size}'}
            
            latencies = self.metrics['latencies'][file_size]
            max_latency = max(latencies)
            threshold_value = self._get_threshold(threshold_path)
            
            if max_latency > threshold_value:
                return {
                    'success': False,
                    'error': f'Download performance failed for {file_size}: {max_latency}ms > {threshold_value}ms'
                }
        
        return {'success': True}
    
    def _action_seed_files_for_tiering(self, step: Dict) -> Dict:
        """Seed files with varied last_accessed dates for tiering tests."""
        count = step.get('count', 1000)
        file_ids = []
        
        # Create files with dates spread across different ranges
        for i in range(count):
            filename = f"tiering_file_{i}.bin"
            content = self._generate_file_content(self._get_file_size('sample'))
            
            response = self.client.post(
                "/files",
                files={"file": (filename, io.BytesIO(content), 'application/octet-stream')}
            )
            
            if response.status_code == 201:
                file_id = response.json().get('file_id')
                file_ids.append(file_id)
                
                # Spread dates: some recent, some old
                if i % 3 == 0:
                    days_ago = 10  # Recent
                elif i % 3 == 1:
                    days_ago = 50  # Medium
                else:
                    days_ago = 100  # Old
                
                self.client.post(
                    f"/admin/files/{file_id}/update-last-accessed",
                    json={"days_ago": days_ago}
                )
        
        self.context['tiering_file_ids'] = file_ids
        return {'success': True, 'count': len(file_ids)}
    
    def _action_run_tiering_with_timing(self, step: Dict) -> Dict:
        """Run tiering process and measure execution time."""
        start_time = time.time()
        response = self.client.post("/admin/tiering/run")
        end_time = time.time()
        
        elapsed_seconds = end_time - start_time
        
        if response.status_code != 200:
            return {'success': False, 'error': f'Tiering failed with status {response.status_code}'}
        
        self.context['tiering_time'] = elapsed_seconds
        self.metrics['response_times'].append(elapsed_seconds * 1000)
        
        return {
            'success': True,
            'elapsed_seconds': elapsed_seconds,
            'response': response.json()
        }
    
    def _action_prepare_concurrent_workload(self, step: Dict) -> Dict:
        """Prepare workload for concurrent operations."""
        upload_count = step.get('uploads', 50)
        download_count = step.get('downloads', 50)
        tiering_count = step.get('tiering', 10)
        
        # Upload files for download tests
        download_file_ids = []
        for i in range(download_count):
            filename = f"concurrent_dl_{i}.bin"
            content = self._generate_file_content(self._get_file_size('sample'))
            
            response = self.client.post(
                "/files",
                files={"file": (filename, io.BytesIO(content), 'application/octet-stream')}
            )
            
            if response.status_code == 201:
                download_file_ids.append(response.json().get('file_id'))
        
        self.context['concurrent_upload_count'] = upload_count
        self.context['concurrent_download_file_ids'] = download_file_ids
        self.context['concurrent_tiering_count'] = tiering_count
        
        return {'success': True}
    
    def _action_run_concurrent_workload(self, step: Dict) -> Dict:
        """Run concurrent workload and collect metrics."""
        upload_count = self.context.get('concurrent_upload_count', 0)
        download_file_ids = self.context.get('concurrent_download_file_ids', [])
        tiering_count = self.context.get('concurrent_tiering_count', 0)
        
        all_latencies = []
        error_count = 0
        total_requests = 0
        
        def upload_worker(i):
            filename = f"concurrent_up_{i}.bin"
            content = self._generate_file_content(self._get_file_size('sample'))
            start = time.time()
            response = self.client.post(
                "/files",
                files={"file": (filename, io.BytesIO(content), 'application/octet-stream')}
            )
            elapsed = (time.time() - start) * 1000
            return {'type': 'upload', 'status': response.status_code, 'latency': elapsed}
        
        def download_worker(file_id):
            start = time.time()
            response = self.client.get(f"/files/{file_id}")
            elapsed = (time.time() - start) * 1000
            return {'type': 'download', 'status': response.status_code, 'latency': elapsed}
        
        def tiering_worker():
            start = time.time()
            response = self.client.post("/admin/tiering/run")
            elapsed = (time.time() - start) * 1000
            return {'type': 'tiering', 'status': response.status_code, 'latency': elapsed}
        
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = []
            
            # Submit upload tasks
            for i in range(upload_count):
                futures.append(executor.submit(upload_worker, i))
            
            # Submit download tasks
            for file_id in download_file_ids:
                futures.append(executor.submit(download_worker, file_id))
            
            # Submit tiering tasks
            for _ in range(tiering_count):
                futures.append(executor.submit(tiering_worker))
            
            # Collect results
            for future in as_completed(futures):
                result = future.result()
                total_requests += 1
                all_latencies.append(result['latency'])
                
                if result['status'] >= 400:
                    error_count += 1
        
        self.metrics['response_times'] = all_latencies
        self.metrics['error_count'] = error_count
        self.metrics['total_requests'] = total_requests
        
        return {'success': True}
    
    def _action_calculate_error_rate(self, step: Dict) -> Dict:
        """Calculate error rate percentage."""
        error_count = self.metrics.get('error_count', 0)
        total_requests = self.metrics.get('total_requests', 0)
        
        if total_requests == 0:
            return {'success': False, 'error': 'No requests recorded'}
        
        error_rate = (error_count / total_requests) * 100
        self.context['error_rate'] = error_rate
        
        return {'success': True, 'error_rate_percent': error_rate}
    
    def _action_calculate_latency_metrics(self, step: Dict) -> Dict:
        """Calculate latency percentiles."""
        latencies = sorted(self.metrics.get('response_times', []))
        
        if not latencies:
            return {'success': False, 'error': 'No latency data collected'}
        
        percentiles = step.get('percentiles', [50, 90, 95, 99])
        percentile_values = {}
        
        for p in percentiles:
            index = int((p / 100) * len(latencies))
            if index >= len(latencies):
                index = len(latencies) - 1
            percentile_values[f'p{p}'] = latencies[index]
        
        self.context['latency_percentiles'] = percentile_values
        return {'success': True, 'percentiles': percentile_values}
    
    def _action_verify_concurrent_performance(self, step: Dict) -> Dict:
        """Verify concurrent performance meets thresholds."""
        error_rate = self.context.get('error_rate', 0)
        error_threshold_path = step.get('error_rate_threshold')
        latency_threshold_path = step.get('latency_threshold')
        
        # Check error rate
        if error_threshold_path:
            error_threshold = self._get_threshold(error_threshold_path)
            if error_rate > error_threshold:
                return {
                    'success': False,
                    'error': f'Error rate {error_rate}% exceeds threshold {error_threshold}%'
                }
        
        # Check latency
        if latency_threshold_path:
            latency_percentiles = self.context.get('latency_percentiles', {})
            p99_latency = latency_percentiles.get('p99', 0)
            latency_threshold = self._get_threshold(latency_threshold_path)
            
            if p99_latency > latency_threshold:
                return {
                    'success': False,
                    'error': f'P99 latency {p99_latency}ms exceeds threshold {latency_threshold}ms'
                }
        
        return {'success': True}
    
    def _action_seed_files(self, step: Dict) -> Dict:
        """Seed system with large number of files."""
        count = step.get('count', 10000)
        file_ids = []
        
        for i in range(count):
            filename = f"stats_file_{i}.bin"
            content = self._generate_file_content(self._get_file_size('sample'))
            
            response = self.client.post(
                "/files",
                files={"file": (filename, io.BytesIO(content), 'application/octet-stream')}
            )
            
            if response.status_code == 201:
                file_ids.append(response.json().get('file_id'))
        
        self.context['seeded_file_ids'] = file_ids
        return {'success': True, 'count': len(file_ids)}
    
    def _action_get_stats_with_timing(self, step: Dict) -> Dict:
        """Get stats and measure response time."""
        start_time = time.time()
        response = self.client.get("/admin/stats")
        end_time = time.time()
        
        elapsed_ms = (end_time - start_time) * 1000
        
        if response.status_code != 200:
            return {'success': False, 'error': f'Stats request failed with status {response.status_code}'}
        
        self.context['stats_time'] = elapsed_ms
        self.metrics['response_times'].append(elapsed_ms)
        
        return {
            'success': True,
            'elapsed_ms': elapsed_ms,
            'stats': response.json()
        }

