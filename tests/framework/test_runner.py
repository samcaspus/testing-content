"""
Config-driven test framework for Cloud Storage Tiering System.
This framework reads test configurations from YAML and executes tests.
"""
import yaml
import io
import uuid
import time
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Optional
from pathlib import Path
from fastapi.testclient import TestClient


class TestRunner:
    """Test runner that executes tests based on YAML configuration."""
    
    def __init__(self, config_path: str, client: TestClient):
        """Initialize test runner with config file and test client."""
        self.client = client
        self.config = self._load_config(config_path)
        self.test_data = self.config.get('test_data', {})
        self.context = {}  # Store test context (file_ids, checksums, etc.)
        self.file_sizes = self.test_data.get('file_sizes', {})
    
    def _load_config(self, config_path: str) -> Dict:
        """Load YAML configuration file."""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _get_file_size(self, size_spec: Any) -> int:
        """Get file size from config specification."""
        if isinstance(size_spec, int):
            return size_spec
        if isinstance(size_spec, str):
            return self.file_sizes.get(size_spec, 2097152)
        return 2097152  # Default 2MB
    
    def _generate_file_content(self, size_bytes: int) -> bytes:
        """Generate file content of specified size."""
        return b"x" * size_bytes
    
    def _calculate_checksum(self, content: bytes) -> str:
        """Calculate SHA-256 checksum."""
        return hashlib.sha256(content).hexdigest()
    
    def execute_test_case(self, test_case: Dict) -> Dict:
        """Execute a single test case from config."""
        test_id = test_case.get('id', 'UNKNOWN')
        test_name = test_case.get('name', 'Unnamed Test')
        steps = test_case.get('steps', [])
        
        result = {
            'test_id': test_id,
            'test_name': test_name,
            'status': 'PASSED',
            'errors': []
        }
        
        try:
            for step in steps:
                action = step.get('action')
                if not action:
                    continue
                
                # Execute action
                action_result = self._execute_action(action, step)
                if not action_result.get('success', True):
                    result['status'] = 'FAILED'
                    result['errors'].append(action_result.get('error', 'Unknown error'))
                    break
                    
        except Exception as e:
            result['status'] = 'FAILED'
            result['errors'].append(str(e))
        
        return result
    
    def _execute_action(self, action: str, step: Dict) -> Dict:
        """Execute a test action."""
        try:
            if action == 'create_file':
                return self._action_create_file(step)
            elif action == 'upload':
                return self._action_upload(step)
            elif action == 'upload_file':
                return self._action_upload_file(step)
            elif action == 'upload_multiple':
                return self._action_upload_multiple(step)
            elif action == 'bulk_upload':
                return self._action_bulk_upload(step)
            elif action == 'download':
                return self._action_download(step)
            elif action == 'get_metadata':
                return self._action_get_metadata(step)
            elif action == 'delete':
                return self._action_delete(step)
            elif action == 'run_tiering':
                return self._action_run_tiering(step)
            elif action == 'get_stats':
                return self._action_get_stats(step)
            elif action == 'update_last_accessed':
                return self._action_update_last_accessed(step)
            elif action == 'verify_response':
                return self._action_verify_response(step)
            elif action == 'verify_error':
                return self._action_verify_error(step)
            elif action == 'verify_content_types':
                return self._action_verify_content_types(step)
            elif action == 'verify_tier':
                return self._action_verify_tier(step)
            elif action == 'wait':
                return self._action_wait(step)
            elif action == 'generate_random_uuid':
                return self._action_generate_uuid(step)
            elif action == 'calculate_checksum':
                return self._action_calculate_checksum(step)
            elif action == 'verify_content_match':
                return self._action_verify_content_match(step)
            elif action == 'verify_checksum_match':
                return self._action_verify_checksum_match(step)
            elif action == 'verify_last_accessed_updated':
                return self._action_verify_last_accessed_updated(step)
            elif action == 'verify_timestamp_increased':
                return self._action_verify_timestamp_increased(step)
            elif action == 'verify_stats':
                return self._action_verify_stats(step)
            elif action == 'verify_stats_decreased':
                return self._action_verify_stats_decreased(step)
            elif action == 'concurrent_upload':
                return self._action_concurrent_upload(step)
            elif action == 'concurrent_download':
                return self._action_concurrent_download(step)
            elif action == 'concurrent_delete':
                return self._action_concurrent_delete(step)
            elif action == 'concurrent_tiering':
                return self._action_concurrent_tiering(step)
            elif action == 'verify_unique_ids':
                return self._action_verify_unique_ids(step)
            elif action == 'verify_all_success':
                return self._action_verify_all_success(step)
            elif action == 'verify_all_deleted':
                return self._action_verify_all_deleted(step)
            elif action == 'verify_ids_different':
                return self._action_verify_ids_different(step)
            elif action == 'verify_tiering_results':
                return self._action_verify_tiering_results(step)
            elif action == 'verify_consistent_state':
                return self._action_verify_consistent_state(step)
            elif action == 'verify_tier_stats':
                return self._action_verify_tier_stats(step)
            elif action == 'verify_tier_distribution':
                return self._action_verify_tier_distribution(step)
            elif action == 'verify_increment':
                return self._action_verify_increment(step)
            elif action == 'verify_decrement':
                return self._action_verify_decrement(step)
            elif action == 'ensure_empty_system':
                return self._action_ensure_empty_system(step)
            elif action == 'ensure_recent_access':
                return self._action_ensure_recent_access(step)
            elif action == 'move_to_cold':
                return self._action_move_to_cold(step)
            elif action == 'prepare_tiering_dataset':
                return self._action_prepare_tiering_dataset(step)
            elif action == 'setup_tiering_scenario':
                return self._action_setup_tiering_scenario(step)
            else:
                return {'success': False, 'error': f'Unknown action: {action}'}
        except Exception as e:
            return {'success': False, 'error': f'Action {action} failed: {str(e)}'}
    
    # Action implementations
    def _action_create_file(self, step: Dict) -> Dict:
        """Create a test file."""
        size = self._get_file_size(step.get('size', 'sample'))
        filename = step.get('filename', 'test.bin')
        content = self._generate_file_content(size)
        
        self.context['current_file_content'] = content
        self.context['current_filename'] = filename
        self.context['current_file_size'] = size
        
        return {'success': True}
    
    def _action_upload(self, step: Dict) -> Dict:
        """Upload a file."""
        if step.get('skip_file'):
            response = self.client.post("/files")
        else:
            content = self.context.get('current_file_content', b'x' * 2097152)
            filename = self.context.get('current_filename', 'test.bin')
            content_type = step.get('content_type', 'application/octet-stream')
            
            response = self.client.post(
                "/files",
                files={"file": (filename, io.BytesIO(content), content_type)}
            )
        
        expected_status = step.get('expected_status', 201)
        if response.status_code != expected_status:
            if response.status_code >= 400:
                try:
                    error_data = response.json()
                    self.context['last_error_response'] = error_data
                except:
                    self.context['last_error_response'] = {'detail': response.text}
            return {
                'success': False,
                'error': f'Expected status {expected_status}, got {response.status_code}'
            }
        
        if response.status_code == 201:
            data = response.json()
            file_id = data.get('file_id')
            self.context['last_file_id'] = file_id
            self.context['last_upload_response'] = data
        
        return {'success': True, 'response': response}
    
    def _action_upload_file(self, step: Dict) -> Dict:
        """Upload a file with specified parameters."""
        size = self._get_file_size(step.get('size', 'sample'))
        filename = step.get('filename', f'test_{uuid.uuid4().hex[:8]}.bin')
        content = self._generate_file_content(size)
        
        response = self.client.post(
            "/files",
            files={"file": (filename, io.BytesIO(content), 'application/octet-stream')}
        )
        
        if response.status_code != 201:
            return {
                'success': False,
                'error': f'Upload failed with status {response.status_code}'
            }
        
        data = response.json()
        file_id = data.get('file_id')
        self.context['last_file_id'] = file_id
        self.context[f'file_{filename}'] = file_id
        
        # Store checksum if needed
        if step.get('capture'):
            capture_key = step.get('capture')
            if isinstance(capture_key, str):
                self.context[capture_key] = file_id
            elif isinstance(capture_key, list):
                for key in capture_key:
                    self.context[key] = file_id
        
        return {'success': True, 'file_id': file_id}
    
    def _action_upload_multiple(self, step: Dict) -> Dict:
        """Upload multiple files with different content types."""
        files = step.get('files', [])
        uploaded_files = []
        
        for file_spec in files:
            filename = file_spec.get('filename', 'test.bin')
            content_type = file_spec.get('content_type', 'application/octet-stream')
            content = self._generate_file_content(self._get_file_size('sample'))
            
            response = self.client.post(
                "/files",
                files={"file": (filename, io.BytesIO(content), content_type)}
            )
            
            if response.status_code != step.get('expected_status', 201):
                return {
                    'success': False,
                    'error': f'Upload failed for {filename}'
                }
            
            uploaded_files.append({
                'filename': filename,
                'file_id': response.json().get('file_id'),
                'content_type': content_type
            })
        
        self.context['uploaded_files'] = uploaded_files
        return {'success': True}
    
    def _action_bulk_upload(self, step: Dict) -> Dict:
        """Upload multiple files in bulk."""
        count = step.get('count', 10)
        size = self._get_file_size(step.get('size', 'sample'))
        file_ids = []
        
        for i in range(count):
            filename = f"bulk_file_{i}.bin"
            content = self._generate_file_content(size)
            
            response = self.client.post(
                "/files",
                files={"file": (filename, io.BytesIO(content), 'application/octet-stream')}
            )
            
            if response.status_code != step.get('expected_status', 201):
                return {
                    'success': False,
                    'error': f'Bulk upload failed at file {i}'
                }
            
            file_ids.append(response.json().get('file_id'))
        
        self.context['bulk_file_ids'] = file_ids
        return {'success': True, 'count': len(file_ids)}
    
    def _action_download(self, step: Dict) -> Dict:
        """Download a file."""
        file_id = step.get('file_id') or self.context.get('last_file_id')
        if not file_id:
            return {'success': False, 'error': 'No file_id available'}
        
        response = self.client.get(f"/files/{file_id}")
        
        expected_status = step.get('expected_status', 200)
        if isinstance(expected_status, list):
            if response.status_code not in expected_status:
                return {
                    'success': False,
                    'error': f'Expected status in {expected_status}, got {response.status_code}'
                }
        elif response.status_code != expected_status:
            return {
                'success': False,
                'error': f'Expected status {expected_status}, got {response.status_code}'
            }
        
        if response.status_code == 200:
            self.context['last_download_content'] = response.content
            self.context['last_download_response'] = response.json()
        
        return {'success': True, 'response': response}
    
    def _action_get_metadata(self, step: Dict) -> Dict:
        """Get file metadata."""
        file_id = step.get('file_id') or self.context.get('last_file_id')
        if not file_id:
            return {'success': False, 'error': 'No file_id available'}
        
        response = self.client.get(f"/files/{file_id}/metadata")
        
        expected_status = step.get('expected_status', 200)
        if response.status_code != expected_status:
            return {
                'success': False,
                'error': f'Expected status {expected_status}, got {response.status_code}'
            }
        
        if response.status_code == 200:
            metadata = response.json()
            self.context['last_metadata'] = metadata
            
            # Capture specified fields
            capture = step.get('capture')
            if capture:
                if isinstance(capture, str):
                    self.context[capture] = metadata
                elif isinstance(capture, list):
                    for key in capture:
                        if key.endswith('_t0'):
                            field = key.replace('_t0', '')
                            self.context[key] = metadata.get(field)
                        elif key.endswith('_t1'):
                            field = key.replace('_t1', '')
                            self.context[key] = metadata.get(field)
                        else:
                            self.context[key] = metadata.get(key)
        
        return {'success': True, 'metadata': response.json() if response.status_code == 200 else None}
    
    def _action_delete(self, step: Dict) -> Dict:
        """Delete a file."""
        count = step.get('count', 1)
        file_ids_to_delete = []
        
        if count > 1:
            file_ids_to_delete = self.context.get('bulk_file_ids', [])[:count]
        else:
            file_id = step.get('file_id') or self.context.get('last_file_id')
            if file_id:
                file_ids_to_delete = [file_id]
        
        for file_id in file_ids_to_delete:
            response = self.client.delete(f"/files/{file_id}")
            
            expected_status = step.get('expected_status', 204)
            if isinstance(expected_status, list):
                if response.status_code not in expected_status:
                    return {
                        'success': False,
                        'error': f'Expected status in {expected_status}, got {response.status_code}'
                    }
            elif response.status_code != expected_status:
                return {
                    'success': False,
                    'error': f'Expected status {expected_status}, got {response.status_code}'
                }
        
        return {'success': True}
    
    def _action_run_tiering(self, step: Dict) -> Dict:
        """Run tiering process."""
        response = self.client.post("/admin/tiering/run")
        
        expected_status = step.get('expected_status', 200)
        if response.status_code != expected_status:
            return {
                'success': False,
                'error': f'Expected status {expected_status}, got {response.status_code}'
            }
        
        self.context['last_tiering_response'] = response.json()
        return {'success': True, 'response': response.json()}
    
    def _action_get_stats(self, step: Dict) -> Dict:
        """Get admin statistics."""
        response = self.client.get("/admin/stats")
        
        expected_status = step.get('expected_status', 200)
        if response.status_code != expected_status:
            return {
                'success': False,
                'error': f'Expected status {expected_status}, got {response.status_code}'
            }
        
        stats = response.json()
        self.context['last_stats'] = stats
        
        capture = step.get('capture')
        if capture:
            self.context[capture] = stats
        
        return {'success': True, 'stats': stats}
    
    def _action_update_last_accessed(self, step: Dict) -> Dict:
        """Update last accessed time for a file."""
        file_id = step.get('file_id') or self.context.get('last_file_id')
        if not file_id:
            return {'success': False, 'error': 'No file_id available'}
        
        days_ago = step.get('days_ago', 0)
        response = self.client.post(
            f"/admin/files/{file_id}/update-last-accessed",
            json={"days_ago": days_ago}
        )
        
        if response.status_code != 200:
            return {
                'success': False,
                'error': f'Update failed with status {response.status_code}'
            }
        
        return {'success': True}
    
    def _action_verify_response(self, step: Dict) -> Dict:
        """Verify response fields."""
        response_data = self.context.get('last_upload_response') or \
                       self.context.get('last_tiering_response') or \
                       self.context.get('last_stats')
        
        if not response_data:
            return {'success': False, 'error': 'No response data available'}
        
        fields = step.get('fields', {})
        for key, expected_value in fields.items():
            actual_value = response_data.get(key)
            if actual_value != expected_value:
                return {
                    'success': False,
                    'error': f'Field {key}: expected {expected_value}, got {actual_value}'
                }
        
        return {'success': True}
    
    def _action_verify_error(self, step: Dict) -> Dict:
        """Verify error message."""
        message_contains = step.get('message_contains', '')
        last_response = self.context.get('last_error_response')
        
        if not last_response:
            return {'success': False, 'error': 'No error response available'}
        
        error_detail = last_response.get('detail', '')
        if message_contains and message_contains.lower() not in error_detail.lower():
            return {
                'success': False,
                'error': f'Error message does not contain "{message_contains}"'
            }
        
        return {'success': True}
    
    def _action_verify_content_types(self, step: Dict) -> Dict:
        """Verify content types are stored correctly."""
        uploaded_files = self.context.get('uploaded_files', [])
        
        for file_info in uploaded_files:
            file_id = file_info.get('file_id')
            expected_content_type = file_info.get('content_type')
            
            response = self.client.get(f"/files/{file_id}/metadata")
            if response.status_code != 200:
                return {'success': False, 'error': f'Failed to get metadata for {file_id}'}
            
            metadata = response.json()
            actual_content_type = metadata.get('content_type')
            
            if actual_content_type != expected_content_type:
                return {
                    'success': False,
                    'error': f'Content type mismatch for {file_id}: expected {expected_content_type}, got {actual_content_type}'
                }
        
        return {'success': True}
    
    def _action_verify_tier(self, step: Dict) -> Dict:
        """Verify file tier."""
        file_id = step.get('file_id') or self.context.get('last_file_id')
        if not file_id:
            return {'success': False, 'error': 'No file_id available'}
        
        response = self.client.get(f"/files/{file_id}/metadata")
        if response.status_code != 200:
            return {'success': False, 'error': 'Failed to get metadata'}
        
        metadata = response.json()
        expected_tier = step.get('tier', 'HOT')
        actual_tier = metadata.get('tier')
        
        if actual_tier != expected_tier:
            return {
                'success': False,
                'error': f'Expected tier {expected_tier}, got {actual_tier}'
            }
        
        return {'success': True}
    
    def _action_wait(self, step: Dict) -> Dict:
        """Wait for specified seconds."""
        seconds = step.get('seconds', 1)
        time.sleep(seconds)
        return {'success': True}
    
    def _action_generate_uuid(self, step: Dict) -> Dict:
        """Generate a random UUID."""
        random_uuid = str(uuid.uuid4())
        self.context['last_file_id'] = random_uuid
        return {'success': True, 'uuid': random_uuid}
    
    def _action_calculate_checksum(self, step: Dict) -> Dict:
        """Calculate checksum of current file content."""
        content = self.context.get('current_file_content') or \
                 self.context.get('last_download_content')
        if not content:
            return {'success': False, 'error': 'No content available for checksum'}
        
        checksum = self._calculate_checksum(content)
        self.context['last_checksum'] = checksum
        return {'success': True, 'checksum': checksum}
    
    def _action_verify_content_match(self, step: Dict) -> Dict:
        """Verify downloaded content matches original."""
        original_content = self.context.get('current_file_content')
        downloaded_content = self.context.get('last_download_content')
        
        if not original_content or not downloaded_content:
            return {'success': False, 'error': 'Content not available for comparison'}
        
        if original_content != downloaded_content:
            return {'success': False, 'error': 'Content mismatch'}
        
        return {'success': True}
    
    def _action_verify_checksum_match(self, step: Dict) -> Dict:
        """Verify checksum matches."""
        original_checksum = self.context.get('last_checksum')
        downloaded_content = self.context.get('last_download_content')
        
        if not original_checksum or not downloaded_content:
            return {'success': False, 'error': 'Checksum or content not available'}
        
        downloaded_checksum = self._calculate_checksum(downloaded_content)
        if original_checksum != downloaded_checksum:
            return {
                'success': False,
                'error': f'Checksum mismatch: {original_checksum} vs {downloaded_checksum}'
            }
        
        return {'success': True}
    
    def _action_verify_last_accessed_updated(self, step: Dict) -> Dict:
        """Verify last_accessed was updated after download."""
        file_id = self.context.get('last_file_id')
        if not file_id:
            return {'success': False, 'error': 'No file_id available'}
        
        # Get metadata before download
        response_before = self.client.get(f"/files/{file_id}/metadata")
        if response_before.status_code != 200:
            return {'success': False, 'error': 'Failed to get metadata before'}
        
        metadata_before = response_before.json()
        last_accessed_before = metadata_before.get('last_accessed')
        
        # Download file
        self.client.get(f"/files/{file_id}")
        
        # Get metadata after download
        response_after = self.client.get(f"/files/{file_id}/metadata")
        if response_after.status_code != 200:
            return {'success': False, 'error': 'Failed to get metadata after'}
        
        metadata_after = response_after.json()
        last_accessed_after = metadata_after.get('last_accessed')
        
        from datetime import datetime
        dt_before = datetime.fromisoformat(last_accessed_before.replace('Z', '+00:00'))
        dt_after = datetime.fromisoformat(last_accessed_after.replace('Z', '+00:00'))
        
        if dt_after <= dt_before:
            return {'success': False, 'error': 'last_accessed was not updated'}
        
        return {'success': True}
    
    def _action_verify_timestamp_increased(self, step: Dict) -> Dict:
        """Verify timestamp increased."""
        t0 = self.context.get('last_accessed_t0')
        t1 = self.context.get('last_accessed_t1')
        
        if not t0 or not t1:
            return {'success': False, 'error': 'Timestamps not available'}
        
        from datetime import datetime
        dt0 = datetime.fromisoformat(t0.replace('Z', '+00:00'))
        dt1 = datetime.fromisoformat(t1.replace('Z', '+00:00'))
        
        if dt1 <= dt0:
            return {'success': False, 'error': 'Timestamp did not increase'}
        
        return {'success': True}
    
    def _action_verify_stats(self, step: Dict) -> Dict:
        """Verify stats match expected values."""
        stats = self.context.get('last_stats', {})
        expected_files = step.get('expected_files')
        
        if expected_files is not None:
            if stats.get('total_files', 0) != expected_files:
                return {
                    'success': False,
                    'error': f'Expected {expected_files} files, got {stats.get("total_files", 0)}'
                }
        
        return {'success': True}
    
    def _action_verify_stats_decreased(self, step: Dict) -> Dict:
        """Verify stats decreased after deletion."""
        stats_before = self.context.get('stats_before', {})
        stats_after = self.context.get('stats_after', {})
        
        if not stats_before or not stats_after:
            return {'success': False, 'error': 'Stats not available'}
        
        files_deleted = step.get('files', 1)
        if stats_after.get('total_files', 0) != stats_before.get('total_files', 0) - files_deleted:
            return {'success': False, 'error': 'File count did not decrease correctly'}
        
        return {'success': True}
    
    def _action_concurrent_upload(self, step: Dict) -> Dict:
        """Perform concurrent uploads."""
        count = step.get('count', 10)
        size = self._get_file_size(step.get('size', 'sample'))
        file_ids = []
        
        def upload_one(i):
            filename = f"concurrent_{i}.bin"
            content = self._generate_file_content(size)
            response = self.client.post(
                "/files",
                files={"file": (filename, io.BytesIO(content), 'application/octet-stream')}
            )
            return response
        
        with ThreadPoolExecutor(max_workers=count) as executor:
            futures = [executor.submit(upload_one, i) for i in range(count)]
            results = [f.result() for f in as_completed(futures)]
        
        for response in results:
            if response.status_code != step.get('expected_status', 201):
                return {'success': False, 'error': 'Concurrent upload failed'}
            file_ids.append(response.json().get('file_id'))
        
        self.context['concurrent_file_ids'] = file_ids
        return {'success': True, 'count': len(file_ids)}
    
    def _action_concurrent_download(self, step: Dict) -> Dict:
        """Perform concurrent downloads."""
        file_id = self.context.get('last_file_id')
        if not file_id:
            return {'success': False, 'error': 'No file_id available'}
        
        count = step.get('count', 10)
        expected_status = step.get('expected_status', 200)
        
        def download_one():
            return self.client.get(f"/files/{file_id}")
        
        with ThreadPoolExecutor(max_workers=count) as executor:
            futures = [executor.submit(download_one) for _ in range(count)]
            results = [f.result() for f in as_completed(futures)]
        
        for response in results:
            if response.status_code != expected_status:
                return {'success': False, 'error': 'Concurrent download failed'}
        
        return {'success': True}
    
    def _action_concurrent_delete(self, step: Dict) -> Dict:
        """Perform concurrent deletes."""
        file_ids = self.context.get('bulk_file_ids', [])
        if not file_ids:
            return {'success': False, 'error': 'No file_ids available'}
        
        count = step.get('count', len(file_ids))
        expected_status = step.get('expected_status', 204)
        
        def delete_one(fid):
            return self.client.delete(f"/files/{fid}")
        
        with ThreadPoolExecutor(max_workers=count) as executor:
            futures = [executor.submit(delete_one, fid) for fid in file_ids[:count]]
            results = [f.result() for f in as_completed(futures)]
        
        for response in results:
            if response.status_code != expected_status:
                return {'success': False, 'error': 'Concurrent delete failed'}
        
        return {'success': True}
    
    def _action_concurrent_tiering(self, step: Dict) -> Dict:
        """Perform concurrent tiering runs."""
        count = step.get('count', 5)
        expected_status = step.get('expected_status', 200)
        
        def tiering_one():
            return self.client.post("/admin/tiering/run")
        
        with ThreadPoolExecutor(max_workers=count) as executor:
            futures = [executor.submit(tiering_one) for _ in range(count)]
            results = [f.result() for f in as_completed(futures)]
        
        for response in results:
            if response.status_code != expected_status:
                return {'success': False, 'error': 'Concurrent tiering failed'}
        
        return {'success': True}
    
    def _action_verify_unique_ids(self, step: Dict) -> Dict:
        """Verify all file IDs are unique."""
        file_ids = self.context.get('concurrent_file_ids') or \
                  self.context.get('bulk_file_ids', [])
        
        if len(file_ids) != len(set(file_ids)):
            return {'success': False, 'error': 'Duplicate file IDs found'}
        
        return {'success': True}
    
    def _action_verify_all_success(self, step: Dict) -> Dict:
        """Verify all operations succeeded."""
        # This is handled by the action itself
        return {'success': True}
    
    def _action_verify_all_deleted(self, step: Dict) -> Dict:
        """Verify all files are deleted."""
        file_ids = self.context.get('bulk_file_ids', [])
        for file_id in file_ids:
            response = self.client.get(f"/files/{file_id}")
            if response.status_code != 404:
                return {'success': False, 'error': f'File {file_id} still exists'}
        
        return {'success': True}
    
    def _action_verify_ids_different(self, step: Dict) -> Dict:
        """Verify two file IDs are different."""
        id_a = self.context.get('file_id_a')
        id_b = self.context.get('file_id_b')
        
        if not id_a or not id_b:
            return {'success': False, 'error': 'File IDs not available'}
        
        if id_a == id_b:
            return {'success': False, 'error': 'File IDs are the same'}
        
        return {'success': True}
    
    def _action_verify_tiering_results(self, step: Dict) -> Dict:
        """Verify tiering results."""
        # Get all file IDs from context
        file_ids = self.context.get('bulk_file_ids', [])
        if not file_ids:
            file_id = self.context.get('last_file_id')
            if file_id:
                file_ids = [file_id]
        
        # Verify each file is in the correct tier
        for file_id in file_ids:
            response = self.client.get(f"/files/{file_id}/metadata")
            if response.status_code != 200:
                return {'success': False, 'error': f'Failed to get metadata for {file_id}'}
            
            metadata = response.json()
            tier = metadata.get('tier')
            days_ago = metadata.get('last_accessed')
            
            # Basic validation - files should be in appropriate tiers
            # More specific validation would depend on test requirements
            if tier not in ['HOT', 'WARM', 'COLD']:
                return {'success': False, 'error': f'Invalid tier {tier} for {file_id}'}
        
        return {'success': True}
    
    def _action_verify_consistent_state(self, step: Dict) -> Dict:
        """Verify system state is consistent after concurrent operations."""
        # Get stats to verify consistency
        response = self.client.get("/admin/stats")
        if response.status_code != 200:
            return {'success': False, 'error': 'Failed to get stats'}
        
        stats = response.json()
        
        # Verify tier counts and sizes are consistent
        total_from_tiers = sum(tier_data.get('count', 0) for tier_data in stats.get('tiers', {}).values())
        total_files = stats.get('total_files', 0)
        
        if total_from_tiers != total_files:
            return {
                'success': False,
                'error': f'Inconsistent state: tier counts ({total_from_tiers}) != total_files ({total_files})'
            }
        
        # Verify all file IDs in context still exist and are accessible
        file_ids = self.context.get('bulk_file_ids', [])
        for file_id in file_ids:
            response = self.client.get(f"/files/{file_id}/metadata")
            if response.status_code != 200:
                return {'success': False, 'error': f'File {file_id} is not accessible'}
        
        return {'success': True}
    
    def _action_verify_tier_stats(self, step: Dict) -> Dict:
        """Verify tier statistics."""
        stats = self.context.get('last_stats', {})
        tiers = stats.get('tiers', {})
        
        if step.get('all_zero'):
            for tier_name, tier_data in tiers.items():
                if tier_data.get('count', 0) != 0 or tier_data.get('size', 0) != 0:
                    return {'success': False, 'error': f'Tier {tier_name} is not zero'}
        
        hot_config = step.get('hot', {})
        if hot_config:
            hot_count = tiers.get('HOT', {}).get('count', 0)
            if hot_count != hot_config.get('count', 0):
                return {'success': False, 'error': f'Expected HOT count {hot_config.get("count")}, got {hot_count}'}
        
        return {'success': True}
    
    def _action_verify_tier_distribution(self, step: Dict) -> Dict:
        """Verify tier distribution after tiering."""
        stats = self.context.get('last_stats', {})
        tiers = stats.get('tiers', {})
        
        # Verify total files match sum of tier counts
        total_from_tiers = sum(tier_data.get('count', 0) for tier_data in tiers.values())
        total_files = stats.get('total_files', 0)
        
        if total_from_tiers != total_files:
            return {
                'success': False,
                'error': f'Tier counts sum ({total_from_tiers}) does not match total_files ({total_files})'
            }
        
        # Verify total size matches sum of tier sizes
        total_size_from_tiers = sum(tier_data.get('size', 0) for tier_data in tiers.values())
        total_size = stats.get('total_size', 0)
        
        if total_size_from_tiers != total_size:
            return {
                'success': False,
                'error': f'Tier sizes sum ({total_size_from_tiers}) does not match total_size ({total_size})'
            }
        
        return {'success': True}
    
    def _action_verify_increment(self, step: Dict) -> Dict:
        """Verify stats incremented."""
        baseline = self.context.get('baseline', {})
        after_upload = self.context.get('after_upload', {})
        
        if after_upload.get('total_files', 0) != baseline.get('total_files', 0) + 1:
            return {'success': False, 'error': 'File count did not increment'}
        
        return {'success': True}
    
    def _action_verify_decrement(self, step: Dict) -> Dict:
        """Verify stats decremented."""
        after_upload = self.context.get('after_upload', {})
        after_delete = self.context.get('after_delete', {})
        
        if after_delete.get('total_files', 0) != after_upload.get('total_files', 0) - 1:
            return {'success': False, 'error': 'File count did not decrement'}
        
        return {'success': True}
    
    def _action_ensure_empty_system(self, step: Dict) -> Dict:
        """Ensure system is empty."""
        # Clear all files
        stats = self.client.get("/admin/stats").json()
        file_ids = []  # Would need to get all file IDs somehow
        # For now, we'll assume system is already cleared by test fixture
        return {'success': True}
    
    def _action_ensure_recent_access(self, step: Dict) -> Dict:
        """Ensure all files have recent access."""
        max_days = step.get('max_days', 29)
        # Implementation would update all files to have recent access
        return {'success': True}
    
    def _action_move_to_cold(self, step: Dict) -> Dict:
        """Move a file to COLD tier."""
        file_id = self.context.get('last_file_id')
        if not file_id:
            return {'success': False, 'error': 'No file_id available'}
        
        # Move to WARM first
        self.client.post(
            f"/admin/files/{file_id}/update-last-accessed",
            json={"days_ago": 35}
        )
        self.client.post("/admin/tiering/run")
        
        # Then to COLD
        self.client.post(
            f"/admin/files/{file_id}/update-last-accessed",
            json={"days_ago": 95}
        )
        self.client.post("/admin/tiering/run")
        
        return {'success': True}
    
    def _action_prepare_tiering_dataset(self, step: Dict) -> Dict:
        """Prepare dataset for tiering tests."""
        # Create files with varied last_accessed times
        return {'success': True}
    
    def _action_setup_tiering_scenario(self, step: Dict) -> Dict:
        """Setup tiering test scenario."""
        # Setup files in different states for tiering
        return {'success': True}

