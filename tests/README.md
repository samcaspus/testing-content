# Test Framework Documentation

## Overview

This test framework provides a config-driven approach to testing the Cloud Storage Tiering System. All test cases are defined in `test_config.yaml` and executed by the test framework.

## Directory Structure

```
tests/
├── conftest.py              # Shared pytest fixtures
├── config/                  # Test configuration files
│   ├── functional_test_config.yaml    # Functional test cases
│   ├── performance_test_config.yaml   # Performance test cases
│   └── security_test_config.yaml      # Security test cases
├── framework/
│   ├── __init__.py
│   ├── test_runner.py       # Test execution framework
│   ├── performance_runner.py # Performance test runner
│   └── security_runner.py   # Security test runner
├── functional/              # Functional tests
│   ├── __init__.py
│   ├── test_file_operations.py
│   └── test_admin_operations.py
├── performance/            # Performance tests
│   ├── __init__.py
│   └── test_performance.py
├── security/               # Security tests
│   ├── __init__.py
│   └── test_security.py
└── fault_injection/        # Fault injection tests
    └── __init__.py
```

## Test Configuration

All test cases are defined in YAML configuration files in the `config/` directory:

- **Functional Tests**: `config/functional_test_config.yaml`
- **Performance Tests**: `config/performance_test_config.yaml`
- **Security Tests**: `config/security_test_config.yaml`

The configuration includes:

- **Test Data**: File sizes, content types, concurrent worker counts
- **Test Cases**: Organized by category (file_upload, file_download, file_metadata, file_delete, tiering, admin_stats)

Each test case includes:
- `id`: Unique test identifier (e.g., TC-FILE-001)
- `priority`: Test priority (P0, P1, P2)
- `name`: Test case name
- `endpoint`: API endpoint being tested
- `steps`: List of actions to execute

## Running Tests

### Run all functional tests:
```bash
pytest tests/functional/
```

### Run performance tests:
```bash
pytest tests/performance/
```

### Run security tests:
```bash
pytest tests/security/
```

### Run specific test category:
```bash
pytest tests/functional/test_file_operations.py
pytest tests/functional/test_admin_operations.py
pytest tests/performance/test_performance.py
pytest tests/security/test_security.py
```

### Run with verbose output:
```bash
pytest tests/functional/ -v
pytest tests/performance/ -v -s  # -s shows print statements
pytest tests/security/ -v -s
```

### Run specific test by ID:
```bash
pytest tests/functional/ -k "TC-FILE-001"
pytest tests/performance/ -k "PERF-001"
pytest tests/security/ -k "SEC-001"
```

### Run tests with coverage:
```bash
pytest tests/functional/ --cov=src --cov-report=html
pytest tests/performance/ --cov=src --cov-report=html
```

## Test Actions

The framework supports the following actions:

### File Operations
- `create_file`: Create a test file with specified size
- `upload`: Upload a file
- `upload_file`: Upload a file with parameters
- `upload_multiple`: Upload multiple files with different content types
- `bulk_upload`: Upload multiple files in bulk
- `download`: Download a file
- `delete`: Delete a file

### Metadata Operations
- `get_metadata`: Get file metadata
- `update_last_accessed`: Update last accessed time

### Admin Operations
- `run_tiering`: Execute tiering process
- `get_stats`: Get admin statistics

### Verification Actions
- `verify_response`: Verify response fields
- `verify_error`: Verify error message
- `verify_tier`: Verify file tier
- `verify_content_match`: Verify downloaded content matches original
- `verify_checksum_match`: Verify checksum matches
- `verify_last_accessed_updated`: Verify last_accessed was updated
- `verify_timestamp_increased`: Verify timestamp increased
- `verify_stats`: Verify statistics
- `verify_stats_decreased`: Verify stats decreased
- `verify_content_types`: Verify content types are stored correctly
- `verify_unique_ids`: Verify all file IDs are unique
- `verify_all_deleted`: Verify all files are deleted

### Concurrent Operations
- `concurrent_upload`: Perform concurrent uploads
- `concurrent_download`: Perform concurrent downloads
- `concurrent_delete`: Perform concurrent deletes
- `concurrent_tiering`: Perform concurrent tiering runs

### Utility Actions
- `wait`: Wait for specified seconds
- `generate_random_uuid`: Generate a random UUID
- `calculate_checksum`: Calculate checksum of content

## Adding New Test Cases

To add a new test case:

1. Open `test_config.yaml`
2. Find the appropriate category (file_upload, file_download, etc.)
3. Add a new test case entry:

```yaml
- id: "TC-FILE-XXX"
  priority: "P1"
  name: "Test Case Name"
  endpoint: "POST /files"
  steps:
    - action: "upload_file"
      size: "sample"
    - action: "verify_response"
      fields:
        tier: "HOT"
```

4. Run the test to verify it works

## Test Data Configuration

Test data is configured in the `test_data` section of `test_config.yaml`:

```yaml
test_data:
  file_sizes:
    minimum: 1048576      # 1MB
    maximum: 10737418240   # 10GB
    sample: 2097152        # 2MB
    small: 524288          # 512KB
    large: 5368709120      # 5GB
    very_large: 11811160064 # 11GB
```

## Example Test Case

```yaml
- id: "TC-FILE-001"
  priority: "P0"
  name: "Upload Valid File (Happy Path)"
  endpoint: "POST /files"
  steps:
    - action: "create_file"
      size: "sample"
      filename: "sample.bin"
    - action: "upload"
      expected_status: 201
    - action: "verify_response"
      fields:
        - file_id
        - tier: "HOT"
    - action: "get_metadata"
      verify:
        - filename
        - size
        - content_type
        - tier
```

## Framework Features

1. **Config-Driven**: All tests defined in YAML configuration
2. **Reusable Actions**: Common actions can be reused across tests
3. **Context Management**: Test context is maintained across steps
4. **Error Handling**: Comprehensive error reporting
5. **Concurrent Testing**: Built-in support for concurrent operations
6. **Extensible**: Easy to add new actions and test cases

## Best Practices

1. **Test Isolation**: Each test should be independent
2. **Clear Test Names**: Use descriptive test case names
3. **Proper Assertions**: Verify all expected outcomes
4. **Error Cases**: Include negative test cases
5. **Boundary Testing**: Test edge cases and boundaries
6. **Documentation**: Document complex test scenarios

## Troubleshooting

### Tests failing with "No file_id available"
- Ensure previous steps in the test case create/upload a file
- Check that file_id is captured in context

### Concurrent tests failing
- Check system resources
- Verify thread safety of operations
- Increase wait times if needed

### Config file not found
- Ensure `test_config.yaml` is in the `tests/` directory
- Check file path in test files

## Future Enhancements

- [ ] Test result reporting (HTML/JSON)
- [ ] Test execution metrics
- [ ] Parallel test execution
- [ ] Test data generation utilities
- [ ] Integration with CI/CD pipelines

