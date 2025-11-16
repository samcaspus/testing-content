"""
Shared test fixtures and utilities for Cloud Storage Tiering System tests.
"""
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pytest
import httpx
import asyncio
from fastapi.testclient import TestClient
from src.storage_service import app, files_metadata, files_content, StorageTier
from datetime import datetime, timedelta
import io
import hashlib


@pytest.fixture(scope="function")
def client():
    """Create a test client for the FastAPI application."""
    # Clear storage before each test
    files_metadata.clear()
    files_content.clear()
    
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def sample_file_content():
    """Generate sample file content (2MB)."""
    return b"x" * (2 * 1024 * 1024)  # 2MB


@pytest.fixture
def small_file_content():
    """Generate small file content (512KB - below minimum)."""
    return b"x" * (512 * 1024)  # 512KB


@pytest.fixture
def large_file_content():
    """Generate large file content (5GB)."""
    return b"x" * (5 * 1024 * 1024 * 1024)  # 5GB


@pytest.fixture
def very_large_file_content():
    """Generate very large file content (11GB - exceeds maximum)."""
    return b"x" * (11 * 1024 * 1024 * 1024)  # 11GB


@pytest.fixture
def priority_file_content():
    """Generate priority file content."""
    return b"priority_content" * 100000  # ~1.7MB


@pytest.fixture
def legal_file_content():
    """Generate legal document file content."""
    return b"legal_document_content" * 100000  # ~2MB


@pytest.fixture
def uploaded_file_id(client, sample_file_content):
    """Upload a file and return its ID."""
    response = client.post(
        "/files",
        files={"file": ("test_file.txt", io.BytesIO(sample_file_content), "text/plain")}
    )
    assert response.status_code == 201
    return response.json()["file_id"]


@pytest.fixture
def uploaded_file_metadata(client, uploaded_file_id):
    """Get uploaded file metadata."""
    response = client.get(f"/files/{uploaded_file_id}/metadata")
    assert response.status_code == 200
    return response.json()


def create_file_with_last_accessed(client, content, filename, days_ago):
    """Helper function to create a file with specific last_accessed time."""
    # Upload file
    response = client.post(
        "/files",
        files={"file": (filename, io.BytesIO(content), "application/octet-stream")}
    )
    assert response.status_code == 201
    file_id = response.json()["file_id"]
    
    # Update last accessed time
    response = client.post(
        f"/admin/files/{file_id}/update-last-accessed",
        json={"days_ago": days_ago}
    )
    assert response.status_code == 200
    
    return file_id


def calculate_checksum(content):
    """Calculate SHA-256 checksum of content."""
    return hashlib.sha256(content).hexdigest()


def generate_file_content(size_bytes):
    """Generate file content of specified size."""
    return b"x" * size_bytes

