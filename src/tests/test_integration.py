import os
import json
import pytest
import uuid
from unittest.mock import patch
from pathlib import Path

import boto3
import moto
import fakeredis
from fastapi.testclient import TestClient

from app.main import app
from pdf_parser.settings import get_settings
from worker.tasks import parse_pdf


# Configure test environment
os.environ["S3_ENDPOINT_URL"] = "http://localhost:9000"
os.environ["S3_ACCESS_KEY"] = "minio"
os.environ["S3_SECRET_KEY"] = "minio123"
os.environ["S3_REGION"] = "us-east-1"
os.environ["S3_BUCKET_NAME"] = "pdf-parser-test"
os.environ["REDIS_URL"] = "redis://localhost:6379/0"
os.environ["CELERY_BROKER_URL"] = "memory://"
os.environ["CELERY_RESULT_BACKEND"] = "redis://localhost:6379/0"


@pytest.fixture
def test_client():
    """Create a test client for the FastAPI app."""
    with TestClient(app) as client:
        yield client


@pytest.fixture
def redis_mock():
    """Create a mock Redis instance."""
    redis_server = fakeredis.FakeRedis()
    with patch("app.main.redis.from_url") as mock_redis:
        mock_redis.return_value = redis_server
        with patch("worker.tasks.BaseTask.redis", redis_server):
            yield redis_server


@pytest.fixture
def s3_mock():
    """Create a mock S3 instance using moto."""
    with moto.mock_s3():
        # Create test bucket
        s3_client = boto3.client(
            "s3",
            endpoint_url=os.environ["S3_ENDPOINT_URL"],
            aws_access_key_id=os.environ["S3_ACCESS_KEY"],
            aws_secret_access_key=os.environ["S3_SECRET_KEY"],
            region_name=os.environ["S3_REGION"],
        )
        s3_client.create_bucket(Bucket=os.environ["S3_BUCKET_NAME"])
        
        # Use the patched S3 client in the app
        with patch("pdf_parser.utils.storage.get_s3_client") as mock_s3:
            mock_s3.return_value = s3_client
            yield s3_client


@pytest.fixture
def sample_pdf():
    """Create a minimal PDF file for testing."""
    # Create a directory for test files
    test_dir = Path("./tests/test_files")
    test_dir.mkdir(exist_ok=True)
    
    # Path to the sample PDF
    pdf_path = test_dir / "sample.pdf"
    
    # If the file doesn't exist, create a minimal PDF
    if not pdf_path.exists():
        try:
            from reportlab.pdfgen import canvas
            c = canvas.Canvas(str(pdf_path))
            c.drawString(100, 750, "Test PDF Document")
            c.drawString(100, 700, "This is a sample PDF for testing.")
            c.save()
        except ImportError:
            # If reportlab is not available, create a dummy binary file
            with open(pdf_path, "wb") as f:
                f.write(b"%PDF-1.7\n1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\ntrailer\n<< /Root 1 0 R >>\n%%EOF")
    
    return pdf_path


@pytest.mark.asyncio
async def test_upload_and_status_flow(test_client, redis_mock, s3_mock, sample_pdf):
    """Test the complete flow from upload to status check."""
    # Upload the sample PDF to S3
    pdf_key = "test/sample.pdf"
    with open(sample_pdf, "rb") as f:
        s3_mock.put_object(
            Bucket=os.environ["S3_BUCKET_NAME"],
            Key=pdf_key,
            Body=f.read()
        )
    
    # Create S3 URL
    pdf_url = f"s3://{os.environ['S3_BUCKET_NAME']}/{pdf_key}"
    
    # Mock the Celery task to update Redis directly
    task_id = str(uuid.uuid4())
    
    # Submit the upload request
    with patch("worker.tasks.parse_pdf.delay") as mock_task:
        mock_task.return_value = None
        
        response = test_client.post(
            "/upload",
            json={"url": pdf_url}
        )
        
        # Check that the upload request was successful
        assert response.status_code == 200
        result = response.json()
        assert "task_id" in result
        task_id = result["task_id"]
    
    # Simulate task processing
    redis_mock.set(
        f"task:{task_id}",
        json.dumps({
            "state": "running",
            "progress": 50
        }),
        ex=86400
    )
    
    # Check the status endpoint - running state
    response = test_client.get(f"/status/{task_id}")
    assert response.status_code == 200
    status_result = response.json()
    assert status_result["state"] == "running"
    assert status_result["progress"] == 50
    
    # Simulate task completion
    result_key = f"results/12345/test-result.json"
    redis_mock.set(
        f"task:{task_id}",
        json.dumps({
            "state": "succeeded",
            "progress": 100,
            "result_key": result_key,
            "table_keys": ["tables/12345/table_1.json"]
        }),
        ex=86400
    )
    
    # Check the status endpoint - completed state
    response = test_client.get(f"/status/{task_id}")
    assert response.status_code == 200
    status_result = response.json()
    assert status_result["state"] == "succeeded"
    assert status_result["progress"] == 100
    assert status_result["result_key"] == result_key
    
    # Test non-existent task
    response = test_client.get(f"/status/{uuid.uuid4()}")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_queue_backpressure(test_client, redis_mock):
    """Test that the API returns 429 when queue is full."""
    # Mock the task count function to simulate a full queue
    with patch("app.main.get_task_count") as mock_task_count:
        mock_task_count.return_value = 1000  # Very high number to trigger backpressure
        
        response = test_client.post(
            "/upload",
            json={"url": "s3://test-bucket/test.pdf"}
        )
        
        # Check that backpressure was triggered
        assert response.status_code == 429 