import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from fastapi.testclient import TestClient
from services.ingestion.main import app

# Create a TestClient. 
# We use a context manager in the tests (with TestClient...) to trigger startup/shutdown events.
client = TestClient(app)

@pytest.fixture
def mock_dependencies():
    """
    This fixture automatically patches the global broker and minio_client 
    in the main.py file for every test function.
    """
    with patch("services.ingestion.main.broker") as mock_broker, \
         patch("services.ingestion.main.minio_client") as mock_minio:
        
        # Setup RabbitMQ Mocks
        mock_broker.connect = AsyncMock()
        mock_broker.disconnect = AsyncMock()
        mock_broker.declare_queue = AsyncMock()
        mock_broker.publish = AsyncMock()

        # Setup MinIO Mocks
        # bucket_exists returns True by default so startup doesn't try to create it
        mock_minio.bucket_exists.return_value = True
        mock_minio.make_bucket = MagicMock()
        mock_minio.put_object = MagicMock()

        yield mock_broker, mock_minio

def test_upload_video_success(mock_dependencies):
    """
    Happy Path: User uploads file -> Saved to MinIO -> Event Published -> 200 OK
    """
    mock_broker, mock_minio = mock_dependencies

    # Simulate a file upload
    file_content = b"fake video content"
    files = {"file": ("test_video.mp4", file_content, "video/mp4")}

    # Make the request
    # Note: We use the client as a context manager to trigger lifespan (startup) events
    with TestClient(app) as client:
        response = client.post("/upload", files=files)

    # Assertions (The Verification)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "queued"
    assert "video_id" in data

    # Verify MinIO was called
    mock_minio.put_object.assert_called_once()
    
    # Verify RabbitMQ publish was called
    mock_broker.publish.assert_called_once()
    
    # Inspect the event sent to RabbitMQ
    # args[0] is the event object passed to publish()
    published_event = mock_broker.publish.call_args[0][0]
    assert published_event.filename.endswith("test_video.mp4")
    assert published_event.content_type == "video/mp4"

def test_upload_minio_failure(mock_dependencies):
    """
    Error Path: MinIO fails (e.g., network error) -> API returns 500
    """
    mock_broker, mock_minio = mock_dependencies

    # Configure MinIO mock to raise an exception
    mock_minio.put_object.side_effect = Exception("S3 Connection Failed")

    files = {"file": ("test_video.mp4", b"content", "video/mp4")}

    with TestClient(app) as client:
        response = client.post("/upload", files=files)

    # Assert we get a 500 error
    assert response.status_code == 500
    assert response.json()["detail"] == "Upload failed"

    # Verify we NEVER published to RabbitMQ (fail fast)
    mock_broker.publish.assert_not_called()

def test_lifespan_startup(mock_dependencies):
    """
    Test that startup logic creates buckets and declares queues.
    """
    mock_broker, mock_minio = mock_dependencies
    
    # Set bucket_exists to False so we verify make_bucket is called
    mock_minio.bucket_exists.return_value = False

    with TestClient(app) as client:
        # Just entering the context manager triggers the startup logic
        pass

    # Assertions
    mock_broker.connect.assert_awaited_once()
    mock_broker.declare_queue.assert_awaited_once()
    mock_minio.make_bucket.assert_called_with("videos")