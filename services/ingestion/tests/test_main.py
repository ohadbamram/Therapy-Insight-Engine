import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from fastapi.testclient import TestClient
from services.ingestion.main import app
from typing import Generator, Tuple


@pytest.fixture
def mock_dependencies() -> (
    Generator[Tuple[AsyncMock, MagicMock, AsyncMock], None, None]
):
    with patch("services.ingestion.main.broker") as mock_broker, patch(
        "services.ingestion.main.minio_client"
    ) as mock_minio, patch(
        "services.ingestion.main.asyncpg.connect"
    ) as mock_pg_connect:

        mock_broker.connect = AsyncMock()
        mock_broker.disconnect = AsyncMock()
        mock_broker.declare_queue = AsyncMock()
        mock_broker.publish = AsyncMock()

        mock_minio.bucket_exists.return_value = True
        mock_minio.make_bucket = MagicMock()
        mock_minio.put_object = MagicMock()

        mock_conn = AsyncMock()
        mock_pg_connect.return_value = mock_conn

        yield mock_broker, mock_minio, mock_pg_connect


@pytest.fixture
def sample_video_file():
    return {"file": ("test_video.mp4", b"fake video content", "video/mp4")}


def test_upload_returns_200_on_success(
    mock_dependencies: Tuple, sample_video_file: dict
) -> None:
    with TestClient(app) as client:
        response = client.post("/upload", files=sample_video_file)

    assert response.status_code == 200


def test_upload_returns_queued_status_on_success(
    mock_dependencies: Tuple, sample_video_file: dict
) -> None:
    with TestClient(app) as client:
        response = client.post("/upload", files=sample_video_file)

    assert response.json()["status"] == "queued"


def test_upload_returns_video_id_on_success(
    mock_dependencies: Tuple, sample_video_file: dict
) -> None:
    with TestClient(app) as client:
        response = client.post("/upload", files=sample_video_file)

    assert "video_id" in response.json()


def test_upload_saves_file_to_minio(
    mock_dependencies: Tuple, sample_video_file: dict
) -> None:
    _, mock_minio, _ = mock_dependencies

    with TestClient(app) as client:
        client.post("/upload", files=sample_video_file)

    mock_minio.put_object.assert_called_once()


def test_upload_publishes_event_to_broker(
    mock_dependencies: Tuple, sample_video_file: dict
) -> None:
    mock_broker, _, _ = mock_dependencies

    with TestClient(app) as client:
        client.post("/upload", files=sample_video_file)

    mock_broker.publish.assert_called_once()


def test_upload_inserts_metadata_into_database(
    mock_dependencies: Tuple, sample_video_file: dict
) -> None:
    _, _, mock_pg_connect = mock_dependencies

    with TestClient(app) as client:
        client.post("/upload", files=sample_video_file)

    mock_pg_connect.return_value.execute.assert_called()


def test_upload_returns_500_on_minio_failure(
    mock_dependencies: Tuple, sample_video_file: dict
) -> None:
    _, mock_minio, _ = mock_dependencies
    mock_minio.put_object.side_effect = Exception("S3 Connection Failed")

    with TestClient(app) as client:
        response = client.post("/upload", files=sample_video_file)

    assert response.status_code == 500


def test_upload_does_not_publish_event_on_minio_failure(
    mock_dependencies: Tuple, sample_video_file: dict
) -> None:
    mock_broker, mock_minio, _ = mock_dependencies
    mock_minio.put_object.side_effect = Exception("S3 Connection Failed")

    with TestClient(app) as client:
        client.post("/upload", files=sample_video_file)

    mock_broker.publish.assert_not_called()


def test_lifespan_connects_to_broker_on_startup(mock_dependencies: Tuple) -> None:
    mock_broker, _, _ = mock_dependencies

    with TestClient(app):
        pass

    mock_broker.connect.assert_awaited_once()


def test_lifespan_creates_minio_bucket_if_missing(mock_dependencies: Tuple) -> None:
    _, mock_minio, _ = mock_dependencies
    mock_minio.bucket_exists.return_value = False

    with TestClient(app):
        pass

    mock_minio.make_bucket.assert_called_with("videos")
