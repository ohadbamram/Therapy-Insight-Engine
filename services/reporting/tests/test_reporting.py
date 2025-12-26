import pytest
from unittest.mock import AsyncMock, patch
from fastapi.testclient import TestClient
from typing import Generator
from services.reporting.main import app

# Mock Data Constants
MOCK_VIDEO_ID = "d41ae112-e5d2-4736-be32-ac01aac266b5"


@pytest.fixture
def mock_db_connection() -> Generator[AsyncMock, None, None]:
    with patch("services.reporting.main.asyncpg.connect") as mock_connect:
        mock_conn = AsyncMock()
        mock_connect.return_value = mock_conn
        yield mock_conn


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture
def mock_video_list_data() -> list[dict]:
    return [
        {
            "video_id": MOCK_VIDEO_ID,
            "filename": "test.mp4",
            "status": "completed",
            "created_at": "2025-12-01 10:00:00",
            "summary_text": "Patient is anxious.",
        }
    ]


@pytest.fixture
def mock_video_detail_summary() -> dict:
    return {
        "summary_text": "Detailed summary.",
        "recommendations": '["Do breathing exercises"]',
        "cognitive_distortions": '[{"quote": "I am bad", "distortion_type": "Labeling", "explanation": "..."}]',
        "therapist_interventions": '[{"quote": "Why?", "technique": "Question", "purpose": "..."}]',
    }


@pytest.fixture
def mock_video_detail_segments() -> list[dict]:
    return [
        {
            "speaker_role": "patient",
            "text_content": "I feel sad.",
            "topic": "sadness",
            "emotion": "sad",
            "confidence_score": 0.99,
        }
    ]


def test_list_videos_returns_200_ok(
    mock_db_connection: AsyncMock, client: TestClient, mock_video_list_data: list
) -> None:
    mock_db_connection.fetch.return_value = mock_video_list_data

    response = client.get("/videos")

    assert response.status_code == 200


def test_list_videos_returns_correct_list_length(
    mock_db_connection: AsyncMock, client: TestClient, mock_video_list_data: list
) -> None:
    mock_db_connection.fetch.return_value = mock_video_list_data

    response = client.get("/videos")

    assert len(response.json()) == 1


def test_list_videos_returns_correct_video_id(
    mock_db_connection: AsyncMock, client: TestClient, mock_video_list_data: list
) -> None:
    mock_db_connection.fetch.return_value = mock_video_list_data

    response = client.get("/videos")

    assert response.json()[0]["video_id"] == MOCK_VIDEO_ID


def test_list_videos_queries_database(
    mock_db_connection: AsyncMock, client: TestClient, mock_video_list_data: list
) -> None:
    mock_db_connection.fetch.return_value = mock_video_list_data

    client.get("/videos")

    mock_db_connection.fetch.assert_called_once()


def test_get_video_detail_returns_200_ok(
    mock_db_connection: AsyncMock,
    client: TestClient,
    mock_video_detail_summary: dict,
    mock_video_detail_segments: list,
) -> None:
    mock_db_connection.fetchrow.return_value = mock_video_detail_summary
    mock_db_connection.fetch.return_value = mock_video_detail_segments

    response = client.get(f"/videos/{MOCK_VIDEO_ID}")

    assert response.status_code == 200


def test_get_video_detail_returns_summary(
    mock_db_connection: AsyncMock,
    client: TestClient,
    mock_video_detail_summary: dict,
    mock_video_detail_segments: list,
) -> None:
    mock_db_connection.fetchrow.return_value = mock_video_detail_summary
    mock_db_connection.fetch.return_value = mock_video_detail_segments

    response = client.get(f"/videos/{MOCK_VIDEO_ID}")

    assert response.json()["summary"] == "Detailed summary."


def test_get_video_detail_parses_recommendations_json(
    mock_db_connection: AsyncMock,
    client: TestClient,
    mock_video_detail_summary: dict,
    mock_video_detail_segments: list,
) -> None:
    mock_db_connection.fetchrow.return_value = mock_video_detail_summary
    mock_db_connection.fetch.return_value = mock_video_detail_segments

    response = client.get(f"/videos/{MOCK_VIDEO_ID}")

    assert len(response.json()["recommendations"]) == 1


def test_get_video_detail_parses_cognitive_distortions_json(
    mock_db_connection: AsyncMock,
    client: TestClient,
    mock_video_detail_summary: dict,
    mock_video_detail_segments: list,
) -> None:
    mock_db_connection.fetchrow.return_value = mock_video_detail_summary
    mock_db_connection.fetch.return_value = mock_video_detail_segments

    response = client.get(f"/videos/{MOCK_VIDEO_ID}")

    assert response.json()["cognitive_distortions"][0]["distortion_type"] == "Labeling"


def test_get_video_detail_parses_therapist_interventions_json(
    mock_db_connection: AsyncMock,
    client: TestClient,
    mock_video_detail_summary: dict,
    mock_video_detail_segments: list,
) -> None:
    mock_db_connection.fetchrow.return_value = mock_video_detail_summary
    mock_db_connection.fetch.return_value = mock_video_detail_segments

    response = client.get(f"/videos/{MOCK_VIDEO_ID}")

    assert response.json()["therapist_interventions"][0]["technique"] == "Question"


def test_get_video_detail_includes_transcript_segments(
    mock_db_connection: AsyncMock,
    client: TestClient,
    mock_video_detail_summary: dict,
    mock_video_detail_segments: list,
) -> None:
    mock_db_connection.fetchrow.return_value = mock_video_detail_summary
    mock_db_connection.fetch.return_value = mock_video_detail_segments

    response = client.get(f"/videos/{MOCK_VIDEO_ID}")

    assert len(response.json()["transcript_segments"]) == 1


def test_get_video_detail_returns_404_if_not_found(
    mock_db_connection: AsyncMock, client: TestClient
) -> None:
    mock_db_connection.fetchrow.return_value = None

    response = client.get("/videos/unknown-id")

    assert response.status_code == 404
