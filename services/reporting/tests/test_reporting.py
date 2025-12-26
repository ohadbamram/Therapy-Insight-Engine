import pytest
from unittest.mock import AsyncMock, patch
from fastapi.testclient import TestClient
from typing import Generator

# Mock Data
MOCK_VIDEO_ID = "d41ae112-e5d2-4736-be32-ac01aac266b5"


@pytest.fixture
def mock_db() -> Generator[AsyncMock, None, None]:
    with patch("services.reporting.main.asyncpg.connect") as mock_connect:
        mock_conn = AsyncMock()
        mock_connect.return_value = mock_conn
        yield mock_conn


def test_list_videos(mock_db) -> None:
    """
    Scenario: GET /videos
    Expectation: Returns list of videos from DB.
    """
    # 1. Mock DB Response
    mock_db.fetch.return_value = [
        {
            "video_id": MOCK_VIDEO_ID,
            "filename": "test.mp4",
            "status": "completed",
            "created_at": "2025-12-01 10:00:00",
            "summary_text": "Patient is anxious.",
        }
    ]

    from services.reporting.main import app

    client = TestClient(app)

    response = client.get("/videos")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["video_id"] == MOCK_VIDEO_ID

    mock_db.fetch.assert_called_once()


def test_get_video_detail_success(mock_db) -> None:
    """
    Scenario: GET /videos/{id}
    Expectation: Returns full analysis including NEW fields.
    """
    from services.reporting.main import app

    client = TestClient(app)

    # Mock Summary Query (fetchrow) - UPDATED FOR NEW SCHEMA
    mock_db.fetchrow.return_value = {
        "summary_text": "Detailed summary.",
        "recommendations": '["Do breathing exercises"]',
        "cognitive_distortions": '[{"quote": "I am bad", "distortion_type": "Labeling", "explanation": "..."}]',
        "therapist_interventions": '[{"quote": "Why?", "technique": "Question", "purpose": "..."}]',
    }

    # Mock Segments Query (fetch)
    mock_db.fetch.return_value = [
        {
            "speaker_role": "patient",
            "text_content": "I feel sad.",
            "topic": "sadness",
            "emotion": "sad",
            "confidence_score": 0.99,
        }
    ]

    # Request
    response = client.get(f"/videos/{MOCK_VIDEO_ID}")

    # Assertions
    assert response.status_code == 200
    data = response.json()

    # Check new fields
    assert data["summary"] == "Detailed summary."
    assert len(data["recommendations"]) == 1
    assert data["cognitive_distortions"][0]["distortion_type"] == "Labeling"
    assert data["therapist_interventions"][0]["technique"] == "Question"

    # Check transcript
    assert len(data["transcript_segments"]) == 1


def test_get_video_detail_not_found(mock_db) -> None:
    """
    Scenario: GET /videos/{unknown_id}
    Expectation: 404 Not Found.
    """
    from services.reporting.main import app

    client = TestClient(app)

    mock_db.fetchrow.return_value = None
    mock_db.fetchval.return_value = False

    response = client.get("/videos/unknown-id")

    assert response.status_code == 404
