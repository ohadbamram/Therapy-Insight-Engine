import pytest
from unittest.mock import AsyncMock, patch
from fastapi.testclient import TestClient

# Mock Data
MOCK_VIDEO_ID = "d41ae112-e5d2-4736-be32-ac01aac266b5"

@pytest.fixture
def mock_db():
    with patch("services.reporting.main.asyncpg.connect") as mock_connect:
        mock_conn = AsyncMock()
        mock_connect.return_value = mock_conn
        yield mock_conn

def test_list_videos(mock_db):
    """
    Scenario: GET /videos
    Expectation: Returns list of videos from DB.
    """
    # Mock DB Response
    mock_db.fetch.return_value = [
        {
            "video_id": MOCK_VIDEO_ID,
            "filename": "test.mp4",
            "status": "completed",
            "created_at": "2025-12-01 10:00:00",
            "summary_text": "Patient is anxious."
        }
    ]

    # Import App (After env vars set)
    from services.reporting.main import app
    client = TestClient(app)

    # Request
    response = client.get("/videos")

    # Assertions
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["video_id"] == MOCK_VIDEO_ID
    assert data[0]["summary_text"] == "Patient is anxious."
    
    # Verify SQL query
    mock_db.fetch.assert_called_once()
    assert "SELECT" in mock_db.fetch.call_args[0][0]

def test_get_video_detail_success(mock_db):
    """
    Scenario: GET /videos/{id}
    Expectation: Returns full analysis (Summary + Transcript).
    """
    from services.reporting.main import app
    client = TestClient(app)

    # Mock Summary Query (fetchrow)
    mock_db.fetchrow.return_value = {
        "summary_text": "Detailed summary.",
        "sentiment_trend": '[{"time": 0, "score": -1}]'
    }

    # Mock Segments Query (fetch)
    mock_db.fetch.return_value = [
        {
            "speaker_role": "patient",
            "text_content": "I feel sad.",
            "topic": "sadness",
            "emotion": "sad",
            "confidence_score": 0.99
        }
    ]

    # Request
    response = client.get(f"/videos/{MOCK_VIDEO_ID}")

    # Assertions
    assert response.status_code == 200
    data = response.json()
    assert data["video_id"] == MOCK_VIDEO_ID
    assert data["summary"] == "Detailed summary."
    assert len(data["transcript_segments"]) == 1
    assert data["transcript_segments"][0]["text_content"] == "I feel sad."

def test_get_video_detail_not_found(mock_db):
    """
    Scenario: GET /videos/{unknown_id}
    Expectation: 404 Not Found.
    """
    from services.reporting.main import app
    client = TestClient(app)

    # Mock DB returning None (No summary found)
    mock_db.fetchrow.return_value = None

    # Request
    response = client.get("/videos/unknown-id")

    # Assertions
    assert response.status_code == 404
    assert response.json()["detail"] == "Analysis not found (or processing)"