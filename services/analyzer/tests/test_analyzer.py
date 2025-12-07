import pytest
import json
from unittest.mock import MagicMock, AsyncMock, patch
from common.events import TranscriptReady
from uuid import uuid4
from datetime import datetime, timezone
from typing import Generator, Tuple

# We define the event payload we expect to receive
@pytest.fixture
def sample_event() -> TranscriptReady:
    return TranscriptReady(
        video_id=uuid4(),
        transcript_text="I feel sad.",
        transcript_json={},
        timestamp=datetime.now(timezone.utc)
    )

@pytest.fixture
def mock_rabbit_msg() -> MagicMock:
    msg = MagicMock()
    msg.reject = AsyncMock()
    msg.ack = AsyncMock()
    return msg

# We mock all external clients
@pytest.fixture
def mock_dependencies() -> Generator[Tuple, None, None]:
    with patch("services.analyzer.main.redis") as mock_redis, \
         patch("services.analyzer.main.client") as mock_gemini, \
         patch("services.analyzer.main.asyncpg.connect") as mock_pg_connect:
        
        # Setup Redis
        mock_redis.get = AsyncMock(return_value=None) # Default: Cache Miss
        mock_redis.set = AsyncMock()

        # Setup Gemini
        mock_response = MagicMock()
        
        mock_json_string = json.dumps({
            "video_id": "test_id",
            "summary": "Patient is sad.",
            "recommendations": ["Do homework"],
            "cognitive_distortions": [
                {"quote": "I am a failure", "distortion_type": "Labeling", "explanation": "Self-labeling"}
            ],
            "therapist_interventions": [
                {"quote": "Tell me more", "technique": "Open Question", "purpose": "Exploration"}
            ],
            "segments": [{
                "text": "I feel very sad today.",
                "speaker_role": "patient",
                "topic": "sadness",
                "emotion": "sad",
                "confidence": 0.99
            }]
        })
        mock_response.text = mock_json_string
        mock_gemini.aio.models.generate_content = AsyncMock(return_value=mock_response)

        # Setup Postgres
        mock_conn = AsyncMock()
        mock_pg_connect.return_value = mock_conn

        # Yield the CONNECTION FUNCTION mock, not the connection object
        yield mock_redis, mock_gemini, mock_pg_connect

@pytest.mark.asyncio
async def test_handle_transcript_cache_miss(mock_dependencies, sample_event, mock_rabbit_msg) -> None:
    """
    Scenario: New transcript (not in Redis).
    Expectation: Call Gemini -> Save to DB -> Save to Redis.
    """
    mock_redis, mock_gemini, mock_pg_connect = mock_dependencies
    
    from services.analyzer.main import handle_transcript

    await handle_transcript(sample_event, mock_rabbit_msg)

    # Verify Redis Checked
    mock_redis.get.assert_called_once()
    
    # Verify Gemini Called
    mock_gemini.aio.models.generate_content.assert_called_once()
    
    # Verify DB Connection happened
    mock_pg_connect.assert_called()
    
    # Verify DB Insertions (Access the return_value which is the mock_conn)
    # We expect at least 2 execute calls (summary + segments)
    assert mock_pg_connect.return_value.execute.call_count >= 2
    
    # Verify Redis Set
    mock_redis.set.assert_called_once()

@pytest.mark.asyncio
async def test_handle_transcript_cache_hit(mock_dependencies, sample_event, mock_rabbit_msg) -> None:
    """
    Scenario: Transcript hash found in Redis.
    Expectation: Skip Gemini -> Skip DB -> Log hit.
    """
    mock_redis, mock_gemini, mock_pg_connect = mock_dependencies
    
    mock_redis.get.return_value = '{"cached": "analysis"}'

    from services.analyzer.main import handle_transcript
    await handle_transcript(sample_event, mock_rabbit_msg)

    mock_gemini.aio.models.generate_content.assert_not_called()
    mock_pg_connect.assert_not_called()

@pytest.mark.asyncio
async def test_gemini_failure(mock_dependencies, sample_event, mock_rabbit_msg) -> None:
    """
    Scenario: Gemini API crashes.
    Expectation: Log error -> Do NOT save to DB/Redis -> Reject Message.
    """
    mock_redis, mock_gemini, mock_pg_connect = mock_dependencies
    
    mock_gemini.aio.models.generate_content.side_effect = Exception("API Down")

    from services.analyzer.main import handle_transcript
    
    await handle_transcript(sample_event, mock_rabbit_msg)

    mock_pg_connect.assert_not_called()
    mock_redis.set.assert_not_called()
    mock_rabbit_msg.reject.assert_awaited_once_with(requeue=False)

@pytest.mark.asyncio
async def test_handle_malformed_llm_json(mock_dependencies, sample_event, mock_rabbit_msg) -> None:
    """
    Scenario: Gemini returns invalid JSON.
    Expectation: Parse error caught -> Reject Message.
    """
    mock_redis, mock_gemini, mock_pg_connect = mock_dependencies
    
    mock_gemini.aio.models.generate_content.side_effect = ValueError("JSON Decode Error")

    from services.analyzer.main import handle_transcript
    
    await handle_transcript(sample_event, mock_rabbit_msg)

    mock_pg_connect.assert_not_called()
    mock_rabbit_msg.reject.assert_awaited_once_with(requeue=False)

@pytest.mark.asyncio
async def test_handle_empty_transcript(mock_dependencies, sample_event, mock_rabbit_msg) -> None:
    """
    Scenario: Transcript text is empty string.
    Expectation: Log warning -> Return early -> Do NOT call Gemini.
    """
    mock_redis, mock_gemini, mock_pg_connect = mock_dependencies
    
    sample_event.transcript_text = ""

    from services.analyzer.main import handle_transcript
    await handle_transcript(sample_event, mock_rabbit_msg)

    mock_gemini.aio.models.generate_content.assert_not_called()

@pytest.mark.asyncio
async def test_database_connection_failure(mock_dependencies, sample_event, mock_rabbit_msg) -> None:
    """
    Scenario: Postgres is down.
    Expectation: Gemini runs OK -> DB connect fails -> Log Error -> Reject Message.
    """
    mock_redis, mock_gemini, mock_pg_connect = mock_dependencies
    
    # Mock DB connection failure
    mock_pg_connect.side_effect = Exception("Connection Refused")

    from services.analyzer.main import handle_transcript
    
    await handle_transcript(sample_event, mock_rabbit_msg)
    
    # Verify we tried to connect
    mock_pg_connect.assert_called_once()
    
    # Verify message was rejected
    mock_rabbit_msg.reject.assert_awaited_once_with(requeue=False)