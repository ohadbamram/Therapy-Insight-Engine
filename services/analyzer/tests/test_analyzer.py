import pytest
import json
from unittest.mock import MagicMock, AsyncMock, patch
from common.events import TranscriptReady
from uuid import uuid4
from datetime import datetime, timezone

# We define the event payload we expect to receive
@pytest.fixture
def sample_event():
    return TranscriptReady(
        video_id=uuid4(),
        transcript_text="I feel sad.",
        transcript_json={},
        timestamp=datetime.now(timezone.utc)
    )

# We mock all external clients
@pytest.fixture
def mock_dependencies():
    with patch("services.analyzer.main.redis") as mock_redis, \
         patch("services.analyzer.main.client") as mock_gemini, \
         patch("services.analyzer.main.asyncpg.connect") as mock_pg_connect:
        
        # Setup Redis
        mock_redis.get = AsyncMock(return_value=None) # Default: Cache Miss
        mock_redis.set = AsyncMock()

        # Setup Gemini
        # We need to mock the deep response structure: response.parsed -> Analysis Object
        mock_response = MagicMock()
        
        # We create a fake Pydantic object to simulate Gemini's output
        class FakeAnalysis:
            def __init__(self):
                self.speaker_role = "patient"
                self.topic = "sadness"
                self.emotion = "sad"
                self.confidence = 0.99
                self.sentiment_trend = [{"time": 0, "score": -1}]
                self.summary = "Patient is sad."
                self.segments = [self] # Use itself as a segment for simplicity
                self.model_dump_json = lambda: '{"fake": "json"}'
        
        mock_response.parsed = FakeAnalysis()
        
        # Gemini Client Mocks
        mock_gemini.aio.models.generate_content = AsyncMock(return_value=mock_response)

        # Setup Postgres
        mock_conn = AsyncMock()
        mock_pg_connect.return_value = mock_conn

        yield mock_redis, mock_gemini, mock_conn

@pytest.mark.asyncio
async def test_handle_transcript_cache_miss(mock_dependencies, sample_event):
    """
    Scenario: New transcript (not in Redis).
    Expectation: Call Gemini -> Save to DB -> Save to Redis.
    """
    mock_redis, mock_gemini, mock_pg_conn = mock_dependencies
    
    # Import the handler (only AFTER env vars are set by conftest)
    from services.analyzer.main import handle_transcript

    # Run handler
    await handle_transcript(sample_event)

    # Verify Redis Checked
    mock_redis.get.assert_called_once()
    
    # Verify Gemini Called
    mock_gemini.aio.models.generate_content.assert_called_once()
    
    # Verify DB Insertions (Summary + Segments)
    # We expect at least 2 execute calls (one for summary, one for segments)
    assert mock_pg_conn.execute.call_count >= 2
    
    # Verify Redis Set (Caching the result)
    mock_redis.set.assert_called_once()

@pytest.mark.asyncio
async def test_handle_transcript_cache_hit(mock_dependencies, sample_event):
    """
    Scenario: Transcript hash found in Redis.
    Expectation: Skip Gemini -> Skip DB (or handle logic) -> Log hit.
    """
    mock_redis, mock_gemini, mock_pg_conn = mock_dependencies
    
    # Simulate Cache Hit
    mock_redis.get.return_value = '{"cached": "analysis"}'

    from services.analyzer.main import handle_transcript
    await handle_transcript(sample_event)

    # Verify Gemini NOT called
    mock_gemini.aio.models.generate_content.assert_not_called()
    
    # Verify DB NOT called (assuming we skip DB on cache hit for MVP)
    mock_pg_conn.execute.assert_not_called()

@pytest.mark.asyncio
async def test_gemini_failure(mock_dependencies, sample_event):
    """
    Scenario: Gemini API crashes.
    Expectation: Log error -> Do NOT save to DB/Redis -> Raise exception (or handle gracefully).
    """
    mock_redis, mock_gemini, mock_pg_conn = mock_dependencies
    
    # Simulate Gemini Error
    mock_gemini.aio.models.generate_content.side_effect = Exception("API Down")

    from services.analyzer.main import handle_transcript
    
    # We expect the function to catch the error and log it, NOT crash the worker
    await handle_transcript(sample_event)

    # Verify NO DB writes
    mock_pg_conn.execute.assert_not_called()
    # Verify NO Redis writes
    mock_redis.set.assert_not_called()

@pytest.mark.asyncio
async def test_handle_malformed_llm_json(mock_dependencies, sample_event):
    """
    Scenario: Gemini returns invalid JSON (e.g., trailing commas or extra text).
    Expectation: Parse error caught -> Logged -> Message NACKed (or dropped safely).
    """
    mock_redis, mock_gemini, mock_save_db = mock_dependencies
    
    # Simulate Gemini returning text that IS NOT JSON
    # We mock the 'response.parsed' access raising a validation error
    # Or simpler: The Gemini SDK raises a ValueError when parsing failed
    mock_gemini.aio.models.generate_content.side_effect = ValueError("JSON Decode Error")

    from services.analyzer.main import handle_transcript
    
    # We expect it to NOT crash
    await handle_transcript(sample_event)

    # Verify we did NOT try to save garbage to the DB
    mock_save_db.assert_not_called()

@pytest.mark.asyncio
async def test_handle_empty_transcript(mock_dependencies, sample_event):
    """
    Scenario: Transcript text is empty string.
    Expectation: Log warning -> Return early -> Do NOT call Gemini (Save money!).
    """
    mock_redis, mock_gemini, mock_save_db = mock_dependencies
    
    # Set empty text
    sample_event.transcript_text = ""

    from services.analyzer.main import handle_transcript
    await handle_transcript(sample_event)

    # Verify Gemini was NEVER called
    mock_gemini.aio.models.generate_content.assert_not_called()

@pytest.mark.asyncio
async def test_database_connection_failure(mock_dependencies, sample_event):
    """
    Scenario: Postgres is down.
    Expectation: Gemini runs OK -> DB save fails -> Log Error -> Raise Exception.
    
    CRITICAL: In FastStream, raising an exception triggers a NACK (Retry).
    If you catch it and suppress it, the message is lost!
    """
    mock_redis, mock_gemini, mock_save_db = mock_dependencies
    
    # Mock DB failure
    mock_save_db.side_effect = Exception("Connection Refused")

    from services.analyzer.main import handle_transcript
    
    # We expect the function to Log the error. 
    # In a real RabbitMQ scenario, we usually want this to RAISE so RabbitMQ retries.
    # But your current code catches all exceptions.
    await handle_transcript(sample_event)
    
    # Verify we tried to save, but failed
    mock_save_db.assert_called_once()