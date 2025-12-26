import pytest
import json
from unittest.mock import MagicMock, AsyncMock, patch
from common.events import TranscriptReady
from uuid import uuid4
from datetime import datetime, timezone
from typing import Generator, Tuple
from services.analyzer.main import handle_transcript


@pytest.fixture
def sample_transcript_event() -> TranscriptReady:
    return TranscriptReady(
        video_id=uuid4(),
        transcript_text="I feel sad.",
        transcript_json={},
        speaker_segments=None,
        timestamp=datetime.now(timezone.utc),
    )


@pytest.fixture
def mock_rabbitmq_message() -> MagicMock:
    message = MagicMock()
    message.reject = AsyncMock()
    message.ack = AsyncMock()
    return message


@pytest.fixture
def valid_analysis_json() -> str:
    return json.dumps(
        {
            "video_id": "test_id",
            "summary": "Patient is sad.",
            "recommendations": ["Do homework"],
            "cognitive_distortions": [
                {
                    "quote": "I am a failure",
                    "distortion_type": "Labeling",
                    "explanation": "Self-labeling",
                }
            ],
            "therapist_interventions": [
                {
                    "quote": "Tell me more",
                    "technique": "Open Question",
                    "purpose": "Exploration",
                }
            ],
            "segments": [
                {
                    "text": "I feel very sad today.",
                    "speaker_role": "patient",
                    "topic": "sadness",
                    "emotion": "sad",
                    "confidence": 0.99,
                }
            ],
        }
    )


@pytest.fixture
def mock_dependencies(
    valid_analysis_json: str,
) -> Generator[Tuple[AsyncMock, MagicMock, AsyncMock], None, None]:
    with patch("services.analyzer.main.redis") as mock_redis, patch(
        "services.analyzer.main.client"
    ) as mock_gemini, patch(
        "services.analyzer.main.asyncpg.connect"
    ) as mock_postgres_connect:

        mock_redis.get = AsyncMock(return_value=None)
        mock_redis.set = AsyncMock()

        mock_llm_response = MagicMock()
        mock_llm_response.text = valid_analysis_json
        mock_gemini.aio.models.generate_content = AsyncMock(
            return_value=mock_llm_response
        )

        mock_db_connection = AsyncMock()
        mock_postgres_connect.return_value = mock_db_connection

        yield mock_redis, mock_gemini, mock_postgres_connect


@pytest.mark.asyncio
async def test_handle_transcript_queries_llm_on_cache_miss(
    mock_dependencies: Tuple[AsyncMock, MagicMock, AsyncMock],
    sample_transcript_event: TranscriptReady,
    mock_rabbitmq_message: MagicMock,
) -> None:
    _, mock_gemini, _ = mock_dependencies

    await handle_transcript(sample_transcript_event, mock_rabbitmq_message)

    mock_gemini.aio.models.generate_content.assert_called_once()


@pytest.mark.asyncio
async def test_handle_transcript_saves_analysis_to_database_on_success(
    mock_dependencies: Tuple[AsyncMock, MagicMock, AsyncMock],
    sample_transcript_event: TranscriptReady,
    mock_rabbitmq_message: MagicMock,
) -> None:
    _, _, mock_postgres_connect = mock_dependencies

    await handle_transcript(sample_transcript_event, mock_rabbitmq_message)

    assert mock_postgres_connect.return_value.execute.called


@pytest.mark.asyncio
async def test_handle_transcript_caches_analysis_in_redis_on_success(
    mock_dependencies: Tuple[AsyncMock, MagicMock, AsyncMock],
    sample_transcript_event: TranscriptReady,
    mock_rabbitmq_message: MagicMock,
) -> None:
    mock_redis, _, _ = mock_dependencies

    await handle_transcript(sample_transcript_event, mock_rabbitmq_message)

    mock_redis.set.assert_called_once()


@pytest.mark.asyncio
async def test_handle_transcript_skips_llm_on_cache_hit(
    mock_dependencies: Tuple[AsyncMock, MagicMock, AsyncMock],
    sample_transcript_event: TranscriptReady,
    mock_rabbitmq_message: MagicMock,
    valid_analysis_json: str,
) -> None:
    mock_redis, mock_gemini, _ = mock_dependencies
    mock_redis.get.return_value = valid_analysis_json

    await handle_transcript(sample_transcript_event, mock_rabbitmq_message)

    mock_gemini.aio.models.generate_content.assert_not_called()


@pytest.mark.asyncio
async def test_handle_transcript_ensures_database_consistency_on_cache_hit(
    mock_dependencies: Tuple[AsyncMock, MagicMock, AsyncMock],
    sample_transcript_event: TranscriptReady,
    mock_rabbitmq_message: MagicMock,
    valid_analysis_json: str,
) -> None:
    mock_redis, _, mock_postgres_connect = mock_dependencies
    mock_redis.get.return_value = valid_analysis_json

    await handle_transcript(sample_transcript_event, mock_rabbitmq_message)

    assert mock_postgres_connect.return_value.execute.called


@pytest.mark.asyncio
async def test_handle_transcript_rejects_message_on_llm_failure(
    mock_dependencies: Tuple[AsyncMock, MagicMock, AsyncMock],
    sample_transcript_event: TranscriptReady,
    mock_rabbitmq_message: MagicMock,
) -> None:
    _, mock_gemini, _ = mock_dependencies
    mock_gemini.aio.models.generate_content.side_effect = Exception("API Down")

    await handle_transcript(sample_transcript_event, mock_rabbitmq_message)

    mock_rabbitmq_message.reject.assert_awaited_once_with(requeue=False)


@pytest.mark.asyncio
async def test_handle_transcript_does_not_save_corrupted_data_on_llm_failure(
    mock_dependencies: Tuple[AsyncMock, MagicMock, AsyncMock],
    sample_transcript_event: TranscriptReady,
    mock_rabbitmq_message: MagicMock,
) -> None:
    _, mock_gemini, mock_postgres_connect = mock_dependencies
    mock_gemini.aio.models.generate_content.side_effect = Exception("API Down")

    await handle_transcript(sample_transcript_event, mock_rabbitmq_message)

    mock_postgres_connect.assert_not_called()


@pytest.mark.asyncio
async def test_handle_transcript_rejects_message_on_malformed_llm_response(
    mock_dependencies: Tuple[AsyncMock, MagicMock, AsyncMock],
    sample_transcript_event: TranscriptReady,
    mock_rabbitmq_message: MagicMock,
) -> None:
    _, mock_gemini, _ = mock_dependencies
    mock_gemini.aio.models.generate_content.side_effect = ValueError(
        "JSON Decode Error"
    )

    await handle_transcript(sample_transcript_event, mock_rabbitmq_message)

    mock_rabbitmq_message.reject.assert_awaited_once_with(requeue=False)


@pytest.mark.asyncio
async def test_handle_transcript_skips_processing_for_empty_transcript(
    mock_dependencies: Tuple[AsyncMock, MagicMock, AsyncMock],
    sample_transcript_event: TranscriptReady,
    mock_rabbitmq_message: MagicMock,
) -> None:
    _, mock_gemini, _ = mock_dependencies
    sample_transcript_event.transcript_text = ""

    await handle_transcript(sample_transcript_event, mock_rabbitmq_message)

    mock_gemini.aio.models.generate_content.assert_not_called()


@pytest.mark.asyncio
async def test_handle_transcript_rejects_message_on_database_connection_failure(
    mock_dependencies: Tuple[AsyncMock, MagicMock, AsyncMock],
    sample_transcript_event: TranscriptReady,
    mock_rabbitmq_message: MagicMock,
) -> None:
    _, _, mock_postgres_connect = mock_dependencies
    mock_postgres_connect.side_effect = Exception("Connection Refused")

    await handle_transcript(sample_transcript_event, mock_rabbitmq_message)

    mock_rabbitmq_message.reject.assert_awaited_once_with(requeue=False)
