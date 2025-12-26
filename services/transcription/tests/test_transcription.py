import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from common.events import AudioExtracted
from uuid import uuid4
from datetime import datetime, timezone
from typing import Generator, Tuple
from services.transcription.main import handle_audio_extracted


@pytest.fixture
def sample_audio_event() -> AudioExtracted:
    return AudioExtracted(
        video_id=uuid4(),
        audio_path="audio/test_audio.mp3",
        duration_seconds=120.5,
        timestamp=datetime.now(timezone.utc),
    )


@pytest.fixture
def mock_rabbitmq_message() -> MagicMock:
    msg = MagicMock()
    msg.reject = AsyncMock()
    msg.ack = AsyncMock()
    return msg


@pytest.fixture
def mock_dependencies() -> (
    Generator[Tuple[MagicMock, MagicMock, AsyncMock], None, None]
):
    with patch("services.transcription.main.minio_client") as mock_minio, patch(
        "services.transcription.main.aai.Transcriber"
    ) as mock_transcriber_cls, patch(
        "services.transcription.main.broker"
    ) as mock_broker, patch(
        "services.transcription.main.os.remove"
    ):

        # MinIO Mocks
        mock_minio.fget_object = MagicMock()

        # AssemblyAI Mocks
        mock_transcriber_instance = mock_transcriber_cls.return_value

        # Default Success Response
        mock_transcript = MagicMock()
        mock_transcript.status = "completed"
        mock_transcript.text = "Hello world."
        mock_transcript.json_response = {"text": "Hello world.", "utterances": []}
        mock_transcript.error = None

        mock_transcriber_instance.transcribe.return_value = mock_transcript

        # Broker Mocks
        mock_broker.publish = AsyncMock()

        yield mock_minio, mock_transcriber_instance, mock_broker


@pytest.mark.asyncio
async def test_handle_audio_extracted_downloads_file_from_minio(
    mock_dependencies: Tuple[MagicMock, MagicMock, AsyncMock],
    sample_audio_event: AudioExtracted,
    mock_rabbitmq_message: MagicMock,
) -> None:
    mock_minio, _, _ = mock_dependencies

    await handle_audio_extracted(sample_audio_event, mock_rabbitmq_message)

    mock_minio.fget_object.assert_called_once()


@pytest.mark.asyncio
async def test_handle_audio_extracted_calls_transcriber(
    mock_dependencies: Tuple[MagicMock, MagicMock, AsyncMock],
    sample_audio_event: AudioExtracted,
    mock_rabbitmq_message: MagicMock,
) -> None:
    _, mock_transcriber, _ = mock_dependencies

    await handle_audio_extracted(sample_audio_event, mock_rabbitmq_message)

    mock_transcriber.transcribe.assert_called_once()


@pytest.mark.asyncio
async def test_handle_audio_extracted_enables_speaker_labels(
    mock_dependencies: Tuple[MagicMock, MagicMock, AsyncMock],
    sample_audio_event: AudioExtracted,
    mock_rabbitmq_message: MagicMock,
) -> None:
    _, mock_transcriber, _ = mock_dependencies

    await handle_audio_extracted(sample_audio_event, mock_rabbitmq_message)

    args, _ = mock_transcriber.transcribe.call_args
    assert args[1].speaker_labels is True


@pytest.mark.asyncio
async def test_handle_audio_extracted_publishes_event_on_success(
    mock_dependencies: Tuple[MagicMock, MagicMock, AsyncMock],
    sample_audio_event: AudioExtracted,
    mock_rabbitmq_message: MagicMock,
) -> None:
    _, _, mock_broker = mock_dependencies

    await handle_audio_extracted(sample_audio_event, mock_rabbitmq_message)

    mock_broker.publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_handle_audio_extracted_propagates_video_id_to_event(
    mock_dependencies: Tuple[MagicMock, MagicMock, AsyncMock],
    sample_audio_event: AudioExtracted,
    mock_rabbitmq_message: MagicMock,
) -> None:
    _, _, mock_broker = mock_dependencies

    await handle_audio_extracted(sample_audio_event, mock_rabbitmq_message)

    published_event = mock_broker.publish.call_args[0][0]
    assert published_event.video_id == sample_audio_event.video_id


@pytest.mark.asyncio
async def test_handle_audio_extracted_includes_transcript_text_in_event(
    mock_dependencies: Tuple[MagicMock, MagicMock, AsyncMock],
    sample_audio_event: AudioExtracted,
    mock_rabbitmq_message: MagicMock,
) -> None:
    _, _, mock_broker = mock_dependencies

    await handle_audio_extracted(sample_audio_event, mock_rabbitmq_message)

    published_event = mock_broker.publish.call_args[0][0]
    assert published_event.transcript_text == "Hello world."


@pytest.mark.asyncio
async def test_handle_audio_extracted_rejects_message_on_transcription_error(
    mock_dependencies: Tuple[MagicMock, MagicMock, AsyncMock],
    sample_audio_event: AudioExtracted,
    mock_rabbitmq_message: MagicMock,
) -> None:
    _, mock_transcriber, _ = mock_dependencies

    # Simulate API Error
    mock_error_transcript = MagicMock()
    mock_error_transcript.status = "error"
    mock_error_transcript.error = "Invalid audio file"
    mock_transcriber.transcribe.return_value = mock_error_transcript

    await handle_audio_extracted(sample_audio_event, mock_rabbitmq_message)

    mock_rabbitmq_message.reject.assert_awaited_once_with(requeue=False)


@pytest.mark.asyncio
async def test_handle_audio_extracted_does_not_publish_on_transcription_error(
    mock_dependencies: Tuple[MagicMock, MagicMock, AsyncMock],
    sample_audio_event: AudioExtracted,
    mock_rabbitmq_message: MagicMock,
) -> None:
    _, mock_transcriber, mock_broker = mock_dependencies

    # Simulate API Error
    mock_error_transcript = MagicMock()
    mock_error_transcript.status = "error"
    mock_error_transcript.error = "Invalid audio file"
    mock_transcriber.transcribe.return_value = mock_error_transcript

    await handle_audio_extracted(sample_audio_event, mock_rabbitmq_message)

    mock_broker.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_handle_audio_extracted_rejects_message_on_download_failure(
    mock_dependencies: Tuple[MagicMock, MagicMock, AsyncMock],
    sample_audio_event: AudioExtracted,
    mock_rabbitmq_message: MagicMock,
) -> None:
    mock_minio, _, _ = mock_dependencies
    mock_minio.fget_object.side_effect = Exception("S3 Bucket Missing")

    await handle_audio_extracted(sample_audio_event, mock_rabbitmq_message)

    mock_rabbitmq_message.reject.assert_awaited_once_with(requeue=False)


@pytest.mark.asyncio
async def test_handle_audio_extracted_skips_transcription_on_download_failure(
    mock_dependencies: Tuple[MagicMock, MagicMock, AsyncMock],
    sample_audio_event: AudioExtracted,
    mock_rabbitmq_message: MagicMock,
) -> None:
    mock_minio, mock_transcriber, _ = mock_dependencies
    mock_minio.fget_object.side_effect = Exception("S3 Bucket Missing")

    await handle_audio_extracted(sample_audio_event, mock_rabbitmq_message)

    mock_transcriber.transcribe.assert_not_called()
