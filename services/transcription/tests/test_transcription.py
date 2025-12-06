import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from common.events import AudioExtracted
from uuid import uuid4
from datetime import datetime, timezone

@pytest.fixture
def sample_event():
    return AudioExtracted(
        video_id=uuid4(),
        audio_path="audio/test_audio.mp3",
        timestamp=datetime.now(timezone.utc)
    )

@pytest.fixture
def mock_rabbit_msg():
    msg = MagicMock()
    msg.reject = AsyncMock()
    msg.ack = AsyncMock()
    return msg

@pytest.fixture
def mock_dependencies():
    with patch("services.transcription.main.minio_client") as mock_minio, \
         patch("services.transcription.main.aai.Transcriber") as mock_transcriber_cls, \
         patch("services.transcription.main.broker") as mock_broker, \
         patch("services.transcription.main.os.remove") as mock_os_remove:
        
        # MinIO Mocks
        mock_minio.fget_object = MagicMock()
        
        # AssemblyAI Mocks
        mock_transcriber_instance = mock_transcriber_cls.return_value
        
        # Create a fake transcript object
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
async def test_transcription_success(mock_dependencies, sample_event, mock_rabbit_msg):
    """Happy Path: Download -> Transcribe (Success) -> Publish Event"""
    mock_minio, mock_transcriber, mock_broker = mock_dependencies
    
    from services.transcription.main import handle_audio_extracted
    
    await handle_audio_extracted(sample_event, mock_rabbit_msg)

    # Verify Download
    mock_minio.fget_object.assert_called_once()
    
    # Verify Transcription Called
    args, _ = mock_transcriber.transcribe.call_args
    config_object = args[1]
    assert config_object.speaker_labels is True
    
    # Verify Publish
    mock_broker.publish.assert_awaited_once()
    published_event = mock_broker.publish.call_args[0][0]
    assert published_event.video_id == sample_event.video_id
    assert published_event.transcript_text == "Hello world."

@pytest.mark.asyncio
async def test_transcription_api_error(mock_dependencies, sample_event, mock_rabbit_msg):
    """Scenario: AssemblyAI returns an error status."""
    mock_minio, mock_transcriber, mock_broker = mock_dependencies
    
    # Mock an error response
    mock_transcript = MagicMock()
    mock_transcript.status = "error"
    mock_transcript.error = "Invalid audio file"
    mock_transcriber.transcribe.return_value = mock_transcript

    from services.transcription.main import handle_audio_extracted
    
    await handle_audio_extracted(sample_event, mock_rabbit_msg)

    # Verify Rejection
    mock_broker.publish.assert_not_awaited()
    mock_rabbit_msg.reject.assert_awaited_once_with(requeue=False)

@pytest.mark.asyncio
async def test_minio_download_failure(mock_dependencies, sample_event, mock_rabbit_msg):
    """Scenario: MinIO fails to download audio."""
    mock_minio, mock_transcriber, mock_broker = mock_dependencies
    
    mock_minio.fget_object.side_effect = Exception("S3 Bucket Missing")

    from services.transcription.main import handle_audio_extracted
    
    await handle_audio_extracted(sample_event, mock_rabbit_msg)

    # Verify Transcriber NEVER called
    mock_transcriber.transcribe.assert_not_called()
    # Verify Rejection
    mock_rabbit_msg.reject.assert_awaited_once_with(requeue=False)