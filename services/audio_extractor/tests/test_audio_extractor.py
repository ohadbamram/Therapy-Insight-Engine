import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from common.events import VideoUploaded
from uuid import uuid4
from datetime import datetime, timezone

# Sample Input Event
@pytest.fixture
def sample_event():
    return VideoUploaded(
        video_id=uuid4(),
        filename="test_video.mp4",
        minio_path="videos/test_video.mp4",
        content_type="video/mp4",
        timestamp=datetime.now(timezone.utc)
    )

# Mock Rabbit Message
@pytest.fixture
def mock_rabbit_msg():
    msg = MagicMock()
    msg.reject = AsyncMock()
    msg.ack = AsyncMock()
    return msg

# Mock Dependencies (MinIO, FFmpeg, Broker)
@pytest.fixture
def mock_dependencies():
    with patch("services.audio_extractor.main.minio_client") as mock_minio, \
         patch("services.audio_extractor.main.ffmpeg") as mock_ffmpeg, \
         patch("services.audio_extractor.main.broker") as mock_broker, \
         patch("services.audio_extractor.main.os.remove") as mock_os_remove:
        
        # MinIO Mocks
        mock_minio.fget_object = MagicMock()
        mock_minio.fput_object = MagicMock()
        
        # FFmpeg Mocks
        mock_stream = MagicMock()
        mock_ffmpeg.input.return_value = mock_stream
        mock_stream.output.return_value = mock_stream
        
        # FIX: Add overwrite_output to the chain so it returns the same mock_stream
        mock_stream.overwrite_output.return_value = mock_stream
        
        mock_stream.run = MagicMock()

        # Broker Mocks
        mock_broker.publish = AsyncMock()

        yield mock_minio, mock_ffmpeg, mock_broker

@pytest.mark.asyncio
async def test_extract_audio_success(mock_dependencies, sample_event, mock_rabbit_msg):
    """
    Happy Path: Download -> Convert -> Upload -> Publish Event
    """
    mock_minio, mock_ffmpeg, mock_broker = mock_dependencies
    
    from services.audio_extractor.main import handle_video_uploaded
    
    await handle_video_uploaded(sample_event, mock_rabbit_msg)

    # Verify Download (Raw video from MinIO)
    mock_minio.fget_object.assert_called_once()
    
    # Verify Conversion (FFmpeg run was called)
    mock_ffmpeg.input.assert_called()
    mock_ffmpeg.input.return_value.output.assert_called()
    mock_ffmpeg.input.return_value.output.return_value.run.assert_called_once()
    
    # Verify Upload (MP3 to MinIO)
    mock_minio.fput_object.assert_called_once()
    args, _ = mock_minio.fput_object.call_args
    assert args[0] == "audio" # Bucket name
    assert args[1].endswith(".mp3") # Object name
    
    # Verify Publish (AudioExtracted event)
    mock_broker.publish.assert_awaited_once()
    # Check the event type
    published_event = mock_broker.publish.call_args[0][0]
    assert published_event.video_id == sample_event.video_id
    assert published_event.audio_path.endswith(".mp3")

@pytest.mark.asyncio
async def test_ffmpeg_failure(mock_dependencies, sample_event, mock_rabbit_msg):
    """
    Scenario: FFmpeg fails (corrupt video).
    Expectation: Log Error -> Reject Message (Don't retry infinitely).
    """
    mock_minio, mock_ffmpeg, mock_broker = mock_dependencies
    
    # Make FFmpeg raise an error
    mock_ffmpeg.Error = Exception # FFmpeg library defines its own Error class
    mock_ffmpeg.input.return_value.output.return_value.run.side_effect = Exception("Conversion Failed")

    from services.audio_extractor.main import handle_video_uploaded
    
    await handle_video_uploaded(sample_event, mock_rabbit_msg)

    # Verify we did NOT upload anything
    mock_minio.fput_object.assert_not_called()
    # Verify we did NOT publish success event
    mock_broker.publish.assert_not_awaited()
    # Verify Message Rejected
    mock_rabbit_msg.reject.assert_awaited_once_with(requeue=False)

@pytest.mark.asyncio
async def test_minio_download_failure(mock_dependencies, sample_event, mock_rabbit_msg):
    """
    Scenario: MinIO download fails (file missing).
    Expectation: Reject Message.
    """
    mock_minio, mock_ffmpeg, mock_broker = mock_dependencies
    
    mock_minio.fget_object.side_effect = Exception("S3 Error")

    from services.audio_extractor.main import handle_video_uploaded
    
    await handle_video_uploaded(sample_event, mock_rabbit_msg)

    # Verify we never even tried FFmpeg
    mock_ffmpeg.input.assert_not_called()
    # Verify Message Rejected
    mock_rabbit_msg.reject.assert_awaited_once_with(requeue=False)