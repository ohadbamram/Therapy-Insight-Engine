import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from common.events import VideoUploaded
from uuid import uuid4
from datetime import datetime, timezone
from typing import Generator, Tuple
from services.audio_extractor.main import handle_video_uploaded


@pytest.fixture
def sample_video_event() -> VideoUploaded:
    return VideoUploaded(
        video_id=uuid4(),
        filename="test_video.mp4",
        minio_path="videos/test_video.mp4",
        content_type="video/mp4",
        timestamp=datetime.now(timezone.utc),
    )


@pytest.fixture
def mock_rabbitmq_message() -> MagicMock:
    message = MagicMock()
    message.reject = AsyncMock()
    message.ack = AsyncMock()
    return message


@pytest.fixture
def mock_dependencies() -> (
    Generator[Tuple[MagicMock, MagicMock, MagicMock], None, None]
):
    with patch("services.audio_extractor.main.minio_client") as mock_minio, patch(
        "services.audio_extractor.main.ffmpeg"
    ) as mock_ffmpeg, patch(
        "services.audio_extractor.main.broker"
    ) as mock_broker, patch(
        "services.audio_extractor.main.os.remove"
    ):

        mock_minio.fget_object = MagicMock()
        mock_minio.fput_object = MagicMock()

        mock_stream = MagicMock()
        mock_ffmpeg.input.return_value = mock_stream
        mock_stream.output.return_value = mock_stream
        mock_stream.overwrite_output.return_value = mock_stream
        mock_stream.run = MagicMock()

        mock_broker.publish = AsyncMock()

        yield mock_minio, mock_ffmpeg, mock_broker


@pytest.mark.asyncio
async def test_handle_video_uploaded_downloads_video_from_minio(
    mock_dependencies: Tuple[MagicMock, MagicMock, MagicMock],
    sample_video_event: VideoUploaded,
    mock_rabbitmq_message: MagicMock,
) -> None:
    mock_minio, _, _ = mock_dependencies

    await handle_video_uploaded(sample_video_event, mock_rabbitmq_message)

    mock_minio.fget_object.assert_called_once()


@pytest.mark.asyncio
async def test_handle_video_uploaded_executes_ffmpeg_conversion(
    mock_dependencies: Tuple[MagicMock, MagicMock, MagicMock],
    sample_video_event: VideoUploaded,
    mock_rabbitmq_message: MagicMock,
) -> None:
    _, mock_ffmpeg, _ = mock_dependencies

    await handle_video_uploaded(sample_video_event, mock_rabbitmq_message)

    mock_ffmpeg.input.return_value.output.return_value.run.assert_called_once()


@pytest.mark.asyncio
async def test_handle_video_uploaded_uploads_audio_to_minio(
    mock_dependencies: Tuple[MagicMock, MagicMock, MagicMock],
    sample_video_event: VideoUploaded,
    mock_rabbitmq_message: MagicMock,
) -> None:
    mock_minio, _, _ = mock_dependencies

    await handle_video_uploaded(sample_video_event, mock_rabbitmq_message)

    mock_minio.fput_object.assert_called_once()


@pytest.mark.asyncio
async def test_handle_video_uploaded_publishes_event_on_success(
    mock_dependencies: Tuple[MagicMock, MagicMock, MagicMock],
    sample_video_event: VideoUploaded,
    mock_rabbitmq_message: MagicMock,
) -> None:
    _, _, mock_broker = mock_dependencies

    await handle_video_uploaded(sample_video_event, mock_rabbitmq_message)

    mock_broker.publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_handle_video_uploaded_rejects_message_on_ffmpeg_failure(
    mock_dependencies: Tuple[MagicMock, MagicMock, MagicMock],
    sample_video_event: VideoUploaded,
    mock_rabbitmq_message: MagicMock,
) -> None:
    _, mock_ffmpeg, _ = mock_dependencies
    mock_ffmpeg.input.return_value.output.return_value.run.side_effect = Exception(
        "Conversion Failed"
    )

    await handle_video_uploaded(sample_video_event, mock_rabbitmq_message)

    mock_rabbitmq_message.reject.assert_awaited_once_with(requeue=False)


@pytest.mark.asyncio
async def test_handle_video_uploaded_aborts_upload_on_ffmpeg_failure(
    mock_dependencies: Tuple[MagicMock, MagicMock, MagicMock],
    sample_video_event: VideoUploaded,
    mock_rabbitmq_message: MagicMock,
) -> None:
    mock_minio, mock_ffmpeg, _ = mock_dependencies
    mock_ffmpeg.input.return_value.output.return_value.run.side_effect = Exception(
        "Conversion Failed"
    )

    await handle_video_uploaded(sample_video_event, mock_rabbitmq_message)

    mock_minio.fput_object.assert_not_called()


@pytest.mark.asyncio
async def test_handle_video_uploaded_rejects_message_on_download_failure(
    mock_dependencies: Tuple[MagicMock, MagicMock, MagicMock],
    sample_video_event: VideoUploaded,
    mock_rabbitmq_message: MagicMock,
) -> None:
    mock_minio, _, _ = mock_dependencies
    mock_minio.fget_object.side_effect = Exception("S3 Error")

    await handle_video_uploaded(sample_video_event, mock_rabbitmq_message)

    mock_rabbitmq_message.reject.assert_awaited_once_with(requeue=False)


@pytest.mark.asyncio
async def test_handle_video_uploaded_skips_conversion_on_download_failure(
    mock_dependencies: Tuple[MagicMock, MagicMock, MagicMock],
    sample_video_event: VideoUploaded,
    mock_rabbitmq_message: MagicMock,
) -> None:
    mock_minio, mock_ffmpeg, _ = mock_dependencies
    mock_minio.fget_object.side_effect = Exception("S3 Error")

    await handle_video_uploaded(sample_video_event, mock_rabbitmq_message)

    mock_ffmpeg.input.assert_not_called()
