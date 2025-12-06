import os
import asyncio
import ffmpeg
from faststream import FastStream
from faststream.rabbit import RabbitBroker, RabbitQueue, RabbitMessage
from minio import Minio
from common.events import VideoUploaded, AudioExtracted
from common.logger import init_logging, get_logger

# Initialize Logging
init_logging()
logger = get_logger(__name__)

# Configuration
RABBITMQ_USER = os.getenv('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_PORT = os.getenv('RABBITMQ_PORT')
RABBITMQ_VHOST = os.getenv('RABBITMQ_VHOST')
RABBITMQ_URL = f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}{RABBITMQ_VHOST}"
MINIO_HOST = os.getenv('MINIO_HOST')
MINIO_PORT = os.getenv('MINIO_PORT')
MINIO_ENDPOINT = f"{MINIO_HOST}:{MINIO_PORT}"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

# Setup Dependencies
broker = RabbitBroker(RABBITMQ_URL)
app = FastStream(broker)

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def run_ffmpeg(input_path: str, output_path: str):
    """
    Runs FFmpeg synchronously. 
    We wrap this in to_thread later to avoid blocking the event loop.
    """
    try:
        (
            ffmpeg
            .input(input_path)
            .output(output_path, acodec='libmp3lame', audio_bitrate='128k', loglevel='error')
            .overwrite_output()
            .run(capture_stdout=True, capture_stderr=True)
        )
    except ffmpeg.Error as e:
        # Log the stderr from ffmpeg to understand why it failed
        error_log = e.stderr.decode('utf8') if e.stderr else str(e)
        raise RuntimeError(f"FFmpeg failed: {error_log}")

@broker.subscriber(RabbitQueue("video_processing"))
async def handle_video_uploaded(event: VideoUploaded, msg: RabbitMessage):
    logger.info("audio_extraction_started", video_id=str(event.video_id))
    
    # Define temp file paths
    # We use /tmp which is writable in the container
    temp_video_path = f"/tmp/{event.video_id}_input.mp4"
    temp_audio_path = f"/tmp/{event.video_id}_output.mp3"
    
    try:
        # Download Video from MinIO
        # Using to_thread because MinIO is blocking
        await asyncio.to_thread(
            minio_client.fget_object,
            "videos",
            event.minio_path,
            temp_video_path
        )
        
        # Extract Audio (CPU Bound work)
        # Using to_thread to keep the event loop responsive
        await asyncio.to_thread(run_ffmpeg, temp_video_path, temp_audio_path)
        
        # Upload Audio to MinIO
        audio_filename = f"{event.video_id}/audio.mp3"
        await asyncio.to_thread(
            minio_client.fput_object,
            "audio",
            audio_filename,
            temp_audio_path
        )
        
        # Publish Event
        output_event = AudioExtracted(
            video_id=event.video_id,
            audio_path=audio_filename
        )
        # We publish to the NEXT queue: 'audio_extracted'
        # The Transcription service will listen to this.
        await broker.publish(output_event, queue="audio_extracted")
        
        logger.info("audio_extraction_complete", video_id=str(event.video_id))

    except Exception as e:
        logger.error("audio_extraction_failed", error=str(e), video_id=str(event.video_id))
        # Reject message so it doesn't loop forever
        await msg.reject(requeue=False)
        
    finally:
        # Cleanup Temp Files (Always run this)
        if os.path.exists(temp_video_path):
            os.remove(temp_video_path)
        if os.path.exists(temp_audio_path):
            os.remove(temp_audio_path)