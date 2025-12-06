import os
import asyncio
import assemblyai as aai
from faststream import FastStream
from faststream.rabbit import RabbitBroker, RabbitQueue, RabbitMessage
from minio import Minio
from common.events import AudioExtracted, TranscriptReady
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
ASSEMBLYAI_API_KEY = os.environ["ASSEMBLYAI_API_KEY"]

# Setup Dependencies
broker = RabbitBroker(RABBITMQ_URL)
app = FastStream(broker)

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Configure AssemblyAI Global Settings
aai.settings.api_key = ASSEMBLYAI_API_KEY

def run_transcription(file_path: str):
    """
    Synchronous wrapper for AssemblyAI SDK.
    This handles upload + transcription + polling.
    """
    transcriber = aai.Transcriber()
    # ENABLE SPEAKER DIARIZATION
    config = aai.TranscriptionConfig(speaker_labels=True) 
    
    transcript = transcriber.transcribe(file_path, config)
    
    if transcript.status == aai.TranscriptStatus.error:
        logger.error("assemblyai_error", error=transcript.error)
        raise RuntimeError(f"Transcription failed: {transcript.error}")
        
    return transcript

@broker.subscriber(RabbitQueue("audio_extracted"))
async def handle_audio_extracted(event: AudioExtracted, msg: RabbitMessage):
    logger.info("transcription_started", video_id=str(event.video_id))
    
    # We need a temp file because AssemblyAI SDK uploads from disk
    temp_audio_path = f"/tmp/{event.video_id}.mp3"
    
    try:
        # Download MP3 from MinIO
        # Run in thread because MinIO client is blocking
        await asyncio.to_thread(
            minio_client.fget_object,
            "audio",
            event.audio_path,
            temp_audio_path
        )
        
        # Call AssemblyAI
        # This is a long-running blocking operation (can take minutes)
        # We run it in a thread to keep the service responsive
        transcript = await asyncio.to_thread(run_transcription, temp_audio_path)
        
        # Publish Result
        # We include the raw text AND the full JSON (for timestamps/speakers)
        output_event = TranscriptReady(
            video_id=event.video_id,
            transcript_text=transcript.text,
            transcript_json=transcript.json_response
        )
        
        # Publish to the queue that the Analyzer is listening to
        await broker.publish(output_event, queue="transcript_ready")
        
        logger.info("transcription_complete", video_id=str(event.video_id))

    except Exception as e:
        logger.error("transcription_failed", error=str(e), video_id=str(event.video_id))
        # Reject the message so we don't retry a bad file forever
        await msg.reject(requeue=False)
        
    finally:
        # Cleanup temp file
        if os.path.exists(temp_audio_path):
            os.remove(temp_audio_path)