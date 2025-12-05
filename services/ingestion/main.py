import uuid
import os
import asyncio
from fastapi import FastAPI, UploadFile, File, HTTPException
from faststream.rabbit import RabbitBroker, RabbitQueue
from minio import Minio
from contextlib import asynccontextmanager
from common.events import VideoUploaded
from common.logger import init_logging, get_logger


# Initialize Logging (Runs once)
init_logging()
logger = get_logger(__name__)

app = FastAPI(title="Therapy Ingestion Service")

# Connection Details (From Environment Variables)
RABBITMQ_URL = os.getenv("RABBITMQ_URL")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

# Setup Connections
broker = RabbitBroker(RABBITMQ_URL)

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for startup and shutdown events.
    Code before 'yield' runs on startup.
    Code after 'yield' runs on shutdown.
    """
    # Startup Logic
    logger.info("service_startup")
    await broker.connect()
    await broker.declare_queue(RabbitQueue("video_processing"))
    
    # Ensure MinIO bucket exists
    bucket_name = "videos"
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        logger.info("bucket_created", bucket=bucket_name)
    
    yield # The application runs here
    
    # Shutdown Logic
    logger.info("service_shutdown")
    await broker.disconnect()

# Initialize FastAPI with lifespan
app = FastAPI(
    title="Therapy Ingestion Service",
    lifespan=lifespan
)

@app.post("/upload")
async def upload_video(file: UploadFile = File(...)):
    """
    1. Generate UUID
    2. Save to MinIO
    3. Publish Event
    """
    video_id = str(uuid.uuid4())
    file_path = f"{video_id}/{file.filename}"
    
    logger.info("upload_started", video_id=video_id, filename=file.filename)

    try:
        # Save file to MinIO
        await asyncio.to_thread(
            minio_client.put_object,
            "videos", 
            file_path, 
            file.file, 
            length=-1, 
            part_size=10*1024*1024
        )
        
        # Create Event
        event = VideoUploaded(
            video_id=video_id, 
            filename=file_path,
            minio_path=file_path, 
            content_type=file.content_type
        )
        
        # Publish to RabbitMQ
        await broker.publish(event, queue="video_processing")
        
        logger.info("upload_success", video_id=video_id)
        return {"status": "queued", "video_id": video_id}

    except Exception as e:
        logger.error("upload_failed", error=str(e), video_id=video_id)
        raise HTTPException(status_code=500, detail="Upload failed")