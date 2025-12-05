import os
import json
import hashlib
import asyncpg
from redis import asyncio as aioredis
from faststream import FastStream
from faststream.rabbit import RabbitBroker, RabbitQueue, RabbitMessage
from google import genai
from google.genai import types
from pydantic import BaseModel, Field, ConfigDict

from common.events import TranscriptReady
from common.logger import init_logging, get_logger

# Initialize Logging
init_logging()
logger = get_logger(__name__)

# Configuration (Strict Env Vars)
RABBITMQ_USER = os.getenv('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_PORT = os.getenv('RABBITMQ_PORT')
RABBITMQ_VHOST = os.getenv('RABBITMQ_VHOST')
RABBITMQ_URL = f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}{RABBITMQ_VHOST}"
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
LLM = os.getenv("LLM")
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT") 
POSTGRES_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
REDDIS_HOST = os.getenv("REDIS_HOST")
REDDIS_PORT = os.getenv("REDIS_PORT")
REDDIS_DB = os.getenv("REDIS_DB")
REDIS_URL = f"redis://{REDDIS_HOST}:{REDDIS_PORT}"

# Setup Dependencies
broker = RabbitBroker(RABBITMQ_URL)
app = FastStream(broker)
redis = aioredis.from_url(REDIS_URL, decode_responses=True)
client = genai.Client(api_key=GEMINI_API_KEY)

# --- Data Models for AI Output ---
class AnalysisResult(BaseModel):
    model_config = ConfigDict(
        json_schema_extra=lambda schema, model: schema.pop('additionalProperties', None)
    )
    speaker_role: str = Field(..., description="The role of the speaker: 'therapist' or 'patient'")
    topic: str = Field(..., description="Primary topic (e.g., Anxiety, Family, Work)")
    emotion: str = Field(..., description="Emotional tone (e.g., Sad, Happy, Neutral)")
    confidence: float

class FullAnalysis(BaseModel):
    model_config = ConfigDict(
        json_schema_extra=lambda schema, model: schema.pop('additionalProperties', None)
    )
    video_id: str = Field(default="") # Filled in after AI generation
    segments: list[AnalysisResult]
    summary: str
    sentiment_trend: list[dict] # [{"time": 10.5, "score": -1}]

# --- Helpers ---
async def save_to_postgres(analysis: FullAnalysis):
    """Save the structured analysis to the database."""
    conn = await asyncpg.connect(POSTGRES_URL)
    try:
        # Save Summary (Upsert using ON CONFLICT)
        await conn.execute("""
            INSERT INTO analysis_summary (video_id, sentiment_trend, summary_text)
            VALUES ($1, $2, $3)
            ON CONFLICT (video_id) DO UPDATE 
            SET summary_text = $3
        """, analysis.video_id, json.dumps(analysis.sentiment_trend), analysis.summary)

        # Save Segments
        # In production, use executemany for batch inserts
        for seg in analysis.segments:
            await conn.execute("""
                INSERT INTO analysis_segments (video_id, speaker_role, topic, emotion, confidence_score)
                VALUES ($1, $2, $3, $4, $5)
            """, analysis.video_id, seg.speaker_role, seg.topic, seg.emotion, seg.confidence)
            
    finally:
        await conn.close()

# --- Message Handler ---
@broker.subscriber(RabbitQueue("transcript_ready"))
async def handle_transcript(event: TranscriptReady, msg: RabbitMessage):
    logger.info("analysis_started", video_id=str(event.video_id))
    
    if not event.transcript_text or not event.transcript_text.strip():
        logger.warning("analysis_skipped_empty_transcript", video_id=str(event.video_id))
        return # This ACKs the message (removes it from queue)
    # Check Redis Cache
    # We hash the transcript text to create a unique content-based key
    transcript_hash = hashlib.sha256(event.transcript_text.encode()).hexdigest()
    cache_key = f"analysis:{transcript_hash}"
    
    cached_data = await redis.get(cache_key)
    if cached_data:
        logger.info("cache_hit", video_id=str(event.video_id))
        return

    # Call LLM (If no cache)
    try:
        prompt = f"""
        Analyze this therapy transcript. 
        1. Identify the 'therapist' and 'patient'.
        2. For every segment, tag the topic and emotion.
        3. Create a sentiment trend (time vs score) and a summary.
        
        Transcript: {event.transcript_text}
        """
        system_instruction = """
        You are an expert clinical psychologist and data analyst. 
        Your job is to analyze therapy session transcripts to provide clinical insights.
        - Be objective and clinical in your tone.
        - Accurately distinguish between the therapist (who asks questions/guides) and the patient.
        - For 'sentiment_trend', map emotions to a score between -1.0 (very negative) and 1.0 (very positive).
        """

        # Call the LLM with JSON Schema enforcement
        response = await client.aio.models.generate_content(
            model=LLM, 
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=FullAnalysis,
                system_instruction=system_instruction,
            ),
        )
        
        result: FullAnalysis = response.parsed
        result.video_id = str(event.video_id) # Inject ID from event

        # Save to DB
        await save_to_postgres(result)

        # Cache result (Expire in 24 hours)
        await redis.set(cache_key, result.model_dump_json(), ex=86400)
        
        logger.info("analysis_complete", video_id=str(event.video_id))

    except Exception as e:
        logger.error("analysis_failed", error=str(e), video_id=str(event.video_id))
        await msg.reject(requeue=False)