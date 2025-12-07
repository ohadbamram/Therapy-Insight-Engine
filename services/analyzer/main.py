import os
import json
import hashlib
import asyncpg
from typing import Any, Type, Awaitable
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
class CognitiveDistortion(BaseModel):
    
    model_config = ConfigDict(json_schema_extra=lambda s, m: s.pop('additionalProperties', None))
    quote: str = Field(..., description="The exact quote from the patient showing the distortion")
    distortion_type: str = Field(..., description="Type (e.g., Catastrophizing, Mind Reading)")
    explanation: str = Field(..., description="Brief explanation of why this is a distortion")

class TherapistIntervention(BaseModel):
    
    model_config = ConfigDict(json_schema_extra=lambda s, m: s.pop('additionalProperties', None))
    quote: str = Field(..., description="The exact quote from the therapist")
    technique: str = Field(..., description="Technique used (e.g., Validation, Open Question)")
    purpose: str = Field(..., description="The intended therapeutic effect")
class AnalysisResult(BaseModel):

    model_config = ConfigDict(extra='forbid')
    text: str = Field(..., description="The exact text content of this segment")
    speaker_role: str = Field(..., description="The role of the speaker: 'therapist' or 'patient'")
    topic: str = Field(..., description="Primary topic (e.g., Anxiety, Family, Work)")
    emotion: str = Field(..., description="Emotional tone (e.g., Sad, Happy, Neutral)")
    confidence: float

class FullAnalysis(BaseModel):
    
    model_config = ConfigDict(extra='forbid')
    video_id: str = Field(default="") # Filled in after AI generation
    segments: list[AnalysisResult]
    summary: str
    recommendations: list[str] = Field(default=[], description="List of recommendations for the therapist")
    cognitive_distortions: list[CognitiveDistortion] = Field(default=[])
    therapist_interventions: list[TherapistIntervention] = Field(default=[])


def get_clean_schema(model_class: Type[BaseModel]) -> dict[str, Any]:
    """
    Adapter function: Converts a Pydantic Model into a Gemini-compatible JSON Schema.
    It recursively removes the 'additionalProperties' key which Gemini prohibits.
    """
    schema: dict[str, Any] = model_class.model_json_schema()
    def strip_forbidden_keys(d: Any) -> None:
        if isinstance(d, dict):
            # Gemini forbids 'additionalProperties', so we remove it if found
            if "additionalProperties" in d:
                del d["additionalProperties"]
            # Recursively clean nested dictionaries
            for v in d.values():
                strip_forbidden_keys(v)
        elif isinstance(d, list):
            # Recursively clean lists
            for item in d:
                strip_forbidden_keys(item)
    strip_forbidden_keys(schema)
    return schema

async def save_to_postgres(analysis: FullAnalysis) -> None:
    """Save the structured analysis to the database."""
    conn: asyncpg.Connection = await asyncpg.connect(POSTGRES_URL)
    try:
        # Convert list of Pydantic models to list of dicts for JSON serialization
        recommendations_json: str = json.dumps(analysis.recommendations)
        distortions_json: str = json.dumps([d.model_dump() for d in analysis.cognitive_distortions])
        interventions_json: str = json.dumps([i.model_dump() for i in analysis.therapist_interventions])

        # Save Summary (Upsert using ON CONFLICT)
        await conn.execute("""
            INSERT INTO analysis_summary (
                video_id, summary_text, recommendations, cognitive_distortions, therapist_interventions
            )
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (video_id) DO UPDATE 
            SET summary_text = $2, recommendations = $3, 
                cognitive_distortions = $4, therapist_interventions = $5
        """, analysis.video_id, analysis.summary, recommendations_json, distortions_json, interventions_json)

        # Save Segments
        # In production, use executemany for batch inserts
        for seg in analysis.segments:
            await conn.execute("""
                INSERT INTO analysis_segments (video_id, speaker_role, text_content, topic, emotion, confidence_score)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, analysis.video_id, seg.speaker_role, seg.text, seg.topic, seg.emotion, seg.confidence)
            
    finally:
        await conn.close()

@broker.subscriber(RabbitQueue("transcript_ready"))
async def handle_transcript(event: TranscriptReady, msg: RabbitMessage) -> Any:
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
        try:
            # Get Result from Cache even if It's not in the DB
            result = FullAnalysis.model_validate_json(cached_data)
            result.video_id = str(event.video_id)
            await save_to_postgres(result)
            logger.info("analysis_restored_from_cache", video_id=str(event.video_id))
            return
        except Exception as e:
            logger.warning("cache_restore_failed", error=str(e), video_id=str(event.video_id))

    # Call LLM (If no cache)
    try:
        prompt = f"""
        Analyze this therapy transcript. 
        1. Identify the 'therapist' and 'patient'.
        2. For every segment, tag the topic and emotion.
        3. Identify specific 'Cognitive Distortions' in the patient's speech (CBT).
        4. Identify specific 'Therapeutic Interventions' used by the therapist.
        5. Provide a clinical summary.
        6. Based on the topics that made the patient feel negative, provide a list of recommendations for the therapist for the next session.
        
        Transcript: {event.transcript_text}
        """
        system_instruction = """
        You are an expert clinical psychologist and data analyst. 
        Your job is to analyze therapy session transcripts to provide clinical insights.
        - Be objective and clinical in your tone.

        DATA FORMATTING RULES:
        1. **Granularity is King**: You must output a detailed script. Never summarize a conversation into one block. 
        2. **Split Often**: If a speaker talks for more than 2-3 sentences, split it into a new segment if the topic shifts slightly.
        3. **Speaker Identification**: accurately label 'therapist' vs 'patient'.
        
        CLINICAL TASKS:
        - Extract deep clinical data.
        - Cognitive Distortions: Catastrophizing, All-or-Nothing thinking, Mind Reading, etc.
        - Interventions: Validation, Reflection, Psychoeducation, Open Question, etc.
        """

        # Use Adapter to get clean schema
        clean_schema = get_clean_schema(FullAnalysis)

        # Call the LLM with JSON Schema enforcement
        response = await client.aio.models.generate_content(
            model=LLM, 
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=clean_schema,
                system_instruction=system_instruction,
            ),
        )
        
        # Manually parse the response (since we passed a dict schema)   
        result = FullAnalysis.model_validate_json(response.text)
        result.video_id = str(event.video_id) # Inject ID from event

        # Save to DB
        await save_to_postgres(result)

        # Cache result (Expire in 24 hours)
        await redis.set(cache_key, result.model_dump_json(), ex=86400)
        
        logger.info("analysis_complete", video_id=str(event.video_id))

    except Exception as e:
        logger.error("analysis_failed", error=str(e), video_id=str(event.video_id))
        await msg.reject(requeue=False)