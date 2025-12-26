import os
import json
import asyncpg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Any, Dict
from common.logger import init_logging, get_logger

# Initialize Logging
init_logging()
logger = get_logger(__name__)

app = FastAPI(title="Therapy Reporting Service")

# Database Config
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


# --- API Models ---
class VideoSummary(BaseModel):
    video_id: str
    filename: str
    status: str
    summary_text: Optional[str] = None
    created_at: str


class VideoDetail(BaseModel):
    video_id: str
    summary: Optional[str] = None
    transcript_segments: List[Any] = []
    recommendations: List[str] = []
    cognitive_distortions: List[Any] = []
    therapist_interventions: List[Any] = []


# --- Database Helper ---
async def get_db_connection() -> asyncpg.Connection:
    return await asyncpg.connect(POSTGRES_URL)


@app.get("/videos", response_model=List[VideoSummary])
async def list_videos() -> List[Dict[str, Any]]:
    """List all videos with their processing status and summary."""
    conn = await get_db_connection()
    try:
        # Join videos with analysis_summary to get the high-level view
        rows = await conn.fetch(
            """
            SELECT 
                v.id::text as video_id, 
                v.filename, 
                v.status, 
                (v.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Jerusalem')::text as created_at,
                a.summary_text
            FROM videos v
            LEFT JOIN analysis_summary a ON v.id = a.video_id
            ORDER BY v.created_at DESC
        """
        )
        return [dict(row) for row in rows]
    finally:
        await conn.close()


@app.get("/videos/{video_id}", response_model=VideoDetail)
async def get_video_analysis(video_id: str) -> Dict[str, Any]:
    """Get the full analysis (segments + insights) for a specific video."""
    conn = await get_db_connection()
    try:
        # Get Summary & Trend
        summary_row = await conn.fetchrow(
            """
            SELECT summary_text, recommendations, cognitive_distortions, therapist_interventions
            FROM analysis_summary 
            WHERE video_id = $1
        """,
            video_id,
        )

        if not summary_row:
            # Check if video exists but is just processing
            video_exists = await conn.fetchval(
                "SELECT 1 FROM videos WHERE id = $1", video_id
            )
            if video_exists:
                raise HTTPException(
                    status_code=404, detail="Analysis not found (or processing)"
                )
            raise HTTPException(status_code=404, detail="Video not found")

        # Get Granular Segments (Now includes text_content!)
        segments_rows = await conn.fetch(
            """
            SELECT speaker_role, text_content, topic, emotion, confidence_score 
            FROM analysis_segments 
            WHERE video_id = $1 
            ORDER BY id ASC
        """,
            video_id,
        )

        return {
            "video_id": video_id,
            "summary": summary_row["summary_text"],
            "recommendations": (
                json.loads(summary_row["recommendations"])
                if summary_row["recommendations"]
                else []
            ),
            "cognitive_distortions": (
                json.loads(summary_row["cognitive_distortions"])
                if summary_row["cognitive_distortions"]
                else []
            ),
            "therapist_interventions": (
                json.loads(summary_row["therapist_interventions"])
                if summary_row["therapist_interventions"]
                else []
            ),
            "transcript_segments": [dict(row) for row in segments_rows],
        }
    finally:
        await conn.close()
