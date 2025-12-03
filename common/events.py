"""
Event models for RabbitMQ message passing between microservices.

These Pydantic models define the contract for asynchronous event communication
across the therapy-insight-engine microservices architecture.
"""

from datetime import datetime
from typing import Optional, Dict, Any
from uuid import UUID

from pydantic import BaseModel, Field


class VideoUploaded(BaseModel):
    """Event emitted when a video is successfully uploaded to MinIO.
    
    This event triggers the audio extraction pipeline.
    """
    video_id: UUID = Field(..., description="Unique identifier for the video")
    filename: str = Field(..., description="Original filename of the uploaded video")
    minio_path: str = Field(..., description="Path to the video in MinIO storage")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Event timestamp")


class AudioExtracted(BaseModel):
    """Event emitted when audio has been extracted from a video.
    
    This event triggers the transcription pipeline with AssemblyAI.
    """
    video_id: UUID = Field(..., description="Reference to the original video")
    audio_path: str = Field(..., description="Path to the extracted audio file in MinIO")
    duration_seconds: Optional[float] = Field(None, description="Duration of the extracted audio")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Event timestamp")


class TranscriptReady(BaseModel):
    """Event emitted when transcription is complete with speaker diarization.
    
    This event triggers the analysis pipeline with Gemini LLM.
    """
    video_id: UUID = Field(..., description="Reference to the original video")
    transcript_json: Dict[str, Any] = Field(..., description="Full transcript from AssemblyAI including speaker labels")
    speaker_segments: Optional[Dict[str, Any]] = Field(None, description="Parsed speaker segments with timestamps")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Event timestamp")

    class Config:
        """Pydantic configuration for TranscriptReady."""
        json_schema_extra = {
            "example": {
                "video_id": "550e8400-e29b-41d4-a716-446655440000",
                "transcript_json": {
                    "id": "asr-123",
                    "text": "Example transcript...",
                    "utterances": []
                },
                "speaker_segments": None,
                "timestamp": "2024-12-03T12:00:00"
            }
        }
