"""
Common module for Therapy-Insight-Engine.

Provides shared utilities for all microservices including event definitions,
logging configuration, and helper functions.
"""

from common.events import VideoUploaded, AudioExtracted, TranscriptReady
from common.logger import get_logger

__all__ = [
    "VideoUploaded",
    "AudioExtracted", 
    "TranscriptReady",
    "get_logger",
]
