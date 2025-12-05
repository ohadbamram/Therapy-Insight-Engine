"""
Centralized logging configuration using structlog.

All microservices must use this logger to ensure consistent JSON output
for DataDog agent ingestion.
"""

import os
import sys
import logging
import structlog
from typing import Any

def get_logger(name: str) -> structlog.BoundLogger:
    """
    Get a pre-configured logger with context bound.
    """
    # Get environment settings
    service_name = os.getenv("SERVICE_NAME", "therapy-engine")
    environment = os.getenv("ENVIRONMENT", "development")
    
    # Just get the logger (configuration happens in init_logging)
    logger = structlog.get_logger(name)
    
    # Bind the standard DataDog tags
    return logger.bind(
        service=service_name,
        environment=environment,
    )
 
def bind_request_context(logger: structlog.BoundLogger, **kwargs: Any) -> structlog.BoundLogger:
    """
    Bind additional context to a logger for the duration of a request.
    
    This is useful for adding request-specific information like video_id,
    user_id, or correlation IDs.
    """
    return logger.bind(**kwargs)


def init_logging() -> None:
    """
    Initialize global logging configuration.
    Call this ONCE at the start of your main.py.
    """
    # basicConfig allows standard logging (like from libraries) to be captured
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.INFO)

    # Configure structlog GLOBALLY
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(), # Crucial for DataDog
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(), # Use stdlib factory for better compat
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
