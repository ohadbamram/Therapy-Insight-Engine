"""
Centralized logging configuration using structlog.

All microservices must use this logger to ensure consistent JSON output
for DataDog agent ingestion.
"""

import os
from typing import Any

import structlog


def get_logger(name: str) -> structlog.BoundLogger:
    """
    Get a configured logger instance with structlog.
    
    This logger outputs structured JSON that is compatible with DataDog's
    automatic parsing and indexing. All microservices should use this function
    to ensure consistent logging across the architecture.
    
    Args:
        name: Logger name, typically the service name or module name
        
    Returns:
        A structlog BoundLogger configured for JSON output
        
    Example:
        >>> logger = get_logger("ingestion_service")
        >>> logger.info("video_uploaded", video_id=video_id, size_mb=100.5)
    """
    
    # Get environment settings
    service_name = os.getenv("SERVICE_NAME", "therapy-engine")
    environment = os.getenv("ENVIRONMENT", "development")
    
    # Configure structlog
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
            structlog.processors.CallsiteParameterAdder(
                parameters=[structlog.processors.CallsiteParameter.FILENAME],
                position="start",
            ),
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    logger = structlog.get_logger(name)
    
    # Add default context fields for DataDog
    logger = logger.bind(
        service=service_name,
        environment=environment,
        logger_name=name,
    )
    
    return logger


def bind_request_context(logger: structlog.BoundLogger, **kwargs: Any) -> structlog.BoundLogger:
    """
    Bind additional context to a logger for the duration of a request.
    
    This is useful for adding request-specific information like video_id,
    user_id, or correlation IDs.
    
    Args:
        logger: The structlog logger instance
        **kwargs: Context key-value pairs to bind
        
    Returns:
        A new logger instance with the additional context bound
        
    Example:
        >>> logger = bind_request_context(logger, video_id=video_id, user_id=user_id)
    """
    return logger.bind(**kwargs)


def init_logging() -> None:
    """
    Initialize logging at application startup.
    
    This function should be called once in the main module of each service
    to ensure consistent logging configuration.
    """
    logger = get_logger(__name__)
    logger.info(
        "logging_initialized",
        environment=os.getenv("ENVIRONMENT", "development"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
