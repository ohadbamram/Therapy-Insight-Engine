FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Code
COPY services/transcription /app/services/transcription
COPY common /app/common

ENV PYTHONPATH=/app

# Run Worker
CMD ["faststream", "run", "services.transcription.main:app"]