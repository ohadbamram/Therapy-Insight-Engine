FROM python:3.11-slim

WORKDIR /app

# Install System Dependencies (FFmpeg is required!)
RUN apt-get update && \
    apt-get install -y curl ffmpeg && \
    rm -rf /var/lib/apt/lists/*

# Install Python Deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Code
COPY services/audio_extractor /app/services/audio_extractor
COPY common /app/common

ENV PYTHONPATH=/app

# Run Worker
CMD ["faststream", "run", "services.audio_extractor.main:app"]