FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Service Code & Shared Library
COPY services/analyzer /app/services/analyzer
COPY common /app/common

# Set Python Path
ENV PYTHONPATH=/app

# Run the FastStream Worker
CMD ["faststream", "run", "services.analyzer.main:app"]