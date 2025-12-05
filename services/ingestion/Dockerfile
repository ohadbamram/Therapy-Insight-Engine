FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (curl is useful for healthchecks)
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the specific service code
COPY services/ingestion /app/services/ingestion

# Copy the shared library (Crucial!)
COPY common /app/common

# Set python path so we can import 'services' and 'common'
ENV PYTHONPATH=/app

# Run the app
CMD ["uvicorn", "services.ingestion.main:app", "--host", "0.0.0.0", "--port", "8000"]