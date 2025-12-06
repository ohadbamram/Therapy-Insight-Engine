FROM python:3.11-slim

WORKDIR /app

# Install curl for healthchecks
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/reporting /app/services/reporting
COPY common /app/common

ENV PYTHONPATH=/app

# Run on port 8000 (mapped to 8001 in docker-compose)
CMD ["uvicorn", "services.reporting.main:app", "--host", "0.0.0.0", "--port", "8000"]