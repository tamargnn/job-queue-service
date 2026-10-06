FROM python:3.11-slim

# Print logs immediately (no buffering) - critical for seeing worker logs in real time
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Copy requirements FIRST, install, THEN copy code.
# Docker caches each step: if only code changes, pip install is skipped (fast rebuilds).
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .