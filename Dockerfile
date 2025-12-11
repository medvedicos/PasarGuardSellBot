# syntax=docker/dockerfile:1
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps (keep minimal)
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py ./

# Runtime data lives in /data (mounted via docker-compose)
RUN mkdir -p /data

# Run as non-root
RUN useradd -m appuser \
    && chown -R appuser:appuser /app /data
USER appuser

# bot.py uses relative paths (users_db.json/plans.json) -> run from /data
WORKDIR /data

CMD ["python", "/app/bot.py"]
