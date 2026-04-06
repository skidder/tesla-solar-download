FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py .
COPY docker/entrypoint.sh /entrypoint.sh
COPY docker/scheduler.py /app/scheduler.py
RUN chmod +x /entrypoint.sh

# Default paths - override via volume mounts in docker-compose.yml
ENV DATA_DIR=/data
ENV TESLA_CACHE_FILE=/config/cache.json

ENTRYPOINT ["/entrypoint.sh"]
