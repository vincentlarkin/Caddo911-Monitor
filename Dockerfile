FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Non-root user for hardening
RUN useradd -u 10001 -m appuser

# Copy only app sources from build context (./repo)
COPY . /app

# Persistent data location (SQLite DB)
RUN mkdir -p /data && chown -R appuser:appuser /app /data

USER appuser

EXPOSE 3911

ENV CADDO911_DB_PATH=/data/caddo911.db

CMD ["python", "app.py", "--mode", "serve", "--host", "0.0.0.0", "--port", "3911"]