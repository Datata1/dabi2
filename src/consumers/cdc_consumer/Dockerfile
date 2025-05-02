# Dockerfile für einen Consumer oder Anpassung für Worker
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

# Systemabhängigkeiten installieren (librdkafka für confluent-kafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN pip install uv
COPY pyproject.toml /app/
RUN uv sync 
# Kopiere den Consumer-Code
COPY ./src/consumers /app/consumers 

# Ggf. weitere COPY-Befehle

CMD ["uv", "run", "python", "./consumers/cdc_consumer/cdc_consumer.py"] 