FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*
    
WORKDIR /app
RUN pip install uv
COPY pyproject.toml /app/ 
RUN uv  sync 

COPY ./src/prefect /app/prefect/

COPY ./data ./prefect/data/
