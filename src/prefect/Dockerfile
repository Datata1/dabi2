FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
RUN pip install uv
COPY pyproject.toml ./ 
RUN uv  sync 


COPY ./src/prefect ./app/
COPY .env ./services/ml_service/utils/.env