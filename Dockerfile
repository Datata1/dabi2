# Dockerfile
FROM python:3.12-slim

WORKDIR /app
RUN pip install  uv
COPY pyproject.toml ./
RUN uv venv .venv
RUN uv sync

RUN uv add --dev ipykernel
RUN uv run ipython kernel install --user --env VIRTUAL_ENV $(pwd)/.venv --name=dabi2

