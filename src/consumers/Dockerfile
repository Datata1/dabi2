FROM ghcr.io/astral-sh/uv:python3.12-alpine
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
ENV UV_LINK_MODE=copy

WORKDIR /app

RUN apk update && \
    apk add --no-cache \
        build-base \
        python3-dev \
        librdkafka-dev

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project

ADD . /app

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync 

CMD ["uv", "run", "main.py"] 