FROM python:3.12-slim

# Pinned version for reproducible builds
COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /uvx /usr/local/bin/

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/usr/local

# curl is needed by the compose healthchecks
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
# hadolint ignore=DL3008
RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
    curl build-essential libpq-dev git gcc python3-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependency files first, to leverage Docker's layer cache
COPY pyproject.toml uv.lock* /app/
RUN uv sync --frozen --no-install-project

COPY . /app
RUN uv sync --frozen

RUN useradd -u 1000 -m analyst && chown -R analyst:analyst /app
USER analyst

CMD ["sleep", "infinity"]
