# Multi-stage: the compiler toolchain needed to build wheels never reaches the
# final image. Previously build-essential, gcc, python3-dev, git and libpq-dev
# all shipped to production, and libpq-dev in particular was dead weight: there
# is no PostgreSQL anywhere in this project (DuckDB and SQLite only).

# ---------- build stage ----------
FROM python:3.12-slim AS builder

# Pinned version for reproducible builds
COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /uvx /usr/local/bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv

# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc python3-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependency files only, so the layer cache survives every source change.
# --no-dev leaves pytest/ruff/bandit/sqlfluff/pre-commit out of the image;
# they are installed by a plain `uv sync` locally and in CI.
COPY pyproject.toml uv.lock /app/
RUN uv sync --frozen --no-install-project --no-dev

# ---------- runtime stage ----------
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    VIRTUAL_ENV=/opt/venv

# curl is needed by the compose healthchecks; git is kept because dbt shells out
# to it for package resolution even when packages.yml is absent.
# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl git \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv

WORKDIR /app
COPY . /app

RUN useradd -u 1000 -m analyst && chown -R analyst:analyst /app
USER analyst

CMD ["sleep", "infinity"]
