# syntax=docker/dockerfile:1.7

ARG PYTHON_IMAGE=python:3.14.6-slim-trixie
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.11.25

FROM ${UV_IMAGE} AS uv

FROM ${PYTHON_IMAGE} AS builder

COPY --from=uv /uv /uvx /usr/local/bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

COPY pyproject.toml uv.lock README.md LICENSE ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable --no-install-project

COPY alembic ./alembic
COPY src ./src
COPY example.jsonc dbinit.jsonc ./
COPY deploy/docker/dbinit.jsonc ./deploy/docker/dbinit.jsonc
COPY workspace-shared-docs/contracts/v1/ai-job-workspace.contract.schema.json \
    ./workspace-shared-docs/contracts/v1/ai-job-workspace.contract.schema.json

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable

FROM ${PYTHON_IMAGE} AS runtime

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates postgresql-client \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 aiws \
    && useradd --uid 10001 --gid aiws --no-create-home --shell /usr/sbin/nologin aiws \
    && install --directory --owner aiws --group aiws --mode 0700 \
        /var/lib/aiws /var/lib/aiws-config

ENV AIWS_CONFIG=/tmp/aiws/config.jsonc \
    PATH=/app/.venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY --from=builder --chown=aiws:aiws /app/.venv /app/.venv
COPY --from=builder --chown=aiws:aiws \
    /app/deploy/docker/dbinit.jsonc /app/deploy/docker/dbinit.jsonc

USER 10001:10001

EXPOSE 8000 8010

ENTRYPOINT ["python", "-m", "dbctl.container_entrypoint"]
CMD ["backend"]

STOPSIGNAL SIGTERM
