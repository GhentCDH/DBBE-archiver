FROM ghcr.io/astral-sh/uv:alpine
WORKDIR /app

COPY pyproject.toml .

RUN uv sync --no-dev

COPY . .

RUN mkdir -p /app/data

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["uv", "run", "run_migration.py"]
