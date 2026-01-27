FROM ghcr.io/astral-sh/uv:alpine

WORKDIR /app

COPY pyproject.toml /app/
COPY app/ /app/app

RUN uv sync --no-dev

RUN mkdir -p /app/data

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["uv", "run", "-m", "app.run_migration"]
