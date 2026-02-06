FROM ghcr.io/astral-sh/uv:alpine

WORKDIR /app

RUN apk add --no-cache bash curl coreutils

COPY pyproject.toml /app/
COPY app/ /app/app
#used for zenodo docs
COPY README.md /app/README.md
COPY docker/healthcheck-wrapper.sh /usr/local/bin/hcwrap
RUN chmod +x /usr/local/bin/hcwrap

RUN uv sync --no-dev

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["/usr/local/bin/hcwrap", "uv", "run", "-m", "app.run_migration"]
