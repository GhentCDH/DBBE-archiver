FROM ghcr.io/astral-sh/uv:alpine
# Set working directory
WORKDIR /app

# Copy dependency files first (for better caching)
COPY pyproject.toml .

# Install dependencies using uv
RUN uv sync --no-dev

# Copy application code
COPY . .

# Create directory for SQLite database
RUN mkdir -p /app/data

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Run the migration
CMD ["uv", "run", "run_migration.py"]
