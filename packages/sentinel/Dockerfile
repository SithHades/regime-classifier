# Build context must be the monorepo root
# docker build -f packages/sentinel/Dockerfile .

FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1

# Install uv for fast package management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# Copy common package
COPY packages/common /app/packages/common

# Copy sentinel package
COPY packages/sentinel /app/packages/sentinel

# Install dependencies
# We install common first
WORKDIR /app/packages/common
RUN uv pip install --system .

# Install sentinel
WORKDIR /app/packages/sentinel
RUN uv pip install --system .

# Run the application
CMD ["python", "-m", "sentinel.main"]
