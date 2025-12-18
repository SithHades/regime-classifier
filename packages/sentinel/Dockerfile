# Use a Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

# Set working directory
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy the uv lockfile and root pyproject.toml
COPY uv.lock pyproject.toml ./

# Copy the package meant to be installed and its local dependencies
# Note: This assumes the build context is the monorepo root
COPY packages/common ./packages/common
COPY packages/sentinel ./packages/sentinel

# Install dependencies and the project
# --frozen: use the lockfile
# --no-dev: do not install dev dependencies
# --package sentinel: explicit install of the target package
# --no-editable: install as standard packages
RUN uv sync --frozen --no-dev --package sentinel --no-editable

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

# Expose the health check port
EXPOSE 8000

# Run the application
CMD ["python", "-m", "sentinel.main"]
