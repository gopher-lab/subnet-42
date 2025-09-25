# Use Python base image
FROM python:3.10-slim

# Install dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    build-essential \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN . "$HOME/.cargo/env"

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Copy just pyproject.toml first and install dependencies
COPY pyproject.toml .
RUN . "$HOME/.cargo/env" && pip install --prefer-binary .

# Now copy application code (these layers will change frequently)
COPY interfaces interfaces/
COPY scripts scripts/
COPY neurons neurons/
COPY miner miner/
COPY validator validator/
COPY db db/
COPY static static/

# Copy entrypoint script and make it executable
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Use entrypoint.sh as the entrypoint
ENTRYPOINT ["/app/entrypoint.sh"] 