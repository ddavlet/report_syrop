FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# System deps (only if needed later)
# RUN apt-get update && apt-get install -y --no-install-recommends \
#   && rm -rf /var/lib/apt/lists/*

# Install Python deps separately for better layer caching
COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

# Copy app code
COPY . /app

# Default command runs Telegram bot
ENV PYTHONPATH=/app
CMD ["python", "-m", "src.bot"]
