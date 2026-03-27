# Use official Python runtime as base image
FROM python:3.11-slim

# Set working directory in container
WORKDIR /app

# Install system dependencies for Playwright and Firefox
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    firefox-esr \
    libglib2.0-0 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxext6 \
    libxrender1 \
    libnss3 \
    libnspr4 \
    libxss1 \
    libasound2 \
    libgbm1 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    fonts-liberation \
    ca-certificates \
    libdbus-glib-1-2 \
    libatk-bridge2.0-0 \
    libatspi2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxdamage1 \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --upgrade pip setuptools wheel

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt || pip install -r requirements.txt

# Install Playwright Firefox browser
RUN python -m playwright install firefox

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p data/checkpoints data/raw logs

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Firefox optimization: Reduce memory usage
ENV MOZ_HEADLESS=1
ENV MOZ_QUIET=1

# Memory limit: Set to 2GB for container
# (Add --memory=2g when running docker run)

# Default command: Extract all data from all companies
# Use max_reviews=50000 to extract as much as possible
CMD ["python", "main.py", "--mode", "full", "--max_reviews", "50000"]
