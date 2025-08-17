# Use full Debian image instead of slim for better package availability
FROM python:3.11-bookworm
# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libgdal-dev \
    gdal-bin \
    libproj-dev \
    proj-data \
    proj-bin \
    libgeos-dev \
    libspatialindex-dev \
    libffi-dev \
    libssl-dev \
    python3-dev \
    build-essential \
    libcairo2-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables for GDAL
ENV GDAL_CONFIG=/usr/bin/gdal-config
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file first
COPY requirements.txt .

# Upgrade pip and install wheel
RUN pip install --upgrade pip wheel

# Install Python dependencies from requirements.txt (including playwright)
RUN pip install --no-cache-dir --timeout=1000 -r requirements.txt

# --- NEW: Install Playwright browsers and dependencies ---
# This command installs the system libraries Playwright needs
RUN playwright install-deps
# This command downloads the browser binaries for Playwright
RUN playwright install

# The command to run your server when the container starts
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
