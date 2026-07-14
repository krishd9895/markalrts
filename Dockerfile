FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies. tesseract-ocr and the image libs are only
# needed for OPTIONAL local OCR (see requirements-ocr.txt / ocr.py) -- if
# this step fails on a low-resource machine, the build continues and the
# bot simply runs without local OCR (falling back to Google Drive / an
# external OCR service instead).
RUN apt-get update && (apt-get install -y --no-install-recommends \
    gcc \
    libjpeg-dev \
    zlib1g-dev \
    libpng-dev \
    libglib2.0-0 \
    tesseract-ocr \
    || echo "WARNING: optional OCR system packages failed to install -- continuing without local OCR") \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for caching
COPY requirements.txt requirements-ocr.txt ./

# Retry + longer timeout to tolerate flaky/slow networks (e.g. read
# timeouts talking to files.pythonhosted.org)
ENV PIP_DEFAULT_TIMEOUT=100
ENV PIP_RETRIES=5

# Install core Python dependencies (required)
RUN pip install --no-cache-dir -r requirements.txt

# Install optional OCR Python dependencies -- do not fail the build if
# they can't be installed (e.g. Pillow needs the image libs above)
RUN pip install --no-cache-dir -r requirements-ocr.txt \
    || echo "WARNING: optional OCR Python packages failed to install -- continuing without local OCR"

EXPOSE 5090
# Copy project files
COPY . .

# Run failover.py
CMD ["python", "failover.py"]
