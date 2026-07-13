FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies including Tesseract OCR engine
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libjpeg-dev \
    zlib1g-dev \
    libpng-dev \
    libglib2.0-0 \
    tesseract-ocr \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5090
# Copy project files
COPY . .

# Run failover.py
CMD ["python", "failover.py"]
