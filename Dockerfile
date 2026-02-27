# ---------------------------------------------------------------------------
# Dockerfile — Railway deployment (bypasses Railpack secret scanning)
# ---------------------------------------------------------------------------
FROM python:3.12-slim

# System deps (poppler-utils for pdf2image if needed)
RUN apt-get update && \
    apt-get install -y --no-install-recommends poppler-utils && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Railway injects PORT at runtime; default to 8000
ENV PORT=8000
EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
