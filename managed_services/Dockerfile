# Managed Services Dockerfile
# Optimized for Google Cloud Run deployment

FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies (minimal set)
RUN apt-get update && apt-get install -y \
    --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY managed_services/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY managed_services/ ./managed_services/

# Copy required files from parent directory
COPY static_prompt.py ./
COPY token_service.py ./
COPY user_service.py ./
COPY queue_service.py ./
COPY table_service.py ./
COPY blob_service.py ./

# Create non-root user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser
RUN chown -R appuser:appuser /app
USER appuser

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/ || exit 1

# Run the application
CMD ["python", "-m", "uvicorn", "managed_services.app_managed:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]