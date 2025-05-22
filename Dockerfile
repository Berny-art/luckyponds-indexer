FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

# Copy application files
COPY *.py /app/
COPY contract_abi.json /app/

# Create data directory for databases
RUN mkdir -p /app/data

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV EVENTS_DB_PATH=/app/data/events.db
ENV APP_DB_PATH=/app/data/application.db

# Expose the API port
EXPOSE 5000

# Default command
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "app:app", "--workers", "4", "--timeout", "120"]