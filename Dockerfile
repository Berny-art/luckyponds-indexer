FROM python:3.10-slim

WORKDIR /app

# Install system dependencies including cron
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cron \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create crontab file
RUN echo "# Pond winner selection cron jobs" > /etc/cron.d/winner-selector
# 5-minute ponds: Run at 20 seconds past 00, 05, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55 minutes of every hour
RUN echo "0,5,10,15,20,25,30,35,40,45,50,55 * * * * appuser cd /app && sleep 20 && python select_winner.py --pond-type=auto --period=five_min >> /app/logs/five_min_selector.log 2>&1" >> /etc/cron.d/winner-selector
# Also run 5-minute ponds check every minute to catch any that might have been missed
RUN echo "* * * * * appuser cd /app && python select_winner.py --pond-type=auto --period=five_min >> /app/logs/five_min_fallback.log 2>&1" >> /etc/cron.d/winner-selector
# Hourly ponds: Run at 1 minute past the hour
RUN echo "1 * * * * appuser cd /app && python select_winner.py --pond-type=auto --period=hourly >> /app/logs/hourly_selector.log 2>&1" >> /etc/cron.d/winner-selector
# Daily ponds: Run at 1 minute past midnight UTC
RUN echo "1 0 * * * appuser cd /app && python select_winner.py --pond-type=auto --period=daily >> /app/logs/daily_selector.log 2>&1" >> /etc/cron.d/winner-selector
# Weekly ponds: Run at 1 minute past midnight UTC on Sunday (day 0)
RUN echo "1 0 * * 0 appuser cd /app && python select_winner.py --pond-type=auto --period=weekly >> /app/logs/weekly_selector.log 2>&1" >> /etc/cron.d/winner-selector
# Monthly ponds: Run at 1 minute past midnight UTC on the first day of the month
RUN echo "1 0 1 * * appuser cd /app && python select_winner.py --pond-type=auto --period=monthly >> /app/logs/monthly_selector.log 2>&1" >> /etc/cron.d/winner-selector
# Fallback job: Run every 15 minutes to catch any missed or custom ponds
RUN echo "*/15 * * * * appuser cd /app && python select_winner.py --pond-type=auto >> /app/logs/auto_selector.log 2>&1" >> /etc/cron.d/winner-selector

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/winner-selector

# Apply cron job
RUN crontab /etc/cron.d/winner-selector

# Create log directory
RUN mkdir -p /app/logs && chmod 777 /app/logs

# Create a non-root user to run the application
RUN adduser --disabled-password --gecos '' appuser
RUN chown -R appuser:appuser /app

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Expose the API port
EXPOSE 5000

# Create startup script to run both cron and the application
RUN echo "#!/bin/bash" > /app/start.sh
RUN echo "service cron start" >> /app/start.sh
RUN echo "cd /app" >> /app/start.sh
RUN echo "sudo -u appuser python indexer.py & sudo -u appuser python api.py" >> /app/start.sh
RUN chmod +x /app/start.sh

# Command to run the services
CMD ["/app/start.sh"]