#!/bin/bash
# Simple monitoring script for cron-based Lucky Ponds system

echo "=== Lucky Ponds System Monitor ==="
echo "Time: $(date)"
echo

echo "=== Container Status ==="
docker-compose ps

echo -e "\n=== Recent Calculator Activity ==="
echo "Last 5 calculator log entries:"
docker-compose exec calculator tail -5 /app/data/calculator.log 2>/dev/null || echo "No calculator logs found"

echo -e "\n=== Recent Keeper Activity ==="
echo "Last 5 keeper log entries:"
docker-compose exec keeper tail -5 /app/data/keeper.log 2>/dev/null || echo "No keeper logs found"

echo -e "\n=== Cron Job Status ==="
echo "Calculator crontab:"
docker-compose exec calculator crontab -l 2>/dev/null || echo "No calculator crontab"

echo -e "\nKeeper crontab:"
docker-compose exec keeper crontab -l 2>/dev/null || echo "No keeper crontab"

echo -e "\n=== Log File Ages ==="
echo "Calculator log:"
docker-compose exec calculator stat /app/data/calculator.log 2>/dev/null | grep Modify || echo "Calculator log not found"

echo "Keeper log:"
docker-compose exec keeper stat /app/data/keeper.log 2>/dev/null | grep Modify || echo "Keeper log not found"

echo -e "\n=== Recent API Activity ==="
if command -v curl >/dev/null 2>&1; then
    echo "API Health Check:"
    curl -s http://localhost:${API_PORT:-5000}/health | head -3 || echo "API not responding"
else
    echo "curl not available - skipping API check"
fi

echo -e "\n=== System Resources ==="
echo "Docker container resource usage:"
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}" 2>/dev/null || echo "Unable to get container stats"

echo -e "\n=== Next Scheduled Executions ==="
current_minute=$(date +%M)
current_hour=$(date +%H)

echo "Next points calculation: $(date -d 'next hour' '+%H:30')"

# Calculate next 5-minute interval
next_5min=$((($current_minute / 5 + 1) * 5))
if [ $next_5min -ge 60 ]; then
    next_5min=0
    next_hour=$((current_hour + 1))
    if [ $next_hour -ge 24 ]; then
        next_hour=0
    fi
    echo "Next winner check: $(printf "%02d:%02d" $next_hour $next_5min)"
else
    echo "Next winner check: $(printf "%02d:%02d" $current_hour $next_5min)"
fi

echo -e "\n=== Monitor Complete ==="