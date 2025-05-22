# Lucky Ponds Backend System

A comprehensive blockchain event indexer and points system for the Lucky Ponds smart contract, featuring automated event processing, points calculation, and REST API access.

## 🏗️ Architecture Overview

The system consists of several interconnected components:

1. **Event Indexer** (`indexer.py`) - Monitors blockchain for contract events
2. **Points Calculator** (`points_calculator.py`) - Processes events and awards points
3. **API Server** (`app.py`) - REST API for accessing data
4. **Scheduler** (`scheduler.py`) - Automated periodic tasks
5. **Winner Selector** (`winner_selector.py`) - Automated winner selection keeper
6. **Database Layer** - SQLite databases for events and application data

## 📋 Prerequisites

- Python 3.11+
- Docker and Docker Compose (recommended)
- Access to Hyperliquid Testnet RPC endpoint
- Contract ABI file (`contract_abi.json`)
- Private key for keeper operations (winner selection)

## 🚀 Quick Start

### Docker Setup (Recommended)

1. **Clone and configure:**
   ```bash
   git clone <repository-url>
   cd lucky-ponds-backend
   cp .env.example .env
   ```

2. **Edit `.env` file with your configuration:**
   ```bash
   # Required settings
   RPC_URL=https://rpc.hyperliquid-testnet.xyz/evm
   CONTRACT_ADDRESS=0x...
   START_BLOCK=12345
   PRIVATE_KEY=0x...  # For keeper operations
   
   # Optional settings
   API_PORT=5000
   POINTS_CALCULATION_INTERVAL=3600  # 1 hour
   TOSS_POINTS_MULTIPLIER=10
   WIN_POINTS=100
   REFERRAL_BONUS_POINTS=20
   ```

3. **Add contract ABI:**
   ```bash
   # Copy your contract ABI to contract_abi.json
   ```

4. **Start all services:**
   ```bash
   # Initial setup (run once)
   docker-compose run --rm setup
   
   # Start all services
   docker-compose up -d
   ```

### Local Development Setup

1. **Create virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Initialize databases:**
   ```bash
   python db_setup.py
   ```

3. **Start components individually:**
   ```bash
   # Terminal 1: Event indexer
   python indexer.py
   
   # Terminal 2: Points calculator (scheduled)
   python scheduler.py
   
   # Terminal 3: API server
   python app.py
   
   # Terminal 4: Winner keeper (optional)
   python winner_selector.py
   ```

## 🎯 Manual Points Operations

### Quick Points Calculation

Run an immediate points calculation to process new events:

```bash
# Docker
docker-compose exec calculator python points_calculator.py

# Local
python points_calculator.py
```

### Full Points Recalculation

Completely recalculate all points from scratch (useful after configuration changes):

```bash
# Docker
docker-compose exec calculator python recalculate_points.py

# Local  
python recalculate_points.py
```

### Interactive Points Management

For more control, use the interactive functions:

```bash
# Docker
docker-compose exec calculator python -c "
from recalculate_points import recalculate, reset_only
print('Available functions:')
print('- recalculate(): Full recalculation from scratch')
print('- reset_only(): Just reset points data without recalculating')
"

# Local
python -c "
from recalculate_points import recalculate, reset_only
result = recalculate()  # or reset_only()
print(result)
"
```

### Advanced Manual Operations

#### 1. Process Specific Event Batches

```bash
# Process only toss events
python -c "
from points_calculator import PointsCalculator
from utils import get_app_db_path, get_events_db_path
calc = PointsCalculator(get_app_db_path(), get_events_db_path())
processed = calc.process_coin_toss_events(batch_size=500)
print(f'Processed {processed} toss events')
"
```

#### 2. Process Winner Events Only

```bash
# Process only winner events
python -c "
from points_calculator import PointsCalculator
from utils import get_app_db_path, get_events_db_path
calc = PointsCalculator(get_app_db_path(), get_events_db_path())
processed = calc.process_winner_events(batch_size=100)
print(f'Processed {processed} winner events')
"
```

#### 3. Process Referral Activations

```bash
# Activate pending referrals
python -c "
from recalculate_points import process_referrals
activated = process_referrals()
print(f'Activated {activated} referrals')
"
```

### Monitoring Points Calculation

Check the status of points calculation:

```bash
# Check calculator state
curl -H "X-API-Key: your-api-key" http://localhost:5000/indexer/status

# Check specific user points
curl -H "X-API-Key: your-api-key" http://localhost:5000/user/0x...

# Check leaderboard
curl -H "X-API-Key: your-api-key" http://localhost:5000/leaderboard?limit=10
```

## 🔧 Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `EVENTS_DB_PATH` | `./data/events.db` | Path to events database |
| `APP_DB_PATH` | `./data/application.db` | Path to application database |
| `TOSS_POINTS_MULTIPLIER` | `10` | Points per ETH for tosses (minimum 1 point) |
| `WIN_POINTS` | `100` | Fixed points for winning |
| `REFERRAL_BONUS_POINTS` | `20` | Points awarded to referrer |
| `POINTS_CALCULATION_INTERVAL` | `3600` | Seconds between automatic calculations |

### Points Calculation Rules

1. **Toss Points**: `(amount_in_eth * TOSS_POINTS_MULTIPLIER)` with minimum of 1 point
2. **Winner Points**: Fixed `WIN_POINTS` per win
3. **Referral Points**: `REFERRAL_BONUS_POINTS` when referred user makes first toss

## 📊 API Endpoints

### Authentication
All protected endpoints require an API key via `X-API-Key` header.

### Main Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/indexer/status` | GET | Indexer and calculator status |
| `/leaderboard` | GET | Global leaderboard with sorting |
| `/user/<address>` | GET | Detailed user statistics |
| `/winners/recent` | GET | Recent winners list |
| `/referral/code/<address>` | GET | Get/create referral code |
| `/referral/apply` | POST | Apply referral code |

### Example API Usage

```bash
# Get leaderboard
curl -H "X-API-Key: your-key" \
  "http://localhost:5000/leaderboard?sort_by=total_points&order=desc&limit=20"

# Get user data
curl -H "X-API-Key: your-key" \
  "http://localhost:5000/user/0x1234567890abcdef1234567890abcdef12345678"

# Apply referral code
curl -X POST -H "X-API-Key: your-key" -H "Content-Type: application/json" \
  -d '{"address":"0x...", "referral_code":"ABC12345"}' \
  "http://localhost:5000/referral/apply"
```

## 🐳 Docker Management

### Service Management

```bash
# View logs
docker-compose logs -f calculator
docker-compose logs -f indexer
docker-compose logs -f api

# Restart specific service
docker-compose restart calculator

# Scale API service
docker-compose up -d --scale api=2

# Update and restart
git pull
docker-compose down
docker-compose build
docker-compose up -d
```

### Database Management

```bash
# Backup databases
docker-compose exec api cp /app/data/events.db /app/data/events_backup.db
docker-compose exec api cp /app/data/application.db /app/data/application_backup.db

# Reset databases (CAUTION: This deletes all data)
docker-compose down
docker volume rm lucky-ponds-backend_data
docker-compose run --rm setup
docker-compose up -d
```

## 🔍 Troubleshooting

### Common Issues

#### Points Not Updating
```bash
# Check indexer status
curl http://localhost:5000/indexer/status

# Check if events are being indexed
docker-compose logs indexer | tail -20

# Manually trigger points calculation
docker-compose exec calculator python points_calculator.py
```

#### API Not Responding
```bash
# Check API logs
docker-compose logs api

# Restart API service
docker-compose restart api

# Check if databases are accessible
docker-compose exec api ls -la /app/data/
```

#### Keeper Issues
```bash
# Check keeper logs
docker-compose logs keeper

# Manually run winner selection
docker-compose exec keeper python winner_selector.py
```

### Performance Tuning

#### Large Event History
If processing a large number of historical events:

```bash
# Increase batch sizes for faster processing
python -c "
from points_calculator import PointsCalculator
from utils import get_app_db_path, get_events_db_path
calc = PointsCalculator(get_app_db_path(), get_events_db_path())
calc.process_coin_toss_events(batch_size=2000)  # Larger batches
"
```

#### Memory Issues
```bash
# Monitor resource usage
docker stats lucky_ponds_indexer
docker stats lucky_ponds_calculator

# Adjust batch sizes in environment
echo "BLOCK_BATCH_SIZE=100" >> .env
docker-compose restart indexer
```

## 🚀 Deployment

### Production Considerations

1. **Database Scaling**: Consider migrating to PostgreSQL for high-volume applications
2. **API Authentication**: Always enable `REQUIRE_AUTH=true` in production
3. **Rate Limiting**: Implement rate limiting for API endpoints
4. **Monitoring**: Add Prometheus/Grafana for metrics
5. **Backup**: Implement automated database backups

### Example Production Environment

```bash
# Production .env additions
REQUIRE_AUTH=true
API_KEY=your-secure-api-key-here
POINTS_CALCULATION_INTERVAL=1800  # 30 minutes
BLOCK_BATCH_SIZE=50  # Conservative for stability
```

## 📝 Development

### Running Tests
```bash
# Run basic functionality tests
python -c "
from points_calculator import PointsCalculator
from utils import get_app_db_path, get_events_db_path
calc = PointsCalculator(get_app_db_path(), get_events_db_path())
print('Calculator initialized successfully')
"
```

### Adding New Features

1. **New Point Types**: Modify `points_calculator.py` and database schema
2. **API Endpoints**: Add routes in `app.py`
3. **Event Types**: Update `indexer.py` to handle new contract events

## 📄 License

MIT License - see LICENSE file for details.