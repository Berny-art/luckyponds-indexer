# Lucky Ponds Event Indexer

This project indexes events from the Lucky Ponds smart contract and provides a REST API to access the indexed data.

## Architecture

The system consists of two main components:

1. **Event Indexer (`indexer.py`)**: Monitors the blockchain for events emitted by the Lucky Ponds contract and stores them in an SQLite database.
2. **REST API (`api.py`)**: Exposes the indexed data through a Flask-based RESTful API.

## Prerequisites

- Python 3.10+
- Docker and Docker Compose (optional, for containerized deployment)
- Access to an Ethereum JSON-RPC endpoint (Infura, Alchemy, your own node, etc.)

## Setup

### Option 1: Local Setup

1. Clone this repository:
   ```
   git clone <repository-url>
   cd lucky-ponds-backend
   ```

2. Create a virtual environment and install dependencies:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. Create an `.env` file based on the provided `.env.example`:
   ```
   cp .env.example .env
   ```

4. Edit the `.env` file and set the appropriate values:
   - `RPC_URL`: URL of your Ethereum RPC provider
   - `CONTRACT_ADDRESS`: Address of the Lucky Ponds contract
   - `START_BLOCK`: The block number to start indexing from (use the contract deployment block for a complete history)

5. Create a `contract_abi.json` file containing the contract ABI:
   ```
   # Copy the ABI from the smart contract
   ```

6. Start the indexer and API:
   ```
   # In one terminal
   python indexer.py
   
   # In another terminal
   python api.py
   ```

### Option 2: Docker Setup

1. Create an `.env` file as described above.

2. Create a `contract_abi.json` file containing the contract ABI.

3. Start the services using Docker Compose:
   ```
   docker-compose up -d
   ```

## API Endpoints

The API provides the following endpoints:

### Health Check
- `GET /api/health` - Check if the API is running

### Indexer Status
- `GET /api/indexer/status` - Get the current status of the indexer

### Ponds
- `GET /api/ponds` - List all ponds
- `GET /api/ponds/{pond_type}` - Get details about a specific pond
- `GET /api/ponds/{pond_type}/tosses` - Get all coin tosses for a specific pond
- `GET /api/ponds/{pond_type}/winners` - Get all winners for a specific pond

### Users
- `GET /api/users/{address}/participation` - Get participation details for a specific user

### Statistics
- `GET /api/stats/global` - Get global statistics about the contract activity

## Database Schema

The system uses SQLite with the following tables:

1. `indexer_state` - Tracks the indexer's progress
2. `coin_tossed_events` - Stores CoinTossed events
3. `lucky_frog_selected_events` - Stores LuckyFrogSelected events
4. `pond_action_events` - Stores PondAction events
5. `config_updated_events` - Stores ConfigUpdated events

## Deployment Considerations

### Hosting Options

1. **VPS/Dedicated Server**: Deploy using Docker on a VPS from providers like DigitalOcean, Linode, or AWS EC2.
2. **Serverless**: Convert the API to use AWS Lambda or similar serverless platforms.
3. **Railway/Heroku**: Deploy directly from GitHub to platforms like Railway or Heroku.

### RPC Provider

For production, consider using a dedicated RPC provider:
- Infura
- Alchemy
- QuickNode
- Running your own Ethereum node

### Database Scaling

If your contract generates a large number of events, consider:
1. Migrating from SQLite to PostgreSQL or MongoDB
2. Implementing database sharding or archiving
3. Adding caching with Redis

### Monitoring

Add monitoring and alerts using:
- Prometheus and Grafana
- Sentry for error tracking
- PagerDuty or similar for alerts

## License

[MIT](LICENSE)