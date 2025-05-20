import os
import sqlite3
import logging
import functools
from datetime import datetime
from flask import Flask, jsonify, request
from dotenv import load_dotenv
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
DB_PATH = os.getenv("DB_PATH", "./app/data/lucky_ponds.db")
API_PORT = int(os.getenv("API_PORT", "5000"))
API_KEY = os.getenv("API_KEY", "")  # Authentication key for protected endpoints
REQUIRE_AUTH = os.getenv("REQUIRE_AUTH", "false").lower() == "true"  # Whether authentication is required

# Import from referral system
try:
    from referral_system import get_user_stats, get_leaderboard, get_or_create_user_referral, apply_referral_code
except ImportError:
    logger.error("Referral system import failed. Some features might not be available.")

# Initialize Flask app
app = Flask(__name__)

def require_api_key(f):
    """Decorator to require API key for protected endpoints."""
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not REQUIRE_AUTH:
            return f(*args, **kwargs)
            
        api_key = request.headers.get('X-API-Key')
        if api_key and api_key == API_KEY:
            return f(*args, **kwargs)
        else:
            return jsonify({
                "status": "error",
                "message": "Unauthorized. Valid API key required."
            }), 401
    return decorated

def get_db_connection():
    """Get a database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # This allows accessing columns by name
    return conn

@app.route('/health', methods=['GET'])
def health_check():
    """API health check endpoint."""
    try:
        # Try to connect to the database
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT 1')
        cursor.fetchone()
        conn.close()
        
        return jsonify({
            "status": "healthy",
            "message": "API is running and database is accessible",
            "timestamp": datetime.now().isoformat()
        }), 200
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            "status": "unhealthy",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/indexer/status', methods=['GET'])
@require_api_key
def indexer_status():
    """Check the status of the blockchain indexer."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get the last indexed block
        cursor.execute('SELECT last_block FROM indexer_state WHERE id = 1')
        indexer_state = cursor.fetchone()
        
        if not indexer_state:
            return jsonify({
                "status": "not_initialized",
                "message": "Indexer has not been initialized yet",
                "timestamp": datetime.now().isoformat()
            }), 404
        
        last_block = indexer_state['last_block']
        
        # Get the count of various events
        cursor.execute('SELECT COUNT(*) FROM coin_tossed_events')
        toss_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM lucky_winner_selected_events')
        winner_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM user_points')
        user_count = cursor.fetchone()[0]
        
        # Get the most recent event timestamp
        cursor.execute('''
        SELECT MAX(block_timestamp) 
        FROM (
            SELECT MAX(block_timestamp) as block_timestamp FROM coin_tossed_events
            UNION
            SELECT MAX(block_timestamp) as block_timestamp FROM lucky_winner_selected_events
        )
        ''')
        last_event_timestamp = cursor.fetchone()[0]
        
        last_event_time = datetime.fromtimestamp(last_event_timestamp) if last_event_timestamp else None
        
        conn.close()
        
        return jsonify({
            "status": "active",
            "last_indexed_block": last_block,
            "total_toss_events": toss_count,
            "total_winner_events": winner_count,
            "total_users": user_count,
            "last_event_timestamp": last_event_timestamp,
            "last_event_time": last_event_time.isoformat() if last_event_time else None,
            "current_time": datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting indexer status: {e}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/leaderboard', methods=['GET'])
@require_api_key
def get_global_leaderboard():
    """
    Get the global leaderboard with sorting options.
    Query parameters:
    - sort_by: field to sort by (points, value, wins)
    - order: asc or desc
    - limit: number of results (default 50)
    - offset: pagination offset (default 0)
    """
    try:
        sort_by = request.args.get('sort_by', 'points')
        order = request.args.get('order', 'desc').upper()
        limit = min(int(request.args.get('limit', 50)), 100)  # Max 100 results
        offset = int(request.args.get('offset', 0))
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Map sort_by parameter to actual column names
        sort_columns = {
            'points': 'up.total_points',
            'value': 'total_value',
            'wins': 'win_count'
        }
        
        sort_column = sort_columns.get(sort_by, 'up.total_points')
        sort_order = "DESC" if order == "DESC" else "ASC"
        
        # Build the query based on the sorting requirements
        query = f'''
        WITH user_values AS (
            SELECT 
                frog_address as address,
                SUM(CAST(amount as DECIMAL)) as total_value
            FROM coin_tossed_events
            GROUP BY frog_address
        ),
        user_wins AS (
            SELECT 
                winner_address as address,
                COUNT(*) as win_count
            FROM lucky_winner_selected_events
            GROUP BY winner_address
        ),
        user_referrals AS (
            SELECT 
                address,
                referral_points_earned
            FROM user_referrals
        )
        SELECT 
            up.address,
            up.total_points,
            COALESCE(uv.total_value, 0) as total_value,
            COALESCE(uw.win_count, 0) as win_count,
            COALESCE(ur.referral_points_earned, 0) as referral_points
        FROM user_points up
        LEFT JOIN user_values uv ON up.address = uv.address
        LEFT JOIN user_wins uw ON up.address = uw.address
        LEFT JOIN user_referrals ur ON up.address = ur.address
        ORDER BY {sort_column} {sort_order}
        LIMIT ? OFFSET ?
        '''
        
        cursor.execute(query, (limit, offset))
        users = cursor.fetchall()
        
        # Count total users for pagination info
        cursor.execute('SELECT COUNT(*) FROM user_points')
        total_users = cursor.fetchone()[0]
        
        # Convert to list of dictionaries
        result = []
        for user in users:
            result.append({
                "address": user['address'],
                "total_points": user['total_points'],
                "total_value_spent": str(user['total_value']),
                "total_wins": user['win_count'],
                "referral_points": user['referral_points']
            })
        
        conn.close()
        
        return jsonify({
            "leaderboard": result,
            "total_users": total_users,
            "limit": limit,
            "offset": offset,
            "sort_by": sort_by,
            "order": order
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting leaderboard: {e}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/user/<address>', methods=['GET'])
@require_api_key
def get_user_data(address):
    """Get detailed data for a specific user."""
    try:
        # Normalize address
        address = address.lower()
        
        # Use the imported function from referral_system
        user_data = get_user_stats(address)
        
        # If the user doesn't exist, we'll still return the default structure
        # from get_user_stats but with a 404 status
        if user_data["total_tosses"] == 0 and user_data["total_wins"] == 0 and user_data["total_points"] == 0:
            return jsonify(user_data), 404
        
        return jsonify(user_data), 200
        
    except Exception as e:
        logger.error(f"Error getting user data: {e}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/winners/recent', methods=['GET'])
@require_api_key
def get_recent_winners():
    """
    Get list of recent winners.
    Query parameters:
    - limit: number of results (default 20)
    - offset: pagination offset (default 0)
    - pond_type: filter by pond type (optional)
    """
    try:
        limit = min(int(request.args.get('limit', 20)), 100)  # Max 100 results
        offset = int(request.args.get('offset', 0))
        pond_type = request.args.get('pond_type')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = '''
        SELECT 
            lw.tx_hash,
            lw.block_number,
            lw.block_timestamp,
            lw.pond_type,
            lw.winner_address,
            lw.prize,
            lw.selector
        FROM lucky_winner_selected_events lw
        '''
        
        params = []
        
        # Add pond_type filter if provided
        if pond_type:
            query += ' WHERE lw.pond_type = ?'
            params.append(pond_type)
        
        # Add ordering and pagination
        query += ' ORDER BY lw.block_timestamp DESC LIMIT ? OFFSET ?'
        params.extend([limit, offset])
        
        cursor.execute(query, params)
        winners = cursor.fetchall()
        
        # Count total winners for pagination info
        count_query = 'SELECT COUNT(*) FROM lucky_winner_selected_events'
        if pond_type:
            count_query += ' WHERE pond_type = ?'
            cursor.execute(count_query, [pond_type])
        else:
            cursor.execute(count_query)
        
        total_winners = cursor.fetchone()[0]
        
        # Convert to list of dictionaries
        result = []
        for winner in winners:
            # Format timestamp as ISO date
            timestamp = datetime.fromtimestamp(winner['block_timestamp']).isoformat()
            
            result.append({
                "tx_hash": winner['tx_hash'],
                "block_number": winner['block_number'],
                "timestamp": timestamp,
                "pond_type": winner['pond_type'],
                "winner_address": winner['winner_address'],
                "prize": winner['prize'],
                "selector": winner['selector']
            })
        
        conn.close()
        
        return jsonify({
            "winners": result,
            "total_winners": total_winners,
            "limit": limit,
            "offset": offset
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting recent winners: {e}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/referral/code/<address>', methods=['GET'])
@require_api_key
def get_referral_code(address):
    """Get or create a referral code for a user."""
    try:
        # Normalize address
        address = address.lower()
        
        # Use the imported function
        user_referral = get_or_create_user_referral(address)
        
        # Check if this user already has a referrer
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT referrer_address FROM user_referrals WHERE address = ?', (address,))
        result = cursor.fetchone()
        has_referrer = result and result['referrer_address'] is not None
        conn.close()
        
        return jsonify({
            "address": address,
            "referral_code": user_referral["referral_code"],
            "created_at": user_referral["created_at"],
            "has_referrer": has_referrer,
            "code_locked": has_referrer  # Indicate whether the user can apply another code
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting referral code: {e}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/referral/apply', methods=['POST'])
@require_api_key
def apply_referral():
    """Apply a referral code to a user account."""
    try:
        data = request.json
        
        if not data or 'address' not in data or 'referral_code' not in data:
            return jsonify({
                "status": "error",
                "message": "Missing required fields: address and referral_code"
            }), 400
        
        # Normalize address
        address = data['address'].lower()
        referral_code = data['referral_code'].upper()
        
        # First check if the user already has a referrer
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT referrer_address FROM user_referrals WHERE address = ?', (address,))
        result = cursor.fetchone()
        conn.close()
        
        if result and result['referrer_address']:
            return jsonify({
                "status": "error",
                "message": "Cannot change referrer: this account already has a referrer code applied",
                "locked": True
            }), 403  # Forbidden - explicitly show this is not allowed
        
        # If no existing referrer, use the imported function to apply the code
        success, message = apply_referral_code(address, referral_code)
        
        if success:
            return jsonify({
                "status": "success",
                "message": message
            }), 200
        else:
            return jsonify({
                "status": "error",
                "message": message
            }), 400
        
    except Exception as e:
        logger.error(f"Error applying referral code: {e}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500
    
@app.route('/', methods=['GET'])
def api_documentation():
    """API documentation endpoint."""
    
    # Show API auth status in documentation
    auth_status = "required" if REQUIRE_AUTH else "disabled"
    
    return jsonify({
        "name": "Lucky Ponds API",
        "version": "1.0.0",
        "authentication": auth_status,
        "endpoints": {
            "/": {
                "methods": ["GET"],
                "description": "API documentation",
                "authentication": "Not required"
            },
            "/health": {
                "methods": ["GET"],
                "description": "API health check",
                "authentication": "Not required"
            },
            "/indexer/status": {
                "methods": ["GET"],
                "description": "Get the current status of the blockchain indexer",
                "authentication": auth_status
            },
            "/leaderboard": {
                "methods": ["GET"],
                "description": "Get the global leaderboard with sorting options",
                "authentication": auth_status,
                "parameters": {
                    "sort_by": "Field to sort by (points, value, wins)",
                    "order": "Sort order (asc, desc)",
                    "limit": "Number of results to return",
                    "offset": "Pagination offset"
                }
            },
            "/user/<address>": {
                "methods": ["GET"],
                "description": "Get detailed data for a specific user",
                "authentication": auth_status
            },
            "/winners/recent": {
                "methods": ["GET"],
                "description": "Get list of recent winners",
                "authentication": auth_status,
                "parameters": {
                    "limit": "Number of results to return",
                    "offset": "Pagination offset",
                    "pond_type": "Filter by pond type (optional)"
                }
            },
            "/referral/code/<address>": {
                "methods": ["GET"],
                "description": "Get or create a referral code for a user",
                "authentication": auth_status
            },
            "/referral/apply": {
                "methods": ["POST"],
                "description": "Apply a referral code to a user account",
                "authentication": auth_status,
                "body": {
                    "address": "User address",
                    "referral_code": "Referral code to apply"
                }
            }
        },
        "authentication_info": {
            "method": "API Key",
            "header": "X-API-Key",
            "status": auth_status
        }
    }), 200

# Add CORS support
@app.after_request
def add_cors_headers(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
    return response

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "status": "error",
        "message": "Endpoint not found"
    }), 404

@app.errorhandler(401)
def unauthorized(error):
    return jsonify({
        "status": "error",
        "message": "Unauthorized. API key required.",
        "authentication_method": "API Key via X-API-Key header"
    }), 401

@app.errorhandler(500)
def server_error(error):
    return jsonify({
        "status": "error",
        "message": "Internal server error"
    }), 500

if __name__ == '__main__':
    logger.info(f"Starting API server on port {API_PORT}")
    app.run(host='0.0.0.0', port=API_PORT)