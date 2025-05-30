import os
import time
import logging
import functools
from datetime import datetime
from flask import Flask, jsonify, request
from dotenv import load_dotenv
from typing import Dict, List, Any, Optional

# Import our database access layers and utilities
from data_access import EventsDatabase, ApplicationDatabase
from utils import (
    get_events_db_path, 
    get_app_db_path, 
    get_referral_bonus_points,
    get_current_timestamp,
    setup_logger
)

# Import the referral system
from referral_system import ReferralSystem

# Configure logging
logger = setup_logger('api')

# Load environment variables
load_dotenv()

# Configuration
EVENTS_DB_PATH = get_events_db_path()
APP_DB_PATH = get_app_db_path()
API_PORT = int(os.getenv("API_PORT", "5000"))
API_KEY = os.getenv("API_KEY", "")  # Authentication key for protected endpoints
REQUIRE_AUTH = os.getenv("REQUIRE_AUTH", "false").lower() == "true"  # Whether authentication is required

# Initialize Flask app
app = Flask(__name__)

# Initialize databases
events_db = EventsDatabase(EVENTS_DB_PATH)
app_db = ApplicationDatabase(APP_DB_PATH)

# Initialize referral system
referral_system = ReferralSystem(APP_DB_PATH)

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

@app.route('/health', methods=['GET'])
def health_check():
    """API health check endpoint."""
    try:
        # Try to connect to both databases
        events_conn = events_db.get_connection()
        app_conn = app_db.get_connection()
        
        # Execute a simple query on each
        cursor1 = events_conn.cursor()
        cursor1.execute('SELECT 1')
        cursor1.fetchone()
        events_conn.close()
        
        cursor2 = app_conn.cursor()
        cursor2.execute('SELECT 1')
        cursor2.fetchone()
        app_conn.close()
        
        return jsonify({
            "status": "healthy",
            "message": "API is running and databases are accessible",
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
        # Get the last indexed block from events database
        last_block = events_db.get_last_indexed_block()
        
        # Get event counts from events database
        conn = events_db.get_connection()
        cursor = conn.cursor()
        
        # Get toss count
        cursor.execute('SELECT COUNT(*) FROM coin_tossed_events')
        toss_count = cursor.fetchone()[0]
        
        # Get winner count
        cursor.execute('SELECT COUNT(*) FROM lucky_winner_selected_events')
        winner_count = cursor.fetchone()[0]
        
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
        
        # Get user count from application database
        app_conn = app_db.get_connection()
        app_cursor = app_conn.cursor()
        app_cursor.execute('SELECT COUNT(*) FROM user_points')
        user_count = app_cursor.fetchone()[0]
        
        # Get calculator state from application database
        app_cursor.execute('SELECT * FROM calculator_state WHERE id = 1')
        calculator_state = app_cursor.fetchone()
        
        # Close connections
        conn.close()
        app_conn.close()
        
        # Format results
        last_event_time = datetime.fromtimestamp(last_event_timestamp) if last_event_timestamp else None
        
        return jsonify({
            "status": "active",
            "last_indexed_block": last_block,
            "total_toss_events": toss_count,
            "total_winner_events": winner_count,
            "total_users": user_count,
            "last_event_timestamp": last_event_timestamp,
            "last_event_time": last_event_time.isoformat() if last_event_time else None,
            "calculator_state": {
                "last_processed_toss_id": calculator_state['last_processed_toss_id'] if calculator_state else 0,
                "last_processed_winner_id": calculator_state['last_processed_winner_id'] if calculator_state else 0,
                "last_run_timestamp": calculator_state['last_run_timestamp'] if calculator_state else 0
            },
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
    - sort_by: field to sort by (points, toss_points, winner_points, referral_points)
    - order: asc or desc
    - limit: number of results (default 50)
    - offset: pagination offset (default 0)
    """
    try:
        sort_by = request.args.get('sort_by', 'total_points')
        order = request.args.get('order', 'desc').upper()
        limit = min(int(request.args.get('limit', 50)), 100)  # Max 100 results
        offset = int(request.args.get('offset', 0))
        
        # Map sort_by parameter to actual column names
        sort_columns = {
            'total_points': 'up.total_points',
            'toss_points': 'up.toss_points',
            'winner_points': 'up.winner_points',
            'referral_points': 'up.referral_points'
        }
        
        sort_column = sort_columns.get(sort_by, 'up.total_points')
        sort_order = "DESC" if order == "DESC" else "ASC"
        
        # Get connection to application database
        conn = app_db.get_connection()
        cursor = conn.cursor()
        
        # Build query for leaderboard
        query = f'''
        SELECT 
            up.address,
            up.total_points,
            up.toss_points,
            up.winner_points,
            up.referral_points,
            ur.referral_code,
            (SELECT COUNT(*) FROM user_referrals WHERE referrer_address = up.address) as referrals_count,
            (SELECT COUNT(*) FROM user_referrals WHERE referrer_address = up.address AND is_activated = 1) as activated_referrals
        FROM user_points up
        LEFT JOIN user_referrals ur ON up.address = ur.address
        ORDER BY {sort_column} {sort_order}
        LIMIT ? OFFSET ?
        '''
        
        cursor.execute(query, (limit, offset))
        users = cursor.fetchall()
        
        # Count total users for pagination info
        cursor.execute('SELECT COUNT(*) FROM user_points')
        total_users = cursor.fetchone()[0]
        
        # Connect to events database to get toss and win data
        events_conn = events_db.get_connection()
        events_cursor = events_conn.cursor()
        
        # Convert to list of dictionaries with full user stats
        result = []
        for user in users:
            address = user['address']
            
            # Get toss data
            events_cursor.execute('''
            SELECT COUNT(*) as toss_count, SUM(CAST(amount as DECIMAL)) as total_value
            FROM coin_tossed_events
            WHERE frog_address = ?
            ''', (address,))
            toss_data = events_cursor.fetchone()
            
            # Get win data
            events_cursor.execute('''
            SELECT COUNT(*) as win_count
            FROM lucky_winner_selected_events
            WHERE winner_address = ?
            ''', (address,))
            win_data = events_cursor.fetchone()
            
            # Build user stats
            user_stats = {
                "address": address,
                "total_points": user['total_points'],
                "toss_points": user['toss_points'],
                "winner_points": user['winner_points'],
                "referral_points": user['referral_points'],
                "referral_code": user['referral_code'],
                "referrals_count": user['referrals_count'],
                "referrals_activated": user['activated_referrals'],
                "total_tosses": toss_data['toss_count'] if toss_data else 0,
                "total_value_spent": str(toss_data['total_value'] or 0) if toss_data else "0",
                "total_wins": win_data['win_count'] if win_data else 0
            }
            result.append(user_stats)
        
        # Close connections
        conn.close()
        events_conn.close()
        
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
        
        # Get application database connection
        app_conn = app_db.get_connection()
        app_cursor = app_conn.cursor()
        
        # Initialize result structure
        result = {
            "address": address,
            "total_points": 0,
            "toss_points": 0,
            "winner_points": 0,
            "referral_points": 0,
            "referral_code": None,
            "referrer_code_used": None,
            "referrals_count": 0,        # Total people who used this user's code
            "referrals_activated": 0,    # Activated referrals (people who made tosses)
            "total_tosses": 0,
            "total_value_spent": "0",
            "total_wins": 0
        }
        
        # Get points info
        app_cursor.execute('SELECT * FROM user_points WHERE address = ?', (address,))
        points_data = app_cursor.fetchone()
        if points_data:
            result["total_points"] = points_data["total_points"]
            result["toss_points"] = points_data["toss_points"]
            result["winner_points"] = points_data["winner_points"]
            result["referral_points"] = points_data["referral_points"]
        
        # Get referral info for this user
        app_cursor.execute('SELECT * FROM user_referrals WHERE address = ?', (address,))
        referral_data = app_cursor.fetchone()
        if referral_data:
            result["referral_code"] = referral_data["referral_code"]
            
            # If this user has a referrer, get that referrer's code
            if referral_data["referrer_address"]:
                app_cursor.execute('SELECT referral_code FROM user_referrals WHERE address = ?', 
                               (referral_data["referrer_address"],))
                referrer_code = app_cursor.fetchone()
                if referrer_code:
                    result["referrer_code_used"] = referrer_code["referral_code"]
        
        # Count referrals: people who used THIS user's referral code
        app_cursor.execute('''
        SELECT 
            COUNT(*) as total_referrals,
            COALESCE(SUM(is_activated), 0) as active_referrals
        FROM user_referrals 
        WHERE referrer_address = ?
        ''', (address,))
        
        referral_counts = app_cursor.fetchone()
        if referral_counts:
            result["referrals_count"] = referral_counts["total_referrals"] or 0
            result["referrals_activated"] = referral_counts["active_referrals"] or 0
        
        # Get events database connection for toss and win data
        events_conn = events_db.get_connection()
        events_cursor = events_conn.cursor()
        
        # Get toss info
        events_cursor.execute('''
        SELECT COUNT(*), COALESCE(SUM(CAST(amount as DECIMAL)), 0) 
        FROM coin_tossed_events 
        WHERE frog_address = ?
        ''', (address,))
        
        toss_data = events_cursor.fetchone()
        if toss_data:
            result["total_tosses"] = toss_data[0]
            result["total_value_spent"] = str(toss_data[1])
        
        # Get win info
        events_cursor.execute('''
        SELECT COUNT(*) 
        FROM lucky_winner_selected_events 
        WHERE winner_address = ?
        ''', (address,))
        
        win_count = events_cursor.fetchone()
        if win_count:
            result["total_wins"] = win_count[0]
        
        # Close connections
        app_conn.close()
        events_conn.close()
        
        # Check if the user exists (has any activity)
        if (result["total_tosses"] == 0 and result["total_wins"] == 0 and 
            not points_data and not referral_data):
            return jsonify(result), 404
        
        return jsonify(result), 200
        
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
        
        # Get events database connection
        conn = events_db.get_connection()
        cursor = conn.cursor()
        
        # Build query
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
        
        # Use the referral system to get or create the code
        user_referral = referral_system.get_or_create_user_referral(address)
        
        # Check if this user already has a referrer
        app_conn = app_db.get_connection()
        cursor = app_conn.cursor()
        cursor.execute('SELECT referrer_address FROM user_referrals WHERE address = ?', (address,))
        result = cursor.fetchone()
        has_referrer = result and result['referrer_address'] is not None
        app_conn.close()
        
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
        
        # Use the referral system to apply the code
        success, message = referral_system.apply_referral_code(address, referral_code)
        
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

@app.route('/events/tosses/<address>', methods=['GET'])
@require_api_key
def get_user_tosses(address):
    """
    Get list of coin toss events for a specific user.
    Query parameters:
    - limit: number of results (default 20)
    - offset: pagination offset (default 0)
    """
    try:
        limit = min(int(request.args.get('limit', 20)), 100)  # Max 100 results
        offset = int(request.args.get('offset', 0))
        address = address.lower()
        
        # Get events database connection
        conn = events_db.get_connection()
        cursor = conn.cursor()
        
        # Get toss events
        cursor.execute('''
        SELECT 
            id, tx_hash, block_number, block_timestamp, pond_type, 
            amount, timestamp, total_pond_tosses, total_pond_value
        FROM coin_tossed_events 
        WHERE frog_address = ?
        ORDER BY block_timestamp DESC
        LIMIT ? OFFSET ?
        ''', (address, limit, offset))
        
        tosses = cursor.fetchall()
        
        # Count total tosses for pagination
        cursor.execute('SELECT COUNT(*) FROM coin_tossed_events WHERE frog_address = ?', (address,))
        total_tosses = cursor.fetchone()[0]
        
        # Convert to list of dictionaries
        result = []
        for toss in tosses:
            # Format timestamp as ISO date
            timestamp = datetime.fromtimestamp(toss['block_timestamp']).isoformat()
            
            result.append({
                "id": toss['id'],
                "tx_hash": toss['tx_hash'],
                "block_number": toss['block_number'],
                "timestamp": timestamp,
                "pond_type": toss['pond_type'],
                "amount": toss['amount'],
                "total_pond_tosses": toss['total_pond_tosses'],
                "total_pond_value": toss['total_pond_value']
            })
        
        conn.close()
        
        return jsonify({
            "address": address,
            "tosses": result,
            "total_tosses": total_tosses,
            "limit": limit,
            "offset": offset
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting user tosses: {e}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/events/wins/<address>', methods=['GET'])
@require_api_key
def get_user_wins(address):
    """
    Get list of winner events for a specific user.
    Query parameters:
    - limit: number of results (default 20)
    - offset: pagination offset (default 0)
    """
    try:
        limit = min(int(request.args.get('limit', 20)), 100)  # Max 100 results
        offset = int(request.args.get('offset', 0))
        address = address.lower()
        
        # Get events database connection
        conn = events_db.get_connection()
        cursor = conn.cursor()
        
        # Get win events
        cursor.execute('''
        SELECT 
            id, tx_hash, block_number, block_timestamp, pond_type, 
            prize, selector
        FROM lucky_winner_selected_events 
        WHERE winner_address = ?
        ORDER BY block_timestamp DESC
        LIMIT ? OFFSET ?
        ''', (address, limit, offset))
        
        wins = cursor.fetchall()
        
        # Count total wins for pagination
        cursor.execute('SELECT COUNT(*) FROM lucky_winner_selected_events WHERE winner_address = ?', (address,))
        total_wins = cursor.fetchone()[0]
        
        # Convert to list of dictionaries
        result = []
        for win in wins:
            # Format timestamp as ISO date
            timestamp = datetime.fromtimestamp(win['block_timestamp']).isoformat()
            
            result.append({
                "id": win['id'],
                "tx_hash": win['tx_hash'],
                "block_number": win['block_number'],
                "timestamp": timestamp,
                "pond_type": win['pond_type'],
                "prize": win['prize'],
                "selector": win['selector']
            })
        
        conn.close()
        
        return jsonify({
            "address": address,
            "wins": result,
            "total_wins": total_wins,
            "limit": limit,
            "offset": offset
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting user wins: {e}")
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
        "version": "2.0.0",
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
                    "sort_by": "Field to sort by (total_points, toss_points, winner_points, referral_points)",
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
            },
            "/events/tosses/<address>": {
                "methods": ["GET"],
                "description": "Get list of coin toss events for a specific user",
                "authentication": auth_status,
                "parameters": {
                    "limit": "Number of results to return",
                    "offset": "Pagination offset"
                }
            },
            "/events/wins/<address>": {
                "methods": ["GET"],
                "description": "Get list of winner events for a specific user",
                "authentication": auth_status,
                "parameters": {
                    "limit": "Number of results to return",
                    "offset": "Pagination offset"
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
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-API-Key')
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