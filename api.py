@app.route('/api/users/<address>/points', methods=['GET'])
def get_user_points(address):
    """Get points details for a specific user."""
    address = address.lower()
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get user points
    cursor.execute('''
    SELECT * FROM user_points 
    WHERE address = ?
    ''', (address,))
    
    user_points = cursor.fetchone()
    if not user_points:
        # User has no points yet, return default values
        result = {
            "address": address,
            "total_points": 0,
            "toss_points": 0,
            "max_toss_points": 0,
            "winner_points": 0,
            "last_updated": 0
        }
    else:
        result = dict(user_points)
    
    # Get point history
    cursor.execute('''
    SELECT * FROM user_point_events 
    WHERE address = ? 
    ORDER BY timestamp DESC 
    LIMIT 100
    ''', (address,))
    
    point_history = [dict(row) for row in cursor.fetchall()]
    result["point_history"] = point_history
    
    # Get user's rank in the leaderboard
    cursor.execute('''
    SELECT COUNT(*) + 1 as rank FROM user_points 
    WHERE total_points > (
        SELECT total_points FROM user_points WHERE address = ?
    )
    ''', (address,))
    
    rank_result = cursor.fetchone()
    result["rank"] = rank_result['rank'] if rank_result else 0
    
    conn.close()
    
    return jsonify(result)

@app.route('/api/leaderboard/points', methods=['GET'])
def get_points_leaderboard():
    """Get leaderboard of users ranked by total points."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Query parameters
    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)
    timeframe = request.args.get('timeframe')  # Optional timeframe: 'day', 'week', 'month', 'all'
    
    # Construct timeframe filter if provided
    timeframe_filter = ""
    if timeframe:
        current_time = int(time.time())
        if timeframe == 'day':
            timeframe_filter = f"WHERE last_updated >= {current_time - 86400}"
        elif timeframe == 'week':
            timeframe_filter = f"WHERE last_updated >= {current_time - 604800}"
        elif timeframe == 'month':
            timeframe_filter = f"WHERE last_updated >= {current_time - 2592000}"
        elif timeframe == 'year':
            timeframe_filter = f"WHERE last_updated >= {current_time - 31536000}"
    
    # Get top users by total points
    query = f"""
    SELECT 
        address,
        total_points,
        toss_points,
        max_toss_points,
        winner_points,
        last_updated,
        ROW_NUMBER() OVER (ORDER BY total_points DESC) as rank
    FROM user_points
    {timeframe_filter}
    ORDER BY total_points DESC
    LIMIT ? OFFSET ?
    """
    
    cursor.execute(query, (limit, offset))
    top_users = [dict(row) for row in cursor.fetchall()]
    
    # Get total count for pagination
    if timeframe_filter:
        cursor.execute(f'SELECT COUNT(*) as count FROM user_points {timeframe_filter}')
    else:
        cursor.execute('SELECT COUNT(*) as count FROM user_points')
    
    total_count = cursor.fetchone()['count']
    
    # Get total points in the system
    cursor.execute('SELECT SUM(total_points) as total FROM user_points')
    total_points = cursor.fetchone()['total'] or 0
    
    # Get category leaders
    cursor.execute('''
    SELECT address, toss_points FROM user_points 
    ORDER BY toss_points DESC LIMIT 1
    ''')
    top_tosser = cursor.fetchone()
    
    cursor.execute('''
    SELECT address, max_toss_points FROM user_points 
    ORDER BY max_toss_points DESC LIMIT 1
    ''')
    top_max_tosser = cursor.fetchone()
    
    cursor.execute('''
    SELECT address, winner_points FROM user_points 
    ORDER BY winner_points DESC LIMIT 1
    ''')
    top_winner = cursor.fetchone()
    
    conn.close()
    
    return jsonify({
        "leaderboard": top_users,
        "pagination": {
            "total_count": total_count,
            "limit": limit,
            "offset": offset
        },
        "stats": {
            "total_points_awarded": total_points,
            "top_tosser": dict(top_tosser) if top_tosser else None,
            "top_max_tosser": dict(top_max_tosser) if top_max_tosser else None,
            "top_winner": dict(top_winner) if top_winner else None
        },
        "timeframe": timeframe or "all"
    })

@app.route('/api/leaderboard/categories', methods=['GET'])
def get_category_leaderboards():
    """Get leaderboards for each point category."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Query parameters
    limit = request.args.get('limit', 10, type=int)
    
    # Get top tossers
    cursor.execute('''
    SELECT 
        address, 
        toss_points,
        ROW_NUMBER() OVER (ORDER BY toss_points DESC) as rank
    FROM user_points 
    ORDER BY toss_points DESC 
    LIMIT ?
    ''', (limit,))
    
    top_tossers = [dict(row) for row in cursor.fetchall()]
    
    # Get top max tossers
    cursor.execute('''
    SELECT 
        address, 
        max_toss_points,
        ROW_NUMBER() OVER (ORDER BY max_toss_points DESC) as rank
    FROM user_points 
    ORDER BY max_toss_points DESC 
    LIMIT ?
    ''', (limit,))
    
    top_max_tossers = [dict(row) for row in cursor.fetchall()]
    
    # Get top winners
    cursor.execute('''
    SELECT 
        address, 
        winner_points,
        ROW_NUMBER() OVER (ORDER BY winner_points DESC) as rank
    FROM user_points 
    ORDER BY winner_points DESC 
    LIMIT ?
    ''', (limit,))
    
    top_winners = [dict(row) for row in cursor.fetchall()]
    
    conn.close()
    
    return jsonify({
        "top_tossers": top_tossers,
        "top_max_tossers": top_max_tossers,
        "top_winners": top_winners,
        "limit": limit
    })

@app.route('/api/pond-points', methods=['GET'])
def get_pond_points():
    """Get point values for each pond type."""
    return jsonify({
        "pond_points": {
            "five_min": {
                "pond_type": FIVE_MIN_POND_TYPE,
                "points": 1,
                "max_toss_bonus": 1  # Additional points for max toss
            },
            "hourly": {
                "pond_type": HOURLY_POND_TYPE,
                "points": 5,
                "max_toss_bonus": 5
            },
            "daily": {
                "pond_type": DAILY_POND_TYPE,
                "points": 10,
                "max_toss_bonus": 10
            },
            "weekly": {
                "pond_type": WEEKLY_POND_TYPE,
                "points": 20,
                "max_toss_bonus": 20
            },
            "monthly": {
                "pond_type": MONTHLY_POND_TYPE,
                "points": 50,
                "max_toss_bonus": 50
            },
            "winner_bonus": 25  # Points for being selected as a winner
        }
    })#!/usr/bin/env python3
import os
import sqlite3
import time
from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
DB_PATH = os.getenv("DB_PATH", "lucky_ponds.db")
API_PORT = int(os.getenv("API_PORT", "5000"))
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS", "").lower()

# Define pond type constants
FIVE_MIN_POND_TYPE = "0x4608d971a2c5e7a58fc11b6e24dfb34a5d5229ba79a246c8db8bff13c28585e3"
HOURLY_POND_TYPE = "0x71436e6480b02d0a0d9d1b32f2605b5a8d5bf57edc5276dbae776a3205ff042a"
DAILY_POND_TYPE = "0x84eebf87e6e26633aeb5b6fb33eabeeade8b46fb27ee88a8c28ef70231ebd6a8"
WEEKLY_POND_TYPE = "0xe1f30d5367a00d703c7de2a91f675de0b1b59b1d7a662b30b1512a39d217148c"
MONTHLY_POND_TYPE = "0xe0069269e2394a85569da74fd56114a3b0219c4ffecfaeb48a5e2a13ee8b4f97"

# Map pond types to point values
POND_POINTS = {
    FIVE_MIN_POND_TYPE: 1,
    HOURLY_POND_TYPE: 5,
    DAILY_POND_TYPE: 10,
    WEEKLY_POND_TYPE: 20,
    MONTHLY_POND_TYPE: 50
}

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ["https://luckyponds.xyz", "http://localhost:3000"]}})

def get_db_connection():
    """Create a database connection and return the connection object."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # This enables column access by name
    return conn

def convert_to_hex(pond_type_bytes):
    """Helper function to convert pond_type from bytes to hex string if needed."""
    if isinstance(pond_type_bytes, bytes):
        return "0x" + pond_type_bytes.hex()
    return pond_type_bytes

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({"status": "ok"})

@app.route('/api/indexer/status', methods=['GET'])
def indexer_status():
    """Get the current indexer status."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT last_block FROM indexer_state WHERE id = 1')
    last_indexed_block = cursor.fetchone()['last_block']
    
    # Count events in each table
    cursor.execute('SELECT COUNT(*) as count FROM coin_tossed_events')
    coin_tossed_count = cursor.fetchone()['count']
    
    cursor.execute('SELECT COUNT(*) as count FROM lucky_winner_selected_events')
    lucky_winner_count = cursor.fetchone()['count']
    
    cursor.execute('SELECT COUNT(*) as count FROM pond_action_events')
    pond_action_count = cursor.fetchone()['count']
    
    cursor.execute('SELECT COUNT(*) as count FROM config_changed_events')
    config_changed_count = cursor.fetchone()['count']
    
    cursor.execute('SELECT COUNT(*) as count FROM emergency_action_events')
    emergency_action_count = cursor.fetchone()['count']
    
    conn.close()
    
    return jsonify({
        "last_indexed_block": last_indexed_block,
        "event_counts": {
            "coin_tossed": coin_tossed_count,
            "lucky_winner_selected": lucky_winner_count,
            "pond_action": pond_action_count,
            "config_changed": config_changed_count,
            "emergency_action": emergency_action_count,
            "total": coin_tossed_count + lucky_winner_count + pond_action_count + 
                    config_changed_count + emergency_action_count
        }
    })

@app.route('/api/ponds', methods=['GET'])
def get_ponds():
    """Get all pond types from the PondAction events."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT DISTINCT pond_type, name 
    FROM pond_action_events 
    WHERE action_type = 'created' 
    ORDER BY block_timestamp DESC
    ''')
    
    ponds = [{"pond_type": convert_to_hex(row['pond_type']), "name": row['name']} for row in cursor.fetchall()]
    conn.close()
    
    return jsonify(ponds)

@app.route('/api/ponds/<pond_type>', methods=['GET'])
def get_pond_info(pond_type):
    """Get information about a specific pond."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get the latest pond action for this pond type
    cursor.execute('''
    SELECT * FROM pond_action_events 
    WHERE pond_type = ? 
    ORDER BY block_timestamp DESC 
    LIMIT 1
    ''', (pond_type,))
    
    pond_action = cursor.fetchone()
    if not pond_action:
        conn.close()
        return jsonify({"error": "Pond not found"}), 404
    
    # Get the latest configuration
    cursor.execute('''
    SELECT * FROM config_changed_events 
    WHERE pond_type = ? 
    ORDER BY block_timestamp DESC
    ''', (pond_type,))
    
    config_updates = [dict(row) for row in cursor.fetchall()]
    
    # Get the total participants and value
    cursor.execute('''
    SELECT COUNT(DISTINCT frog_address) as participant_count, 
           SUM(amount) as total_value 
    FROM coin_tossed_events 
    WHERE pond_type = ?
    ''', (pond_type,))
    
    stats = cursor.fetchone()
    
    # Get the latest winner if any
    cursor.execute('''
    SELECT * FROM lucky_winner_selected_events 
    WHERE pond_type = ? 
    ORDER BY block_timestamp DESC 
    LIMIT 1
    ''', (pond_type,))
    
    winner = cursor.fetchone()
    
    result = {
        "pond_type": convert_to_hex(pond_action['pond_type']),
        "name": pond_action['name'],
        "start_time": pond_action['start_time'],
        "end_time": pond_action['end_time'],
        "participant_count": stats['participant_count'] if stats else 0,
        "total_value": stats['total_value'] if stats else "0",
        "latest_winner": dict(winner) if winner else None,
        "config_updates": config_updates
    }
    
    conn.close()
    return jsonify(result)

@app.route('/api/ponds/<pond_type>/tosses', methods=['GET'])
def get_pond_tosses(pond_type):
    """Get all coin tosses for a specific pond."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Pagination parameters
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    offset = (page - 1) * per_page
    
    # Get the coin tosses for this pond
    cursor.execute('''
    SELECT * FROM coin_tossed_events 
    WHERE pond_type = ? 
    ORDER BY block_timestamp DESC 
    LIMIT ? OFFSET ?
    ''', (pond_type, per_page, offset))
    
    tosses = [dict(row) for row in cursor.fetchall()]
    
    # Get the total count for pagination
    cursor.execute('SELECT COUNT(*) as count FROM coin_tossed_events WHERE pond_type = ?', (pond_type,))
    total_count = cursor.fetchone()['count']
    
    conn.close()
    
    return jsonify({
        "pond_type": convert_to_hex(pond_type) if pond_type else pond_type,
        "tosses": tosses,
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total_count": total_count,
            "total_pages": (total_count + per_page - 1) // per_page
        }
    })

@app.route('/api/ponds/<pond_type>/winners', methods=['GET'])
def get_pond_winners(pond_type):
    """Get all winners for a specific pond."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get the winners for this pond
    cursor.execute('''
    SELECT * FROM lucky_winner_selected_events 
    WHERE pond_type = ? 
    ORDER BY block_timestamp DESC
    ''', (pond_type,))
    
    winners = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    return jsonify({
        "pond_type": convert_to_hex(pond_type) if pond_type else pond_type,
        "winners": winners
    })

@app.route('/api/ponds/<pond_type>/emergency-actions', methods=['GET'])
def get_pond_emergency_actions(pond_type):
    """Get all emergency actions for a specific pond."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get the emergency actions for this pond
    cursor.execute('''
    SELECT * FROM emergency_action_events 
    WHERE pond_type = ? 
    ORDER BY block_timestamp DESC
    ''', (pond_type,))
    
    actions = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    return jsonify({
        "pond_type": convert_to_hex(pond_type) if pond_type else pond_type,
        "emergency_actions": actions
    })

@app.route('/api/users/<address>/participation', methods=['GET'])
def get_user_participation(address):
    """Get participation details for a specific user."""
    address = address.lower()
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get all tosses by this user
    cursor.execute('''
    SELECT * FROM coin_tossed_events 
    WHERE frog_address = ? 
    ORDER BY block_timestamp DESC
    ''', (address,))
    
    tosses = [dict(row) for row in cursor.fetchall()]
    
    # Get all wins by this user
    cursor.execute('''
    SELECT * FROM lucky_winner_selected_events 
    WHERE winner_address = ? 
    ORDER BY block_timestamp DESC
    ''', (address,))
    
    wins = [dict(row) for row in cursor.fetchall()]
    
    # Aggregate stats by pond
    cursor.execute('''
    SELECT 
        pond_type,
        COUNT(*) as toss_count,
        SUM(amount) as total_amount
    FROM coin_tossed_events 
    WHERE frog_address = ?
    GROUP BY pond_type
    ''', (address,))
    
    pond_stats = {}
    for row in cursor.fetchall():
        pond_type = convert_to_hex(row['pond_type'])
        pond_stats[pond_type] = {
            "toss_count": row['toss_count'],
            "total_amount": row['total_amount']
        }
    
    # Get user points information
    cursor.execute('''
    SELECT * FROM user_points 
    WHERE address = ?
    ''', (address,))
    
    points_data = cursor.fetchone()
    points_info = dict(points_data) if points_data else {
        "total_points": 0,
        "toss_points": 0,
        "max_toss_points": 0,
        "winner_points": 0,
        "last_updated": 0
    }
    
    # Get user rank in leaderboard
    cursor.execute('''
    SELECT COUNT(*) + 1 as rank FROM user_points 
    WHERE total_points > (
        SELECT COALESCE(total_points, 0) FROM user_points WHERE address = ?
    )
    ''', (address,))
    
    rank_result = cursor.fetchone()
    points_info["rank"] = rank_result['rank'] if rank_result else 0
    
    conn.close()
    
    return jsonify({
        "address": address,
        "tosses": tosses,
        "wins": wins,
        "pond_stats": pond_stats,
        "points": points_info
    })

@app.route('/api/stats/global', methods=['GET'])
def get_global_stats():
    """Get global statistics about the contract activity."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get total tosses and value
    cursor.execute('''
    SELECT 
        COUNT(*) as total_tosses,
        SUM(amount) as total_value,
        COUNT(DISTINCT frog_address) as unique_participants
    FROM coin_tossed_events
    ''')
    
    global_stats = dict(cursor.fetchone())
    
    # Get total winners and prizes
    cursor.execute('''
    SELECT 
        COUNT(*) as total_winners,
        SUM(prize) as total_prizes
    FROM lucky_winner_selected_events
    ''')
    
    winner_stats = dict(cursor.fetchone())
    global_stats.update(winner_stats)
    
    # Get pond counts
    cursor.execute('''
    SELECT COUNT(DISTINCT pond_type) as pond_count
    FROM pond_action_events
    ''')
    
    pond_stats = dict(cursor.fetchone())
    global_stats.update(pond_stats)
    
    # Get emergency action counts
    cursor.execute('''
    SELECT COUNT(*) as emergency_action_count
    FROM emergency_action_events
    ''')
    
    emergency_stats = dict(cursor.fetchone())
    global_stats.update(emergency_stats)
    
    # Get points statistics
    cursor.execute('''
    SELECT 
        COUNT(*) as users_with_points,
        SUM(total_points) as total_points_awarded,
        SUM(toss_points) as total_toss_points,
        SUM(max_toss_points) as total_max_toss_points,
        SUM(winner_points) as total_winner_points,
        MAX(total_points) as highest_user_points
    FROM user_points
    ''')
    
    points_stats = dict(cursor.fetchone())
    global_stats.update(points_stats)
    
    # Get activity over time (daily)
    cursor.execute('''
    SELECT 
        date(block_timestamp, 'unixepoch') as day,
        COUNT(*) as toss_count,
        SUM(amount) as daily_value
    FROM coin_tossed_events
    GROUP BY day
    ORDER BY day DESC
    LIMIT 30
    ''')
    
    daily_activity = [dict(row) for row in cursor.fetchall()]
    
    # Get top point earners
    cursor.execute('''
    SELECT address, total_points 
    FROM user_points 
    ORDER BY total_points DESC 
    LIMIT 5
    ''')
    
    top_point_earners = [dict(row) for row in cursor.fetchall()]
    
    conn.close()
    
    return jsonify({
        "global_stats": global_stats,
        "daily_activity": daily_activity,
        "points_leaderboard": top_point_earners,
        "points_system": {
            "five_min_pond": 1,
            "hourly_pond": 5,
            "daily_pond": 10,
            "weekly_pond": 20,
            "monthly_pond": 50,
            "max_toss_bonus": "double points",
            "winner_bonus": 25
        }
    })

@app.route('/api/leaderboard', methods=['GET'])
def get_leaderboard():
    """Get leaderboard of top participants and winners."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Query parameters
    pond_type = request.args.get('pond_type')  # Optional pond type filter
    limit = request.args.get('limit', 10, type=int)  # Number of users to return
    timeframe = request.args.get('timeframe')  # Optional timeframe, e.g., '7d', '30d', 'all'
    
    # Construct timeframe filter if provided
    timeframe_filter = ""
    if timeframe:
        current_time = int(time.time())
        if timeframe == '1d':
            timeframe_filter = f"AND block_timestamp >= {current_time - 86400}"
        elif timeframe == '7d':
            timeframe_filter = f"AND block_timestamp >= {current_time - 604800}"
        elif timeframe == '30d':
            timeframe_filter = f"AND block_timestamp >= {current_time - 2592000}"
    
    # Construct pond type filter if provided
    pond_filter = ""
    if pond_type:
        pond_filter = f"AND pond_type = '{pond_type}'"
    
    # 1. Top tossers by count
    query_top_tossers_count = f"""
    SELECT 
        frog_address as address, 
        COUNT(*) as toss_count,
        SUM(amount) as total_value
    FROM coin_tossed_events
    WHERE 1=1 {pond_filter} {timeframe_filter}
    GROUP BY frog_address
    ORDER BY toss_count DESC
    LIMIT ?
    """
    
    cursor.execute(query_top_tossers_count, (limit,))
    top_tossers_by_count = [dict(row) for row in cursor.fetchall()]
    
    # 2. Top tossers by value
    query_top_tossers_value = f"""
    SELECT 
        frog_address as address, 
        COUNT(*) as toss_count,
        SUM(amount) as total_value
    FROM coin_tossed_events
    WHERE 1=1 {pond_filter} {timeframe_filter}
    GROUP BY frog_address
    ORDER BY total_value DESC
    LIMIT ?
    """
    
    cursor.execute(query_top_tossers_value, (limit,))
    top_tossers_by_value = [dict(row) for row in cursor.fetchall()]
    
    # 3. Top winners
    query_top_winners = f"""
    SELECT 
        winner_address as address, 
        COUNT(*) as win_count,
        SUM(prize) as total_prize
    FROM lucky_winner_selected_events
    WHERE 1=1 {pond_filter} {timeframe_filter}
    GROUP BY winner_address
    ORDER BY win_count DESC
    LIMIT ?
    """
    
    cursor.execute(query_top_winners, (limit,))
    top_winners = [dict(row) for row in cursor.fetchall()]
    
    # 4. Top win rate (minimum 5 tosses to qualify)
    query_win_rate = f"""
    WITH toss_counts AS (
        SELECT 
            frog_address as address, 
            COUNT(*) as toss_count
        FROM coin_tossed_events
        WHERE 1=1 {pond_filter} {timeframe_filter}
        GROUP BY frog_address
        HAVING toss_count >= 5
    ),
    win_counts AS (
        SELECT 
            winner_address as address, 
            COUNT(*) as win_count
        FROM lucky_winner_selected_events
        WHERE 1=1 {pond_filter} {timeframe_filter}
        GROUP BY winner_address
    )
    SELECT 
        t.address,
        t.toss_count,
        COALESCE(w.win_count, 0) as win_count,
        CAST(COALESCE(w.win_count, 0) AS FLOAT) / t.toss_count as win_rate
    FROM toss_counts t
    LEFT JOIN win_counts w ON t.address = w.address
    ORDER BY win_rate DESC
    LIMIT ?
    """
    
    cursor.execute(query_win_rate, (limit,))
    top_win_rates = [dict(row) for row in cursor.fetchall()]
    
    # 5. Most consistent tossers (tossed in the most different ponds)
    query_most_consistent = f"""
    SELECT 
        frog_address as address, 
        COUNT(DISTINCT pond_type) as pond_count,
        COUNT(*) as total_tosses
    FROM coin_tossed_events
    WHERE 1=1 {timeframe_filter}
    GROUP BY frog_address
    ORDER BY pond_count DESC, total_tosses DESC
    LIMIT ?
    """
    
    cursor.execute(query_most_consistent, (limit,))
    most_consistent_tossers = [dict(row) for row in cursor.fetchall()]
    
    # 6. Most active ponds
    query_most_active_ponds = f"""
    SELECT 
        pond_type,
        COUNT(*) as toss_count,
        COUNT(DISTINCT frog_address) as unique_participants,
        SUM(amount) as total_value
    FROM coin_tossed_events
    WHERE 1=1 {timeframe_filter}
    GROUP BY pond_type
    ORDER BY toss_count DESC
    LIMIT ?
    """
    
    cursor.execute(query_most_active_ponds, (limit,))
    most_active_ponds = []
    for row in cursor.fetchall():
        pond_dict = dict(row)
        pond_dict['pond_type'] = convert_to_hex(pond_dict['pond_type'])
        most_active_ponds.append(pond_dict)
    
    conn.close()
    
    return jsonify({
        "top_tossers_by_count": top_tossers_by_count,
        "top_tossers_by_value": top_tossers_by_value,
        "top_winners": top_winners,
        "top_win_rates": top_win_rates,
        "most_consistent_tossers": most_consistent_tossers,
        "most_active_ponds": most_active_ponds,
        "filters": {
            "pond_type": pond_type,
            "timeframe": timeframe,
            "limit": limit
        }
    })

# New endpoint for emergency actions
@app.route('/api/emergency-actions', methods=['GET'])
def get_emergency_actions():
    """Get all emergency actions across all ponds."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Pagination parameters
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    offset = (page - 1) * per_page
    
    # Get all emergency actions
    cursor.execute('''
    SELECT * FROM emergency_action_events 
    ORDER BY block_timestamp DESC 
    LIMIT ? OFFSET ?
    ''', (per_page, offset))
    
    actions = [dict(row) for row in cursor.fetchall()]
    
    # Get the total count for pagination
    cursor.execute('SELECT COUNT(*) as count FROM emergency_action_events')
    total_count = cursor.fetchone()['count']
    
    conn.close()
    
    return jsonify({
        "emergency_actions": actions,
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total_count": total_count,
            "total_pages": (total_count + per_page - 1) // per_page
        }
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=API_PORT)