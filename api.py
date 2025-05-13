#!/usr/bin/env python3
import os
import sqlite3
from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
DB_PATH = os.getenv("DB_PATH", "lucky_ponds.db")
API_PORT = int(os.getenv("API_PORT", "5000"))
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS", "").lower()

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
    
    cursor.execute('SELECT COUNT(*) as count FROM lucky_frog_selected_events')
    lucky_frog_count = cursor.fetchone()['count']
    
    cursor.execute('SELECT COUNT(*) as count FROM pond_action_events')
    pond_action_count = cursor.fetchone()['count']
    
    cursor.execute('SELECT COUNT(*) as count FROM config_updated_events')
    config_updated_count = cursor.fetchone()['count']
    
    conn.close()
    
    return jsonify({
        "last_indexed_block": last_indexed_block,
        "event_counts": {
            "coin_tossed": coin_tossed_count,
            "lucky_frog_selected": lucky_frog_count,
            "pond_action": pond_action_count,
            "config_updated": config_updated_count,
            "total": coin_tossed_count + lucky_frog_count + pond_action_count + config_updated_count
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
    SELECT * FROM config_updated_events 
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
    SELECT * FROM lucky_frog_selected_events 
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
    SELECT * FROM lucky_frog_selected_events 
    WHERE pond_type = ? 
    ORDER BY block_timestamp DESC
    ''', (pond_type,))
    
    winners = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    return jsonify({
        "pond_type": convert_to_hex(pond_type) if pond_type else pond_type,
        "winners": winners
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
    SELECT * FROM lucky_frog_selected_events 
    WHERE lucky_frog = ? 
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
    
    conn.close()
    
    return jsonify({
        "address": address,
        "tosses": tosses,
        "wins": wins,
        "pond_stats": pond_stats
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
    FROM lucky_frog_selected_events
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
    
    conn.close()
    
    return jsonify({
        "global_stats": global_stats,
        "daily_activity": daily_activity
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
        lucky_frog as address, 
        COUNT(*) as win_count,
        SUM(prize) as total_prize
    FROM lucky_frog_selected_events
    WHERE 1=1 {pond_filter} {timeframe_filter}
    GROUP BY lucky_frog
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
            lucky_frog as address, 
            COUNT(*) as win_count
        FROM lucky_frog_selected_events
        WHERE 1=1 {pond_filter} {timeframe_filter}
        GROUP BY lucky_frog
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=API_PORT)