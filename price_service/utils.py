import requests
import os
import sqlite3
from datetime import datetime, timezone

# Service URLs
PRICE_SERVICE_URL = os.getenv("PRICE_SERVICE_URL", "http://price-service:8001/prices")
FUNDAMENTAL_SERVICE_URL = os.getenv("FUNDAMENTAL_SERVICE_URL", "http://fundamental-service:8002/marketcap")

# Database path
DB_PATH = "/app/data/analysis_cache.db"


# ---------------------------------------------------------
# Database Setup
# ---------------------------------------------------------
def init_db():
    """Initialize SQLite database for analysis data"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Table for market cap
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_cap_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            market_cap INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            UNIQUE(symbol, timestamp)
        )
    """)
    
    # Table for fused data snapshots
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fused_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            price REAL,
            volume INTEGER,
            market_cap INTEGER,
            timestamp TEXT NOT NULL,
            UNIQUE(symbol, timestamp)
        )
    """)
    
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mcap_symbol ON market_cap_history(symbol, timestamp DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fused_symbol ON fused_snapshots(symbol, timestamp DESC)")
    
    conn.commit()
    conn.close()
    print("✅ Analysis database initialized")


def save_market_cap_to_db(symbol, market_cap, timestamp=None):
    """Save market cap to database"""
    if market_cap is None:
        return
    
    if timestamp is None:
        timestamp = datetime.now(timezone.utc).isoformat()
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT OR REPLACE INTO market_cap_history (symbol, market_cap, timestamp)
            VALUES (?, ?, ?)
        """, (symbol, market_cap, timestamp))
        conn.commit()
    except Exception as e:
        print(f"Error saving market cap: {e}")
    finally:
        conn.close()


def save_fused_snapshot(symbol, price, volume, market_cap, timestamp=None):
    """Save complete fused data snapshot"""
    if timestamp is None:
        timestamp = datetime.now(timezone.utc).isoformat()
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT OR REPLACE INTO fused_snapshots 
            (symbol, price, volume, market_cap, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (symbol, price, volume, market_cap, timestamp))
        conn.commit()
    except Exception as e:
        print(f"Error saving fused snapshot: {e}")
    finally:
        conn.close()


def get_market_cap_history(symbol, limit=100):
    """Get market cap history from database"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT market_cap, timestamp
        FROM market_cap_history
        WHERE symbol = ?
        ORDER BY timestamp DESC
        LIMIT ?
    """, (symbol, limit))
    
    rows = cursor.fetchall()
    conn.close()
    
    return [{"market_cap": row[0], "timestamp": row[1]} for row in reversed(rows)]


def get_fused_history(symbol, limit=100):
    """Get complete fused data history"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT price, volume, market_cap, timestamp
        FROM fused_snapshots
        WHERE symbol = ?
        ORDER BY timestamp DESC
        LIMIT ?
    """, (symbol, limit))
    
    rows = cursor.fetchall()
    conn.close()
    
    return [{
        "price": row[0],
        "volume": row[1],
        "market_cap": row[2],
        "timestamp": row[3]
    } for row in reversed(rows)]


# Initialize database on import
init_db()


# ---------------------------------------------------------
# Fused Data Function (No RSI)
# ---------------------------------------------------------
def get_fused_data(symbol: str):
    """Get fused data - price and market cap only"""
    symbol = symbol.upper()
    price_data = {}
    market_cap = None
    errors = []
    timestamp = datetime.now(timezone.utc).isoformat()

    # 1. Latest price
    try:
        price_resp = requests.get(f"{PRICE_SERVICE_URL}/{symbol}", timeout=5)
        if price_resp.status_code == 200:
            price_data = price_resp.json()
        else:
            errors.append(f"Price service returned: {price_resp.status_code}")
    except Exception as e:
        errors.append(f"Price error: {str(e)}")
        print(f"Price error: {e}")

    # 2. Market Cap
    try:
        fund_resp = requests.get(f"{FUNDAMENTAL_SERVICE_URL}/{symbol}", timeout=5)
        if fund_resp.status_code == 200:
            market_cap = fund_resp.json().get("market_cap")
            # Save to database
            if market_cap:
                save_market_cap_to_db(symbol, market_cap, timestamp)
        else:
            errors.append(f"Fundamental service returned: {fund_resp.status_code}")
    except Exception as e:
        errors.append(f"Fundamental error: {str(e)}")
        print(f"Fundamental error: {e}")

    # Save complete snapshot
    latest_price = None
    if price_data:
        current_day = price_data.get("current_day", {}) or {}
        previous_day = price_data.get("previous_day", {}) or {}
        current_candles = current_day.get("candles", []) or []
        previous_candles = previous_day.get("candles", []) or []
        
        if current_candles:
            latest_price = current_candles[-1]
        elif previous_candles:
            latest_price = previous_candles[-1]

    if latest_price:
        save_fused_snapshot(
            symbol,
            latest_price.get("close"),
            latest_price.get("volume"),
            market_cap,
            timestamp
        )

    return {
        "symbol": symbol,
        "price": latest_price,
        "price_history": price_data.get("current_day", {}).get("candles", []) if price_data else [],
        "price_days": price_data if price_data else None,
        "market_cap": market_cap,
        "errors": errors if errors else None
    }