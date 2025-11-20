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
    
    # Table for RSI values
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rsi_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            rsi_value REAL NOT NULL,
            timestamp TEXT NOT NULL,
            UNIQUE(symbol, timestamp)
        )
    """)
    
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
            rsi REAL,
            market_cap INTEGER,
            timestamp TEXT NOT NULL,
            UNIQUE(symbol, timestamp)
        )
    """)
    
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_rsi_symbol ON rsi_history(symbol, timestamp DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mcap_symbol ON market_cap_history(symbol, timestamp DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fused_symbol ON fused_snapshots(symbol, timestamp DESC)")
    
    conn.commit()
    conn.close()
    print("✅ Analysis database initialized")


def save_rsi_to_db(symbol, rsi_value, timestamp=None):
    """Save RSI value to database"""
    if rsi_value is None:
        return
    
    if timestamp is None:
        timestamp = datetime.now(timezone.utc).isoformat()
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT OR REPLACE INTO rsi_history (symbol, rsi_value, timestamp)
            VALUES (?, ?, ?)
        """, (symbol, rsi_value, timestamp))
        conn.commit()
    except Exception as e:
        print(f"Error saving RSI: {e}")
    finally:
        conn.close()


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


def save_fused_snapshot(symbol, price, volume, rsi, market_cap, timestamp=None):
    """Save complete fused data snapshot"""
    if timestamp is None:
        timestamp = datetime.now(timezone.utc).isoformat()
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT OR REPLACE INTO fused_snapshots 
            (symbol, price, volume, rsi, market_cap, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (symbol, price, volume, rsi, market_cap, timestamp))
        conn.commit()
    except Exception as e:
        print(f"Error saving fused snapshot: {e}")
    finally:
        conn.close()


def get_rsi_history(symbol, limit=100):
    """Get RSI history from database"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT rsi_value, timestamp
        FROM rsi_history
        WHERE symbol = ?
        ORDER BY timestamp DESC
        LIMIT ?
    """, (symbol, limit))
    
    rows = cursor.fetchall()
    conn.close()
    
    return [{"rsi": row[0], "timestamp": row[1]} for row in reversed(rows)]


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
        SELECT price, volume, rsi, market_cap, timestamp
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
        "rsi": row[2],
        "market_cap": row[3],
        "timestamp": row[4]
    } for row in reversed(rows)]


# Initialize database on import
init_db()


# ---------------------------------------------------------
# RSI Calculation
# ---------------------------------------------------------
def compute_rsi(closes, period=14):
    if len(closes) < period + 1:
        return None

    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains = [max(delta, 0) for delta in deltas]
    losses = [max(-delta, 0) for delta in deltas]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    if avg_loss == 0:
        return 100

    RS = avg_gain / avg_loss
    RSI = 100 - (100 / (1 + RS))
    return round(RSI, 2)


# ---------------------------------------------------------
# Fused Data Function
# ---------------------------------------------------------
def get_fused_data(symbol: str):
    symbol = symbol.upper()
    price_data = []
    market_cap = None
    rsi_value = None
    errors = []
    timestamp = datetime.now(timezone.utc).isoformat()

    # 1. Latest price
    try:
        price_resp = requests.get(f"{PRICE_SERVICE_URL}/{symbol}", timeout=5)
        if price_resp.status_code == 200:
            data = price_resp.json().get("data", [])
            price_data = data if isinstance(data, list) else [data]
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

    # 3. RSI Calculation
    try:
        hist_resp = requests.get(f"{PRICE_SERVICE_URL}/{symbol}/history", timeout=5)
        if hist_resp.status_code == 200:
            candles = hist_resp.json().get("data", [])
            closes = [c["close"] for c in candles]
            rsi_value = compute_rsi(closes)
            
            # Save to database
            if rsi_value:
                save_rsi_to_db(symbol, rsi_value, timestamp)
    except Exception as e:
        errors.append(f"RSI error: {str(e)}")
        print(f"RSI error: {e}")

    # Save complete snapshot
    latest_price = price_data[-1] if price_data else None
    if latest_price:
        save_fused_snapshot(
            symbol,
            latest_price.get("close"),
            latest_price.get("volume"),
            rsi_value,
            market_cap,
            timestamp
        )

    return {
        "symbol": symbol,
        "price": latest_price,
        "price_history": price_data,
        "market_cap": market_cap,
        "indicator": {
            "rsi14": rsi_value
        },
        "errors": errors if errors else None
    }