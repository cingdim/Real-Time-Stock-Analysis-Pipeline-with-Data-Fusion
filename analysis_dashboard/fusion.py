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
            closing_price REAL NOT NULL,          -- Closing price for the day
    
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
    
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_rsi_symbol ON rsi_history(symbol, timestamp DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mcap_symbol ON market_cap_history(symbol, timestamp DESC)")
    
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
    price_data = {}
    market_cap = None
    rsi_value = None
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

    # Get latest price for response
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

    return {
        "symbol": symbol,
        "price": latest_price,
        "price_history": price_data.get("current_day", {}).get("candles", []) if price_data else [],
        "price_days": price_data if price_data else None,
        "market_cap": market_cap,
        "indicator": {
            "rsi14": rsi_value
        },
        "errors": errors if errors else None
    }

def save_daily_rsi(symbol: str, trading_date, rsi_value: float):
    """Save one RSI value for the entire trading day"""
    if rsi_value is None:
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_rsi (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            trading_date DATE NOT NULL,
            rsi_value REAL NOT NULL,
            calculated_at TEXT NOT NULL,
            UNIQUE(symbol, trading_date)
        )
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_daily_rsi_lookup 
        ON daily_rsi(symbol, trading_date DESC)
    """)
    
    try:
        cursor.execute("""
            INSERT OR REPLACE INTO daily_rsi 
            (symbol, trading_date, rsi_value, calculated_at)
            VALUES (?, ?, ?, ?)
        """, (
            symbol,
            trading_date if isinstance(trading_date, str) else trading_date.isoformat(),
            rsi_value,
            datetime.now(timezone.utc).isoformat()
        ))
        conn.commit()
        print(f"✅ Saved daily RSI: {symbol} {trading_date} = {rsi_value}")
    except Exception as e:
        print(f"❌ Error saving daily RSI: {e}")
    finally:
        conn.close()

def calculate_and_save_daily_rsi(symbol: str, trading_date):
    """
    Calculate RSI for a full trading day and save it
    
    Args:
        symbol: Stock ticker (e.g., "AAPL")
        trading_date: Date string "YYYY-MM-DD" or date object
    """
    symbol = symbol.upper()
    
    # Convert to string if date object
    date_str = trading_date if isinstance(trading_date, str) else trading_date.isoformat()
    
    print(f"\n📊 Calculating daily RSI for {symbol} on {date_str}...")
    
    try:
        # Get all candles for this specific day from Price Service
        price_resp = requests.get(f"{PRICE_SERVICE_URL}/prices/{symbol}", timeout=10)
        
        if price_resp.status_code != 200:
            print(f"❌ Failed to fetch price data: {price_resp.status_code}")
            return None
        
        data = price_resp.json()
        
        # Check which day's data matches our target date
        current_day = data.get("current_day", {})
        previous_day = data.get("previous_day", {})
        
        candles = []
        if current_day.get("date") == date_str:
            candles = current_day.get("candles", [])
        elif previous_day.get("date") == date_str:
            candles = previous_day.get("candles", [])
        else:
            print(f"⚠️  Date {date_str} not found in available data")
            return None
        
        if len(candles) < 15:
            print(f"⚠️  Not enough candles ({len(candles)}) for RSI calculation")
            return None
        
        # Sort candles chronologically
        candles_sorted = sorted(candles, key=lambda x: x["timestamp_utc"])
        
        # Extract closing prices
        closes = [c["close"] for c in candles_sorted]
        
        # Calculate RSI using all day's data
        rsi_value = compute_rsi(closes, period=14)
        
        if rsi_value is None:
            print(f"❌ RSI calculation returned None")
            return None
        
        # Save to database
        save_daily_rsi(symbol, date_str, rsi_value)
        
        print(f"✅ {symbol} {date_str}: RSI = {rsi_value:.2f} (from {len(candles)} candles)")
        return rsi_value
        
    except Exception as e:
        print(f"❌ Error calculating daily RSI: {e}")
        return None

def get_daily_rsi_history(symbol: str, limit: int = 100):
    """Get historical daily RSI values"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT trading_date, rsi_value, calculated_at
        FROM daily_rsi
        WHERE symbol = ?
        ORDER BY trading_date DESC
        LIMIT ?
    """, (symbol.upper(), limit))
    
    rows = cursor.fetchall()
    conn.close()
    
    # Return in chronological order (oldest first)
    return [
        {
            "date": row[0],
            "rsi": row[1],
            "calculated_at": row[2]
        }
        for row in reversed(rows)
    ]