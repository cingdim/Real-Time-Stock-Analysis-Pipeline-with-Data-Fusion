import requests
import os
import sqlite3
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from contextlib import contextmanager

# Service URLs
PRICE_SERVICE_URL = os.getenv("PRICE_SERVICE_URL", "http://price-service:8001/prices")
FUNDAMENTAL_SERVICE_URL = os.getenv("FUNDAMENTAL_SERVICE_URL", "http://fundamental-service:8002/marketcap")
DB_PATH = "/app/data/analysis_cache.db"


# ---------------------------------------------------------
# Database Context Manager
# ---------------------------------------------------------
@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


# ---------------------------------------------------------
# Database Setup
# ---------------------------------------------------------
def init_db():
    """Initialize SQLite database for analysis data"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Market cap history
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS market_cap_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                market_cap INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                UNIQUE(symbol, timestamp)
            )
        """)
        
        # Index
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_mcap_symbol ON market_cap_history(symbol, timestamp DESC)")
    
    print("✅ Analysis database initialized")


# ---------------------------------------------------------
# Database Operations
# ---------------------------------------------------------
def save_market_cap_to_db(symbol: str, market_cap: int, timestamp: Optional[str] = None):
    """Save market cap to database"""
    if market_cap is None:
        return
    
    timestamp = timestamp or datetime.now(timezone.utc).isoformat()
    
    with get_db_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO market_cap_history (symbol, market_cap, timestamp) VALUES (?, ?, ?)",
            (symbol, market_cap, timestamp)
        )


def get_market_cap_history(symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
    """Get market cap history from database"""
    with get_db_connection() as conn:
        cursor = conn.execute(
            "SELECT market_cap, timestamp FROM market_cap_history WHERE symbol = ? ORDER BY timestamp DESC LIMIT ?",
            (symbol, limit)
        )
        rows = cursor.fetchall()
    
    return [{"market_cap": row[0], "timestamp": row[1]} for row in reversed(rows)]


# ---------------------------------------------------------
# HTTP Helper
# ---------------------------------------------------------
def fetch_from_service(url: str, error_context: str) -> Optional[Dict[str, Any]]:
    """Generic HTTP GET with error handling"""
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"{error_context}: HTTP {response.status_code}")
            return None
    except Exception as e:
        print(f"{error_context}: {e}")
        return None


# ---------------------------------------------------------
# Fused Data Function
# ---------------------------------------------------------
def get_fused_data(symbol: str) -> Dict[str, Any]:
    """Get fused data - price and market cap"""
    symbol = symbol.upper()
    timestamp = datetime.now(timezone.utc).isoformat()
    errors = []
    
    # Fetch price data
    price_data = fetch_from_service(
        f"{PRICE_SERVICE_URL}/{symbol}",
        f"Price error for {symbol}"
    )
    if not price_data:
        errors.append("Price service unavailable")
    
    # Fetch market cap
    market_cap = None
    fund_data = fetch_from_service(
        f"{FUNDAMENTAL_SERVICE_URL}/{symbol}",
        f"Fundamental error for {symbol}"
    )
    if fund_data:
        market_cap = fund_data.get("market_cap")
        if market_cap:
            save_market_cap_to_db(symbol, market_cap, timestamp)
    else:
        errors.append("Fundamental service unavailable")
    
    # Extract latest price candle
    latest_price = None
    if price_data:
        for day_key in ["current_day", "previous_day"]:
            candles = (price_data.get(day_key) or {}).get("candles") or []
            if candles:
                latest_price = candles[-1]
                break
    
    return {
        "symbol": symbol,
        "price": latest_price,
        "price_history": (price_data.get("current_day", {}) or {}).get("candles", []) if price_data else [],
        "price_days": price_data,
        "market_cap": market_cap,
        "errors": errors if errors else None
    }


# Initialize database on import
init_db()