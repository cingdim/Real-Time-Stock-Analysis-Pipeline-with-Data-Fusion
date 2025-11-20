from fastapi import FastAPI, HTTPException
from datetime import datetime, timedelta, timezone
import threading, time, yfinance as yf, os
from zoneinfo import ZoneInfo
import sqlite3
import json

# ---------------------------------------------------------
# Configuration & Global State
# ---------------------------------------------------------
DEFAULT_TICKERS = os.getenv("TICKERS", "AAPL, AMZN, META, NVDA, TSLA").split(",")
DEFAULT_TICKERS = [t.strip() for t in DEFAULT_TICKERS]
INTERVAL = os.getenv("INTERVAL", "5m")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "60"))
CENTRAL_TZ = ZoneInfo("America/Chicago")

DB_PATH = "/app/data/price_cache.db"
LAST_TIMESTAMP = {}
PRICE_CACHE = {sym: [] for sym in DEFAULT_TICKERS}

app = FastAPI(title="Price Polling Service")


# ---------------------------------------------------------
# Database Setup
# ---------------------------------------------------------
def init_db():
    """Initialize SQLite database"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS price_candles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp_utc TEXT NOT NULL,
            timestamp_local TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume INTEGER NOT NULL,
            interval TEXT NOT NULL,
            source TEXT NOT NULL,
            stale INTEGER DEFAULT 0,
            asof TEXT NOT NULL,
            UNIQUE(symbol, timestamp_utc)
        )
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_symbol_timestamp 
        ON price_candles(symbol, timestamp_utc DESC)
    """)
    
    conn.commit()
    conn.close()
    print("✅ Database initialized")


def save_candle_to_db(candle):
    """Save a candle to database"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT OR REPLACE INTO price_candles 
            (symbol, timestamp_utc, timestamp_local, open, high, low, close, 
             volume, interval, source, stale, asof)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            candle["symbol"],
            candle["timestamp_utc"],
            candle["timestamp_local"],
            candle["open"],
            candle["high"],
            candle["low"],
            candle["close"],
            candle["volume"],
            candle["interval"],
            candle["source"],
            1 if candle.get("stale") else 0,
            candle["asof"]
        ))
        conn.commit()
    except Exception as e:
        print(f"Error saving to DB: {e}")
    finally:
        conn.close()


def load_candles_from_db(symbol, limit=15):
    """Load recent candles from database"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT symbol, timestamp_utc, timestamp_local, open, high, low, close,
               volume, interval, source, stale, asof
        FROM price_candles
        WHERE symbol = ?
        ORDER BY timestamp_utc DESC
        LIMIT ?
    """, (symbol, limit))
    
    rows = cursor.fetchall()
    conn.close()
    
    candles = []
    for row in rows:
        candles.append({
            "symbol": row[0],
            "timestamp_utc": row[1],
            "timestamp_local": row[2],
            "open": row[3],
            "high": row[4],
            "low": row[5],
            "close": row[6],
            "volume": row[7],
            "interval": row[8],
            "source": row[9],
            "stale": bool(row[10]),
            "asof": row[11]
        })
    
    return list(reversed(candles))  # Return in chronological order


# ---------------------------------------------------------
# Market Status Helper
# ---------------------------------------------------------
def get_market_status():
    now_ct = datetime.now(CENTRAL_TZ)
    weekday = now_ct.weekday()

    if weekday >= 5:
        return "closed"

    market_open = now_ct.replace(hour=8, minute=0, second=0, microsecond=0)
    market_close = now_ct.replace(hour=15, minute=30, second=0, microsecond=0)

    if now_ct < market_open or now_ct >= market_close:
        return "closed"

    return "open"


# ---------------------------------------------------------
# Fetch Latest Candle
# ---------------------------------------------------------
# Add this function before the endpoint definitions
def backfill_today(symbol: str):
    """Backfill today's data for a symbol"""
    print(f"🔄 Backfilling today's data for {symbol}...")
    
    try:
        # Fetch today's data with 5-minute intervals
        data = yf.download(
            tickers=symbol,
            period="1d",  # Just today
            interval=INTERVAL,
            progress=False,
            auto_adjust=False
        )
        
        if data.empty:
            print(f"[{symbol}] No data returned")
            return 0
        
        count = 0
        for idx, row in data.iterrows():
            ts_utc = idx.to_pydatetime().replace(tzinfo=timezone.utc)
            ts_ct = ts_utc.astimezone(CENTRAL_TZ)
            
            candle = {
                "symbol": symbol,
                "interval": INTERVAL,
                "timestamp_local": ts_ct.isoformat(),
                "timestamp_utc": ts_utc.isoformat(),
                "asof": datetime.now(timezone.utc).isoformat(),
                "open": round(float(row.Open.item()), 2),
                "high": round(float(row.High.item()), 2),
                "low": round(float(row.Low.item()), 2),
                "close": round(float(row.Close.item()), 2),
                "volume": int(row.Volume.item()),
                "source": "yfinance_backfill",
                "stale": False,
            }
            
            save_candle_to_db(candle)
            count += 1
        
        print(f"✅ [{symbol}] Backfilled {count} candles from today")
        
        # Reload into memory
        PRICE_CACHE[symbol] = load_candles_from_db(symbol, limit=100)
        if PRICE_CACHE[symbol]:
            LAST_TIMESTAMP[symbol] = datetime.fromisoformat(PRICE_CACHE[symbol][-1]["timestamp_utc"])
        
        return count
        
    except Exception as e:
        print(f"[{symbol}] Backfill error: {e}")
        return 0

def fetch_latest_candle(symbol: str):
    """Fetches the latest OHLCV candle from Yahoo Finance."""
    try:
        data = yf.download(
            tickers=symbol,
            period="1d",
            interval=INTERVAL,
            progress=False,
            auto_adjust=False
        )

        if data.empty:
            print(f"[{symbol}] No data returned.")
            return None

        latest = data.iloc[-1]
        ts_utc = data.index[-1].to_pydatetime().replace(tzinfo=timezone.utc)
        ts_ct = ts_utc.astimezone(CENTRAL_TZ)

        return {
            "symbol": symbol,
            "timestamp_utc": ts_utc,
            "timestamp_ct": ts_ct,
            "open": round(float(latest.Open.item()), 2),
            "high": round(float(latest.High.item()), 2),
            "low": round(float(latest.Low.item()), 2),
            "close": round(float(latest.Close.item()), 2),
            "volume": int(latest.Volume.item()),
            "source": "yfinance"
        }

    except Exception as e:
        print(f"[{symbol}] Error fetching data: {e}")
        return None


# ---------------------------------------------------------
# Initial Prefetch at Startup
# ---------------------------------------------------------
def initial_fetch():
    print("📊 Loading data from database...")
    
    for sym in DEFAULT_TICKERS:
        # Load from database first
        candles = load_candles_from_db(sym, limit=15)
        
        if candles:
            PRICE_CACHE[sym] = candles
            LAST_TIMESTAMP[sym] = datetime.fromisoformat(candles[-1]["timestamp_utc"])
            print(f"[{sym}] Loaded {len(candles)} candles from database")
        else:
            # If no data in DB, fetch fresh
            print(f"[{sym}] No cached data, fetching fresh...")
            latest = fetch_latest_candle(sym)
            if latest:
                candle = {
                    "symbol": sym,
                    "interval": INTERVAL,
                    "timestamp_local": latest["timestamp_ct"].isoformat(),
                    "timestamp_utc": latest["timestamp_utc"].isoformat(),
                    "asof": datetime.now(timezone.utc).isoformat(),
                    "open": latest["open"],
                    "high": latest["high"],
                    "low": latest["low"],
                    "close": latest["close"],
                    "volume": latest["volume"],
                    "source": latest["source"],
                    "stale": True,
                }
                PRICE_CACHE[sym] = [candle]
                LAST_TIMESTAMP[sym] = latest["timestamp_utc"]
                save_candle_to_db(candle)
                print(f"[{sym}] Initial candle saved")


# ---------------------------------------------------------
# Polling Loop
# ---------------------------------------------------------
def smart_polling_loop():
    print("🔄 Smart polling thread started...")

    while True:
        now_ct = datetime.now(CENTRAL_TZ)
        weekday = now_ct.weekday()

        if weekday >= 5:
            print(f"Weekend ({now_ct.strftime('%A')}) — sleeping 1h.")
            time.sleep(3600)
            continue

        market_open = now_ct.replace(hour=8, minute=0, second=0)
        market_close = now_ct.replace(hour=15, minute=30, second=0)

        if now_ct < market_open:
            minutes = int((market_open - now_ct).total_seconds() // 60)
            print(f"Market opens in {minutes} minute(s).")
            time.sleep(minutes * 60)
            continue

        if now_ct >= market_close:
            next_open = (now_ct + timedelta(days=1)).replace(hour=8, minute=0)
            while next_open.weekday() >= 5:
                next_open += timedelta(days=1)
            print(f"Market closed. Next open: {next_open}")
            time.sleep(600)
            continue

        print(f"Checking for new candles at {now_ct.strftime('%I:%M:%S %p %Z')}")

        for sym in DEFAULT_TICKERS:
            latest = fetch_latest_candle(sym)
            if not latest:
                continue

            if LAST_TIMESTAMP.get(sym) != latest["timestamp_utc"]:
                LAST_TIMESTAMP[sym] = latest["timestamp_utc"]

                candle = {
                    "symbol": sym,
                    "interval": INTERVAL,
                    "timestamp_local": latest["timestamp_ct"].isoformat(),
                    "timestamp_utc": latest["timestamp_utc"].isoformat(),
                    "asof": datetime.now(timezone.utc).isoformat(),
                    "open": latest["open"],
                    "high": latest["high"],
                    "low": latest["low"],
                    "close": latest["close"],
                    "volume": latest["volume"],
                    "source": latest["source"],
                    "stale": False,
                }
                
                PRICE_CACHE[sym].append(candle)
                PRICE_CACHE[sym] = PRICE_CACHE[sym][-100:]
                save_candle_to_db(candle)
                
                print(f"[{sym}] New candle saved. Total in memory: {len(PRICE_CACHE[sym])}")
            else:
                print(f"[{sym}] No new candle yet.")

        time.sleep(CHECK_INTERVAL)


# ---------------------------------------------------------
# Start Services
# ---------------------------------------------------------
init_db()
initial_fetch()
threading.Thread(target=smart_polling_loop, daemon=True).start()


# ---------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------
@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "tickers": DEFAULT_TICKERS,
        "market_status": get_market_status(),
        "last_check": datetime.now(timezone.utc).isoformat(),
        "db_path": DB_PATH
    }


@app.get("/prices")
def get_all_prices():
    if not PRICE_CACHE:
        raise HTTPException(status_code=503, detail="No data available yet.")

    return {
        "market_status": get_market_status(),
        "asof": datetime.now(timezone.utc).isoformat(),
        "count": len(PRICE_CACHE),
        "data": list(PRICE_CACHE.values()),
    }


@app.get("/prices/{symbol}")
def get_price(symbol: str):
    symbol = symbol.upper()

    if symbol not in PRICE_CACHE:
        raise HTTPException(status_code=404, detail=f"No data for symbol {symbol}")

    return {
        "market_status": get_market_status(),
        "asof": datetime.now(timezone.utc).isoformat(),
        "data": PRICE_CACHE[symbol],
    }

@app.get("/prices/{symbol}/history")
def get_price_history(symbol: str):
    symbol = symbol.upper()

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT timestamp_local, timestamp_utc, open, high, low, close, volume, interval, source, stale
        FROM price_candles
        WHERE symbol = ?
        ORDER BY timestamp_utc ASC
    """, (symbol,))

    rows = cur.fetchall()
    conn.close()

    candles = [
        {
            "timestamp_local": row[0],
            "timestamp_utc": row[1],
            "open": row[2],
            "high": row[3],
            "low": row[4],
            "close": row[5],
            "volume": row[6],
            "interval": row[7],
            "source": row[8],
            "stale": row[9]
        }
        for row in rows
    ]

    return {
        "symbol": symbol,
        "market_status": get_market_status(),
        "count": len(candles),
        "data": candles
    }

@app.post("/backfill/today/{symbol}")
def backfill_today_symbol(symbol: str):
    """Backfill today's data for a symbol"""
    symbol = symbol.upper()
    count = backfill_today(symbol)
    
    return {
        "symbol": symbol,
        "candles_added": count,
        "total_in_cache": len(PRICE_CACHE.get(symbol, [])),
        "message": f"Backfilled {count} candles from today for {symbol}"
    }


@app.post("/backfill/today/all")
def backfill_today_all():
    """Backfill today's data for all tracked symbols"""
    results = {}
    
    for symbol in DEFAULT_TICKERS:
        count = backfill_today(symbol)
        results[symbol] = {
            "candles_added": count,
            "total_in_cache": len(PRICE_CACHE.get(symbol, []))
        }
    
    return {
        "message": f"Backfilled today's data for {len(DEFAULT_TICKERS)} symbols",
        "results": results
    }