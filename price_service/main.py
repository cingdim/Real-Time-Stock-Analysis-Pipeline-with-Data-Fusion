from fastapi import FastAPI, HTTPException
from datetime import datetime, timedelta, timezone, date as dt_date
import threading
import time
import yfinance as yf
import os
from zoneinfo import ZoneInfo
import sqlite3

# ---------------------------------------------------------
# Configuration & Global State
# ---------------------------------------------------------
DEFAULT_TICKERS = os.getenv("TICKERS", "AAPL, AMZN, META, NVDA, TSLA").split(",")
DEFAULT_TICKERS = [t.strip() for t in DEFAULT_TICKERS]
INTERVAL = os.getenv("INTERVAL", "5m")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "60"))
CENTRAL_TZ = ZoneInfo("America/Chicago")

DB_PATH = "data/price_cache.db"
LAST_TIMESTAMP = {}
PRICE_CACHE = {}

app = FastAPI(title="Price Polling Service")


# ---------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------
def get_ct_today() -> dt_date:
    return datetime.now(CENTRAL_TZ).date()


def get_previous_trading_day(start_date: dt_date) -> dt_date:
    previous = start_date - timedelta(days=1)
    while previous.weekday() >= 5:
        previous -= timedelta(days=1)
    return previous


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
    print("Database initialized")


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


def load_candles_for_day(symbol: str, target_day: dt_date):
    """Load all candles for a specific calendar day (Central Time)."""
    start_local = datetime.combine(target_day, datetime.min.time(), tzinfo=CENTRAL_TZ)
    end_local = start_local + timedelta(days=1)
    
    start_utc = start_local.astimezone(timezone.utc).isoformat()
    end_utc = end_local.astimezone(timezone.utc).isoformat()
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT symbol, timestamp_utc, timestamp_local, open, high, low, close,
               volume, interval, source, stale, asof
        FROM price_candles
        WHERE symbol = ?
          AND timestamp_utc >= ?
          AND timestamp_utc < ?
        ORDER BY timestamp_utc ASC
    """, (symbol, start_utc, end_utc))
    
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
    return candles


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
# Backfill & Fetch Functions
# ---------------------------------------------------------
def backfill_day(symbol: str, target_day: dt_date):
    """Backfill all 5m candles for the given day (Central Time)."""
    print(f"Backfilling {symbol} for {target_day.isoformat()}...")
    
    try:
        start = target_day.strftime("%Y-%m-%d")
        end = (target_day + timedelta(days=1)).strftime("%Y-%m-%d")
        
        data = yf.download(
            tickers=symbol,
            start=start,
            end=end,
            interval=INTERVAL,
            progress=False,
            auto_adjust=False
        )
        
        if data.empty:
            print(f"[{symbol}] No data returned for {target_day}")
            return 0
        
        count = 0
        for idx, row in data.iterrows():
            ts_utc = idx.to_pydatetime()
            if ts_utc.tzinfo is None:
                ts_utc = ts_utc.replace(tzinfo=timezone.utc)
            else:
                ts_utc = ts_utc.astimezone(timezone.utc)
            ts_ct = ts_utc.astimezone(CENTRAL_TZ)
            
            # Only include candles that fall on the target day in Central Time
            if ts_ct.date() != target_day:
                continue
            
            o = float(row["Open"])
            h = float(row["High"])
            l = float(row["Low"])
            c = float(row["Close"])
            v = int(row["Volume"])
            
            # Skip placeholder candles
            if v == 0 and o == h == l == c:
                continue
            
            candle = {
                "symbol": symbol,
                "interval": INTERVAL,
                "timestamp_local": ts_ct.isoformat(),
                "timestamp_utc": ts_utc.isoformat(),
                "asof": datetime.now(timezone.utc).isoformat(),
                "open": round(o, 2),
                "high": round(h, 2),
                "low": round(l, 2),
                "close": round(c, 2),
                "volume": v,
                "source": "yfinance_backfill",
                "stale": False,
            }
            
            save_candle_to_db(candle)
            count += 1
        
        print(f"[{symbol}] Backfilled {count} candles for {target_day}")
        return count
        
    except Exception as e:
        print(f"[{symbol}] Backfill error for {target_day}: {e}")
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

        o = float(latest["Open"])
        h = float(latest["High"])
        l = float(latest["Low"])
        c = float(latest["Close"])
        v = int(latest["Volume"])

        # Ignore placeholder candles
        if v == 0 and o == h == l == c:
            print(f"[{symbol}] Ignored placeholder candle.")
            return None

        return {
            "symbol": symbol,
            "timestamp_utc": ts_utc,
            "timestamp_ct": ts_ct,
            "open": round(o, 2),
            "high": round(h, 2),
            "low": round(l, 2),
            "close": round(c, 2),
            "volume": v,
            "source": "yfinance",
        }

    except Exception as e:
        print(f"[{symbol}] Error fetching data: {e}")
        return None


# ---------------------------------------------------------
# Initial Prefetch at Startup
# ---------------------------------------------------------
def is_data_complete_for_today(symbol: str, today: dt_date):
    """Check if we have all expected candles for today based on current time"""
    market_status = get_market_status()
    
    if market_status == "closed":
        # Market is closed, we should have all candles for the day if market was open
        now_ct = datetime.now(CENTRAL_TZ)
        if now_ct.weekday() < 5:  # Weekday
            # Should have full day of candles (78 candles for 5m intervals: 8am-3:30pm = 7.5 hours = 90 candles)
            candles = load_candles_for_day(symbol, today)
            return len(candles) >= 75  # Allow some tolerance
        return True  # Weekend, don't need to check
    
    # Market is open - check if we have candles up to the last completed interval
    now_ct = datetime.now(CENTRAL_TZ)
    market_open = now_ct.replace(hour=8, minute=0, second=0, microsecond=0)
    
    if now_ct < market_open:
        return True  # Before market open, no data needed yet
    
    # Calculate expected number of 5-minute candles since market open
    minutes_since_open = (now_ct - market_open).total_seconds() / 60
    expected_candles = max(0, int(minutes_since_open / 5))
    
    candles = load_candles_for_day(symbol, today)
    actual_candles = len(candles)
    
    # Allow some tolerance (we might be waiting for the current 5m bar to close)
    tolerance = 2
    is_complete = actual_candles >= (expected_candles - tolerance)
    
    if not is_complete:
        print(f"[{symbol}] Data incomplete: have {actual_candles}, expected ~{expected_candles}")
    
    return is_complete


def initial_fetch():
    """Load today's and previous day's data for all symbols"""
    print("Initializing price data...")
    
    today = get_ct_today()
    prev_day = get_previous_trading_day(today)
    
    for sym in DEFAULT_TICKERS:
        print(f"\n[{sym}] Checking data availability...")
        
        # Load today's candles
        todays_candles = load_candles_for_day(sym, today)
        
        # Check if today's data is complete
        if not is_data_complete_for_today(sym, today):
            print(f"[{sym}] Today's data incomplete or missing. Backfilling...")
            backfill_day(sym, today)
            todays_candles = load_candles_for_day(sym, today)
        
        # Load previous day's candles
        prev_candles = load_candles_for_day(sym, prev_day)
        if not prev_candles:
            print(f"[{sym}] No data for previous trading day ({prev_day}). Backfilling...")
            backfill_day(sym, prev_day)
            prev_candles = load_candles_for_day(sym, prev_day)
        
        # Initialize cache
        PRICE_CACHE[sym] = {
            "today": todays_candles,
            "previous": prev_candles
        }
        
        # Set last timestamp from today's data
        if todays_candles:
            LAST_TIMESTAMP[sym] = datetime.fromisoformat(todays_candles[-1]["timestamp_utc"])
            print(f"[{sym}] Loaded {len(todays_candles)} candles for today, {len(prev_candles)} for previous day")
        else:
            print(f"[{sym}] No data available for today yet")


# ---------------------------------------------------------
# Polling Loop
# ---------------------------------------------------------
def smart_polling_loop():
    """Continuously check for new 5-minute candles during market hours"""
    print("Smart polling thread started...")

    while True:
        now_ct = datetime.now(CENTRAL_TZ)
        weekday = now_ct.weekday()

        # Weekend handling
        if weekday >= 5:
            print(f"Weekend ({now_ct.strftime('%A')}) — sleeping 1h.")
            time.sleep(3600)
            continue

        market_open = now_ct.replace(hour=8, minute=0, second=0)
        market_close = now_ct.replace(hour=15, minute=30, second=0)

        # Pre-market handling
        if now_ct < market_open:
            minutes = int((market_open - now_ct).total_seconds() // 60)
            print(f"Market opens in {minutes} minute(s). Sleeping...")
            time.sleep(min(minutes * 60, 600))
            continue

        # After-hours handling
        if now_ct >= market_close:
            next_open = (now_ct + timedelta(days=1)).replace(hour=8, minute=0)
            while next_open.weekday() >= 5:
                next_open += timedelta(days=1)
            print(f"Market closed. Next open: {next_open.strftime('%Y-%m-%d %H:%M %Z')}")
            time.sleep(600)
            continue

        # During market hours - check for new candles
        print(f"\n🔍 Checking for new candles at {now_ct.strftime('%I:%M:%S %p %Z')}")

        for sym in DEFAULT_TICKERS:
            latest = fetch_latest_candle(sym)
            if not latest:
                print(f"[{sym}] No new data available")
                continue

            # Check if this is a new candle
            last_ts = LAST_TIMESTAMP.get(sym)
            if last_ts != latest["timestamp_utc"]:
                LAST_TIMESTAMP[sym] = latest["timestamp_utc"]

                # Create candle record
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
                
                # Save to database
                save_candle_to_db(candle)
                
                # Update in-memory cache
                if sym in PRICE_CACHE:
                    PRICE_CACHE[sym]["today"].append(candle)
                    print(f"✅ [{sym}] New candle saved | Today's total: {len(PRICE_CACHE[sym]['today'])}")
                else:
                    PRICE_CACHE[sym] = {"today": [candle], "previous": []}
                    print(f"✅ [{sym}] First candle of the day saved")
            else:
                print(f"[{sym}] No new candle yet (last: {last_ts.strftime('%I:%M %p') if last_ts else 'N/A'})")

        # Wait before next check
        time.sleep(CHECK_INTERVAL)


# ---------------------------------------------------------
# Cleanup Old Data
# ---------------------------------------------------------
def cleanup_old_data():
    """Remove data older than previous trading day at midnight"""
    print("Cleanup thread started...")
    
    while True:
        now_ct = datetime.now(CENTRAL_TZ)
        
        # Calculate next midnight
        next_midnight = (now_ct + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        sleep_seconds = (next_midnight - now_ct).total_seconds()
        
        print(f"Next cleanup at {next_midnight.strftime('%Y-%m-%d %I:%M %p %Z')}")
        time.sleep(sleep_seconds)
        
        # It's now past midnight - cleanup old data
        today = get_ct_today()
        previous = get_previous_trading_day(today)
        
        # Keep only today and previous trading day
        cutoff_date = previous
        cutoff_datetime = datetime.combine(cutoff_date, datetime.min.time(), tzinfo=CENTRAL_TZ)
        cutoff_utc = cutoff_datetime.astimezone(timezone.utc).isoformat()
        
        print(f"\nCleaning up data older than {cutoff_date.isoformat()}...")
        
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Count rows to be deleted
        cursor.execute("""
            SELECT COUNT(*) FROM price_candles
            WHERE timestamp_utc < ?
        """, (cutoff_utc,))
        count = cursor.fetchone()[0]
        
        if count > 0:
            # Delete old data
            cursor.execute("""
                DELETE FROM price_candles
                WHERE timestamp_utc < ?
            """, (cutoff_utc,))
            conn.commit()
            print(f"Deleted {count} old candle records")
        else:
            print("No old data to clean up")
        
        conn.close()
        
        # Refresh cache with current data
        print("Refreshing cache after cleanup...")
        for sym in DEFAULT_TICKERS:
            today_candles = load_candles_for_day(sym, today)
            prev_candles = load_candles_for_day(sym, previous)
            PRICE_CACHE[sym] = {
                "today": today_candles,
                "previous": prev_candles
            }
            print(f"[{sym}] Cache refreshed: {len(today_candles)} today, {len(prev_candles)} previous")


# ---------------------------------------------------------
# Start Services
# ---------------------------------------------------------
init_db()
initial_fetch()
threading.Thread(target=smart_polling_loop, daemon=True).start()
threading.Thread(target=cleanup_old_data, daemon=True).start()


# ---------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------
@app.get("/health")
def health_check():
    """Health check with data freshness info"""
    today = get_ct_today()
    
    # Count total records in database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM price_candles")
    total_records = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT MIN(timestamp_utc), MAX(timestamp_utc) 
        FROM price_candles
    """)
    date_range = cursor.fetchone()
    conn.close()
    
    freshness = {}
    for sym in DEFAULT_TICKERS:
        if sym in PRICE_CACHE and PRICE_CACHE[sym]["today"]:
            latest = PRICE_CACHE[sym]["today"][-1]
            latest_ts = datetime.fromisoformat(latest["timestamp_utc"])
            age_minutes = (datetime.now(timezone.utc) - latest_ts).total_seconds() / 60
            
            freshness[sym] = {
                "candles_today": len(PRICE_CACHE[sym]["today"]),
                "candles_previous": len(PRICE_CACHE[sym]["previous"]),
                "latest_timestamp": latest["timestamp_local"],
                "age_minutes": round(age_minutes, 1)
            }
    
    return {
        "status": "ok",
        "tickers": DEFAULT_TICKERS,
        "market_status": get_market_status(),
        "current_date": today.isoformat(),
        "previous_trading_day": get_previous_trading_day(today).isoformat(),
        "last_check": datetime.now(timezone.utc).isoformat(),
        "db_path": DB_PATH,
        "db_stats": {
            "total_records": total_records,
            "oldest_record": date_range[0] if date_range[0] else None,
            "newest_record": date_range[1] if date_range[1] else None
        },
        "data_freshness": freshness
    }


@app.get("/prices")
def get_all_prices():
    """Get today's and previous day's data for all symbols"""
    today = get_ct_today()
    previous = get_previous_trading_day(today)
    
    data = []
    for sym in DEFAULT_TICKERS:
        # Reload from DB to ensure freshness
        today_candles = load_candles_for_day(sym, today)
        prev_candles = load_candles_for_day(sym, previous)
        
        payload = {
            "symbol": sym,
            "current_day": {
                "date": today.isoformat(),
                "candle_count": len(today_candles),
                "candles": today_candles
            },
            "previous_day": {
                "date": previous.isoformat(),
                "candle_count": len(prev_candles),
                "candles": prev_candles
            }
        }
        data.append(payload)

    return {
        "market_status": get_market_status(),
        "asof": datetime.now(timezone.utc).isoformat(),
        "count": len(data),
        "data": data,
    }


@app.get("/prices/{symbol}")
def get_price(symbol: str):
    """Get today's and previous day's data for a specific symbol"""
    symbol = symbol.upper()
    
    today = get_ct_today()
    previous = get_previous_trading_day(today)
    
    # Load fresh data from database
    today_candles = load_candles_for_day(symbol, today)
    prev_candles = load_candles_for_day(symbol, previous)
    
    if not today_candles and not prev_candles:
        raise HTTPException(status_code=404, detail=f"No data available for symbol {symbol}")

    return {
        "symbol": symbol,
        "market_status": get_market_status(),
        "asof": datetime.now(timezone.utc).isoformat(),
        "current_day": {
            "date": today.isoformat(),
            "candle_count": len(today_candles),
            "candles": today_candles
        },
        "previous_day": {
            "date": previous.isoformat(),
            "candle_count": len(prev_candles),
            "candles": prev_candles
        }
    }


@app.get("/prices/{symbol}/history")
def get_price_history(symbol: str):
    """Get complete historical data for a symbol"""
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
            "stale": bool(row[9])
        }
        for row in rows
    ]

    return {
        "symbol": symbol,
        "market_status": get_market_status(),
        "count": len(candles),
        "data": candles
    }


@app.post("/backfill/{symbol}/{date}")
def backfill_specific_day(symbol: str, date: str):
    """Backfill data for a specific date (YYYY-MM-DD format)"""
    symbol = symbol.upper()
    
    try:
        target_day = dt_date.fromisoformat(date)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    
    count = backfill_day(symbol, target_day)
    
    return {
        "symbol": symbol,
        "date": date,
        "candles_added": count,
        "message": f"Backfilled {count} candles for {symbol} on {date}"
    }


@app.post("/backfill/today/{symbol}")
def backfill_today_symbol(symbol: str):
    """Backfill today's data for a symbol"""
    symbol = symbol.upper()
    today = get_ct_today()
    count = backfill_day(symbol, today)
    
    # Reload cache
    PRICE_CACHE[symbol]["today"] = load_candles_for_day(symbol, today)
    
    return {
        "symbol": symbol,
        "date": today.isoformat(),
        "candles_added": count,
        "total_today": len(PRICE_CACHE[symbol]["today"]),
        "message": f"Backfilled {count} candles for {symbol} today"
    }


@app.post("/backfill/today/all")
def backfill_today_all():
    """Backfill today's data for all tracked symbols"""
    today = get_ct_today()
    results = {}
    
    for symbol in DEFAULT_TICKERS:
        count = backfill_day(symbol, today)
        if symbol in PRICE_CACHE:
            PRICE_CACHE[symbol]["today"] = load_candles_for_day(symbol, today)
        results[symbol] = {
            "candles_added": count,
            "total_today": len(PRICE_CACHE.get(symbol, {}).get("today", []))
        }
    
    return {
        "date": today.isoformat(),
        "message": f"Backfilled today's data for {len(DEFAULT_TICKERS)} symbols",
        "results": results
    }