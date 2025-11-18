from fastapi import FastAPI, HTTPException
from datetime import datetime, timedelta, timezone
import threading, time, yfinance as yf, os
from zoneinfo import ZoneInfo

# ---------------------------------------------------------
# Configuration & Global State
# ---------------------------------------------------------
DEFAULT_TICKERS = os.getenv("TICKERS", "AAPL, AMZN, META, NVDA, TSLA").split(",")
INTERVAL = os.getenv("INTERVAL", "5m")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "60"))  # Check every 1 minute
CENTRAL_TZ = ZoneInfo("America/Chicago")

LAST_TIMESTAMP = {}   # Tracks latest UTC timestamp per symbol
PRICE_CACHE = {}      # Stores last known candle per symbol

app = FastAPI(title="Price Polling Service")


# ---------------------------------------------------------
# Market Status Helper
# ---------------------------------------------------------
def get_market_status():
    now_ct = datetime.now(CENTRAL_TZ)
    weekday = now_ct.weekday()

    # Weekend (Saturday=5, Sunday=6)
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
def fetch_latest_candle(symbol: str):
    """
    Fetches the latest OHLCV candle from Yahoo Finance.
    """
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
            "open": round(float(latest["Open"]), 2),
            "high": round(float(latest["High"]), 2),
            "low": round(float(latest["Low"]), 2),
            "close": round(float(latest["Close"]), 2),
            "volume": int(latest["Volume"]),
            "source": "yfinance",
        }

    except Exception as e:
        print(f"[{symbol}] Error fetching data: {e}")
        return None


# ---------------------------------------------------------
# Initial Prefetch at Startup (Fixes 503/404 When Closed)
# ---------------------------------------------------------
def initial_fetch():
    print("Performing initial startup fetch...")

    for sym in DEFAULT_TICKERS:
        latest = fetch_latest_candle(sym)
        if not latest:
            print(f"[{sym}] Initial fetch failed.")
            continue

        PRICE_CACHE[sym] = {
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
            "stale": True,  # Startup data may be old
        }

        LAST_TIMESTAMP[sym] = latest["timestamp_utc"]
        print(f"[{sym}] Initial candle loaded.")


# ---------------------------------------------------------
# Polling Loop (Only Polls During Market Hours)
# ---------------------------------------------------------
def smart_polling_loop():
    print("Smart polling thread started...")

    while True:
        now_ct = datetime.now(CENTRAL_TZ)
        weekday = now_ct.weekday()

        # Weekend
        if weekday >= 5:
            print(f"Weekend ({now_ct.strftime('%A')}) — sleeping 1h.")
            time.sleep(3600)
            continue

        market_open = now_ct.replace(hour=8, minute=0, second=0)
        market_close = now_ct.replace(hour=15, minute=30, second=0)

        # Before open
        if now_ct < market_open:
            minutes = int((market_open - now_ct).total_seconds() // 60)
            print(f"Market opens in {minutes} minute(s).")
            time.sleep(minutes * 60)
            continue

        # After close
        if now_ct >= market_close:
            next_open = (now_ct + timedelta(days=1)).replace(hour=8, minute=0)
            while next_open.weekday() >= 5:
                next_open += timedelta(days=1)
            print(f"Market closed. Next open: {next_open}")
            time.sleep(600)
            continue

        # Market open → Poll candles
        print(f"Checking for new candles at {now_ct.strftime('%I:%M:%S %p %Z')}")

        for sym in DEFAULT_TICKERS:
            latest = fetch_latest_candle(sym)
            if not latest:
                continue

            # Only update if new candle
            if LAST_TIMESTAMP.get(sym) != latest["timestamp_utc"]:
                LAST_TIMESTAMP[sym] = latest["timestamp_utc"]

                PRICE_CACHE[sym] = {
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

                print(f"[{sym}] Updated candle.")
            else:
                print(f"[{sym}] No new candle yet.")

        time.sleep(CHECK_INTERVAL)


# ---------------------------------------------------------
# Start Services (Prefetch + Poll Thread)
# ---------------------------------------------------------
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
