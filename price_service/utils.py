from fastapi import FastAPI, HTTPException
from datetime import datetime, timedelta, timezone, date as dt_date
import threading, time, yfinance as yf, os
from zoneinfo import ZoneInfo

# ---------------------------------------------------------
# Configuration & Global State
# ---------------------------------------------------------
DEFAULT_TICKERS = os.getenv("TICKERS", "AAPL,MSFT, TSLA, GOOGL, AMZN").split(",")
INTERVAL = os.getenv("INTERVAL", "5m")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "60"))  # Poll every 1 min
CENTRAL_TZ = ZoneInfo("America/Chicago")

PRICE_HISTORY = {}   # Stores previous_day + current_day candles
LAST_TIMESTAMP = {}  # Tracks latest candle timestamp

app = FastAPI(title="Price Polling Service")

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
# Fetch Latest Candle (5m)
# ---------------------------------------------------------
def fetch_latest_candle(symbol: str):
    try:
        data = yf.download(
            tickers=symbol,
            period="1d",
            interval=INTERVAL,
            progress=False,
            auto_adjust=False
        )

        if data.empty:
            return None

        latest = data.iloc[-1]
        ts_utc = data.index[-1].to_pydatetime().replace(tzinfo=timezone.utc)
        ts_ct = ts_utc.astimezone(CENTRAL_TZ)

        return {
            "timestamp_utc": ts_utc.isoformat(),
            "timestamp_local": ts_ct.isoformat(),
            "open": round(float(latest["Open"]), 2),
            "high": round(float(latest["High"]), 2),
            "low": round(float(latest["Low"]), 2),
            "close": round(float(latest["Close"]), 2),
            "volume": int(latest["Volume"])
        }

    except Exception as e:
        print(f"[{symbol}] Error: {e}")
        return None


# ---------------------------------------------------------
# Initialize Previous Day + Current Day at Startup
# ---------------------------------------------------------
def initial_fetch():
    print("Performing startup initialization...")
    now_ct = datetime.now(CENTRAL_TZ)
    today_str = now_ct.date().isoformat()

    for sym in DEFAULT_TICKERS:
        # Fetch 2 days of 5-min candles
        data = yf.download(sym, period="2d", interval=INTERVAL)
        if data.empty:
            continue

        prev_day_candles = []
        curr_day_candles = []

        for ts, row in data.iterrows():
            ts_utc = ts.to_pydatetime().replace(tzinfo=timezone.utc)
            ts_local = ts_utc.astimezone(CENTRAL_TZ)
            candle = {
                "timestamp_utc": ts_utc.isoformat(),
                "timestamp_local": ts_local.isoformat(),
                "open": round(float(row["Open"]), 2),
                "high": round(float(row["High"]), 2),
                "low": round(float(row["Low"]), 2),
                "close": round(float(row["Close"]), 2),
                "volume": int(row["Volume"])
            }

            # Assign to previous_day or current_day based on local date
            if ts_local.date() == now_ct.date():
                curr_day_candles.append(candle)
            else:
                prev_day_candles.append(candle)

        prev_day_date = prev_day_candles[0]["timestamp_local"][:10] if prev_day_candles else (now_ct.date() - timedelta(days=1)).isoformat()
        PRICE_HISTORY[sym] = {
            "symbol": sym,
            "interval": INTERVAL,
            "previous_day": {
                "date": prev_day_date,
                "candles": prev_day_candles
            },
            "current_day": {
                "date": today_str,
                "candles": curr_day_candles
            }
        }

        if curr_day_candles:
            LAST_TIMESTAMP[sym] = curr_day_candles[-1]["timestamp_utc"]
        elif prev_day_candles:
            LAST_TIMESTAMP[sym] = prev_day_candles[-1]["timestamp_utc"]

        print(f"[{sym}] Loaded initial historical data.")
# ------------------------------------------
# Smart Polling Loop – Appends Candles into Current Day
# ---------------------------------------------------------
def smart_polling_loop():
    print("Polling loop started.")

    while True:
        now_ct = datetime.now(CENTRAL_TZ)  # Local time
        midnight_ct = now_ct.replace(hour=0, minute=0, second=0, microsecond=0)

        for sym in DEFAULT_TICKERS:
            latest = fetch_latest_candle(sym)
            if not latest:
                continue

            last_ts = LAST_TIMESTAMP.get(sym)
            if last_ts == latest["timestamp_utc"]:
                continue

            LAST_TIMESTAMP[sym] = latest["timestamp_utc"]

            current_day = PRICE_HISTORY[sym]["current_day"]

            # Rotate only after local midnight
            if now_ct >= midnight_ct and current_day["date"] != now_ct.date().isoformat():
                PRICE_HISTORY[sym]["previous_day"] = current_day
                PRICE_HISTORY[sym]["current_day"] = {
                    "date": now_ct.date().isoformat(),
                    "candles": []
                }
                print(f"[{sym}] Rotated current_day to previous_day at local midnight.")

            # Append new candle
            PRICE_HISTORY[sym]["current_day"]["candles"].append(latest)
            print(f"[{sym}] Added new 5m candle.")

        time.sleep(CHECK_INTERVAL)


# ---------------------------------------------------------
# Startup
# ---------------------------------------------------------
initial_fetch()
threading.Thread(target=smart_polling_loop, daemon=True).start()


# ---------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------
@app.get("/prices")
def get_all_prices():
    return {
        "market_status": get_market_status(),
        "asof": datetime.now(timezone.utc).isoformat(),
        "count": len(PRICE_HISTORY),
        "data": PRICE_HISTORY
    }


@app.get("/prices/{symbol}")
def get_price(symbol: str):
    symbol = symbol.upper()
    if symbol not in PRICE_HISTORY:
        raise HTTPException(404, f"No data for {symbol}")

    return {
        "market_status": get_market_status(),
        "asof": datetime.now(timezone.utc).isoformat(),
        "data": PRICE_HISTORY[symbol]
    }


