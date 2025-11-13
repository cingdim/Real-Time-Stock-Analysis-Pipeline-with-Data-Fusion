#create a real time prices
#service, a Fundamental Data Service, and an Analysis and Visualization Service.
#2. The Price Polling Service must automatically fetch (generate/move) time-series price data (e.g., 5-
#minute intervals) for at least three different stock tickers from a public financial API (e.g., AlphaVantage, Finnhub, or Yahoo Finance).

from fastapi import FastAPI, HTTPException
from datetime import datetime, timedelta, timezone
import threading, time, yfinance as yf, os
from zoneinfo import ZoneInfo
DEFAULT_TICKERS = os.getenv("TICKERS", "AAPL, AMZN, META, NVDA, TSLA").split(",")
INTERVAL = os.getenv("INTERVAL", "5m")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "60"))  # check every 1 minute
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "300"))
LAST_TIMESTAMP={}   #LASTEST CANDLE TIME STAMP
PRICE_CACHE = {}
CENTRAL_TZ = ZoneInfo("America/Chicago")
app = FastAPI(title="Price Polling Service")

def fetch_latest_candle(symbol):
    """
    Fetch 5 minute data for OHLCV data for a list of tickers
    Updates the global price_cache dictionary
    """
    try:
        data = yf.download(
            tickers=symbol,
            period="1d",
            interval=INTERVAL,
            progress=False,
            auto_adjust = False)
        
        if data.empty:
            print(f"[{symbol}] No data returned")
            return None
        
        #getting the newest data, (the last row from the dataframe)
        latest = data.iloc[-1]
        ts = data.index[-1].to_pydatetime().replace(tzinfo=timezone.utc)
        ts_ct = ts.astimezone(CENTRAL_TZ)

        return {
            "symbol": symbol,
            "timestamp_utc": ts, 
            "timestamp_ct": ts_ct,
            "open": round(float(latest["Open"].iloc[0]), 2),
            "high": round(float(latest["High"].iloc[0]), 2),
            "low": round(float(latest["Low"].iloc[0]), 2),
            "close": round(float(latest["Close"].iloc[0]), 2),
            "volume": int(latest["Volume"].iloc[0]),
            "source": "yfinance",
        }
    except Exception as e:
        print(f"[{symbol}] Error fetching data: {e}")
        return None


def smart_polling_loop():
    """Continuously checks for new 5-min candles during market hours (8:00 AM–3:30 PM CT)."""

    print("Smart polling thread started...")
    while True:
        now_ct = datetime.now(CENTRAL_TZ)
        # hour = now_ct.hour
        # minute = now_ct.minute
        weekday = now_ct.weekday()  # Monday=0, Sunday=6

        # Skip weekends
        if weekday >= 5:  # Saturday or Sunday
            print(f"Weekend detected ({now_ct.strftime('%A')}) — sleeping 1 hour.")
            time.sleep(3600)
            continue

        # Define market open/close times
        market_open_time = now_ct.replace(hour=8, minute=0, second=0, microsecond=0)
        market_close_time = now_ct.replace(hour=15, minute=30, second=0, microsecond=0)

        if now_ct < market_open_time:
            # Market not open yet
            #minute until open is calculated in seconds. 
            mins_until_open = int((market_open_time - now_ct).total_seconds() // 60)
            print(
                f"Market opens in {mins_until_open} minute(s) "
                f"({market_open_time.strftime('%I:%M %p %Z')})"
            )
            #sleep until it is time to open.
            time.sleep(mins_until_open * 60)
            continue

        if now_ct >= market_close_time:
            # Market closed for the day
            next_open = (now_ct + timedelta(days=1)).replace(hour=8, minute=0, second=0, microsecond=0)
            # Skip weekends automatically
            while next_open.weekday() >= 5:
                next_open += timedelta(days=1)
            mins_until_next_open = int((next_open - now_ct).total_seconds() // 60)
            print(
                f"Market closed — will resume at {next_open.strftime('%A %I:%M %p %Z')} "
                f"({mins_until_next_open} minutes from now)"
            )
            time.sleep(600)  # sleep 10 min chunks overnight
            continue

        # Normal polling during market hours
        print(f"\nChecking for new candles at {now_ct.strftime('%Y-%m-%d %I:%M:%S %p %Z')}")
        for sym in DEFAULT_TICKERS:
            latest = fetch_latest_candle(sym)
            if not latest:
                continue

            last_ts = LAST_TIMESTAMP.get(sym)
            if last_ts is None or latest["timestamp_utc"] != last_ts:
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
                print(f"[{sym}] New 5m candle at {latest['timestamp_ct']} updated.")
            else:
                print(f"[{sym}] No new candle yet (last at {last_ts}).")

        print(f"Sleeping {CHECK_INTERVAL} seconds before next check...\n")
        time.sleep(CHECK_INTERVAL)

threading.Thread(target=smart_polling_loop,daemon=True).start()

@app.get("/health")
def health_check():
    return{
        "status": "ok",
        "tickers": DEFAULT_TICKERS,
        "last_check": datetime.now(timezone.utc),
    }

@app.get("/prices")
def get_all_prices():
    """Return the latest data for all tracked tickers."""
    if not PRICE_CACHE:
        raise HTTPException(status_code=503, detail="close: No data available yet.")
    return {
        "asof": datetime.now(timezone.utc).isoformat(),
        "count": len(PRICE_CACHE),
        "data": list(PRICE_CACHE.values()),
    }
@app.get("/prices/{symbol}")
def get_price(symbol: str):
    """Return the most recent data for one ticker."""
    symbol = symbol.upper()
    if symbol not in PRICE_CACHE:
        raise HTTPException(status_code=404, detail=f"No data for symbol {symbol}")
    return PRICE_CACHE[symbol]




