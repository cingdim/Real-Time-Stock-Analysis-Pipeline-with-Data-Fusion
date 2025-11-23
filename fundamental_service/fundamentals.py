from contextlib import asynccontextmanager
from fastapi import FastAPI
import asyncio
import yfinance as yf
import datetime as dt

app = FastAPI(title="Fundamental Data Service")

CACHE = {}
TICKERS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]

@app.get("/")
def root():
    return {"message": "Welcome to the Fundamental Data Service! Use /marketcap/{ticker}."}

@app.get("/marketcap/{ticker}")
def get_market_cap(ticker: str):
    ticker = ticker.upper()

    if ticker in CACHE:
        last_update, cap = CACHE[ticker]
        if (dt.datetime.now() - last_update).total_seconds() < 900:
            return {"ticker": ticker, "market_cap": cap, "cached": True}

    try:
        stock = yf.Ticker(ticker)
        cap = stock.info.get("marketCap")
        CACHE[ticker] = (dt.datetime.now(), cap)
        return {"ticker": ticker, "market_cap": cap, "cached": False}
    except Exception as e:
        return {"error": str(e), "ticker": ticker}


async def auto_update_market_caps():
    while True:
        for t in TICKERS:
            stock = yf.Ticker(t)
            cap = stock.info.get("marketCap")
            CACHE[t] = (dt.datetime.now(), cap)
            print(f"Updated {t}: {cap}")
        await asyncio.sleep(900)


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(auto_update_market_caps())
    yield

app.router.lifespan_context = lifespan
