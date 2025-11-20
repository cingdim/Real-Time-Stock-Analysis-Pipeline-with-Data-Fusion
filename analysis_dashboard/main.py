from fastapi import FastAPI
from fusion import get_fused_data, get_rsi_history, get_market_cap_history, get_fused_history

app = FastAPI(title="Analysis & Fusion Service")

@app.get("/")
def root():
    return {"message": "Analysis Service - use /fused/{symbol}"}

@app.get("/fused/{symbol}")
def fused(symbol: str):
    return get_fused_data(symbol)

@app.get("/rsi/history/{symbol}")
def rsi_history(symbol: str, limit: int = 100):
    """Get RSI history for a symbol"""
    return {
        "symbol": symbol.upper(),
        "history": get_rsi_history(symbol.upper(), limit)
    }

@app.get("/marketcap/history/{symbol}")
def marketcap_history(symbol: str, limit: int = 100):
    """Get market cap history for a symbol"""
    return {
        "symbol": symbol.upper(),
        "history": get_market_cap_history(symbol.upper(), limit)
    }

@app.get("/history/{symbol}")
def complete_history(symbol: str, limit: int = 100):
    """Get complete fused data history"""
    return {
        "symbol": symbol.upper(),
        "history": get_fused_history(symbol.upper(), limit)
    }

@app.get("/health")
def health():
    return {"status": "ok"}