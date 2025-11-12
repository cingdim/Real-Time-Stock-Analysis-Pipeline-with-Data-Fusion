import streamlit as st
import requests

st.title("📊 Stock Market Cap Dashboard")

tickers = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]
selected = st.selectbox("Choose a stock ticker:", tickers)

BACKEND_URL = "http://fundamental-service:8002"

try:
    res = requests.get(f"{BACKEND_URL}/marketcap/{selected}")
    data = res.json()
    st.metric("Market Cap", f"${data['market_cap']:,}")
except Exception as e:
    st.error(f"Failed to fetch data: {e}")