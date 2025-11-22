import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
import os


# Configuration - Read from environment variables
ANALYSIS_SERVICE_URL = os.getenv("ANALYSIS_SERVICE_URL", "http://analysis-service:8003")
PRICE_SERVICE_URL = os.getenv("PRICE_SERVICE_URL", "http://price-service:8001")

st.set_page_config(
    page_title="Stock Market Analytics Dashboard",
    page_icon="📈",
    layout="wide"
)

# Title
st.title("Real-Time Stock Market Analytics Dashboard")
st.markdown("---")

# Sidebar for stock selection
st.sidebar.header("Stock Selection")
symbols = ["AAPL", "AMZN", "META", "NVDA", "TSLA"]
selected_symbol = st.sidebar.selectbox("Select Stock Symbol", symbols)

# SMA configuration
st.sidebar.header("Technical Indicators")
sma_window = st.sidebar.slider(
    "SMA Window (candles)",
    min_value=5,
    max_value=60,
    value=14,
    help="Number of recent candles used when computing the simple moving average."
)

# Historical data range for correlation
correlation_limit = st.sidebar.slider(
    "Correlation Data Points",
    min_value=20,
    max_value=200,
    value=100,
    help="Number of market cap snapshots to use for correlation analysis"
)

# Refresh button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Auto-refresh toggle
auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=False)

# Fetch data functions
@st.cache_data(ttl=30)
def fetch_fused_data(symbol):
    try:
        url = f"{ANALYSIS_SERVICE_URL}/fused/{symbol}"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        st.error(f"Connection error to analysis service: {str(e)}")
        return None

@st.cache_data(ttl=30)
def fetch_price_days(symbol):
    try:
        url = f"{PRICE_SERVICE_URL}/prices/{symbol}"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Price service error for {symbol}: {response.status_code}")
            return None
    except Exception as e:
        st.error(f"Connection error to price service: {str(e)}")
        return None

@st.cache_data(ttl=30)
def fetch_all_prices():
    try:
        url = f"{PRICE_SERVICE_URL}/prices"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Price service error: {response.status_code}")
            return None
    except Exception as e:
        st.error(f"Connection error to price service: {str(e)}")
        return None

@st.cache_data(ttl=30)
def fetch_marketcap_history(symbol, limit=100):
    try:
        url = f"{ANALYSIS_SERVICE_URL}/marketcap/history/{symbol}?limit={limit}"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json().get("history", [])
        return []
    except Exception as e:
        print(f"Market cap history error: {e}")
        return []

def build_day_dataframe(day_data, window):
    candles = (day_data or {}).get("candles", []) or []
    if not candles:
        return pd.DataFrame()
    
    df = pd.DataFrame(candles)
    df['timestamp_local'] = pd.to_datetime(df['timestamp_local'])
    df = df.sort_values('timestamp_local').reset_index(drop=True)
    df['SMA'] = df['close'].rolling(window=window, min_periods=1).mean()
    return df

# Service health check
st.sidebar.subheader("🏥 Service Status")

try:
    health_url = f"{ANALYSIS_SERVICE_URL}/health"
    response = requests.get(health_url, timeout=5)
    if response.status_code == 200:
        st.sidebar.success("✅ Analysis Service")
    else:
        st.sidebar.error(f"❌ Analysis Service ({response.status_code})")
except Exception as e:
    st.sidebar.error(f"❌ Analysis Service: {str(e)}")

try:
    health_url = f"{PRICE_SERVICE_URL}/health"
    response = requests.get(health_url, timeout=5)
    if response.status_code == 200:
        st.sidebar.success("✅ Price Service")
    else:
        st.sidebar.error(f"❌ Price Service ({response.status_code})")
except Exception as e:
    st.sidebar.error(f"❌ Price Service: {str(e)}")


# Get data
data = fetch_fused_data(selected_symbol)
all_prices = fetch_all_prices()
price_days = fetch_price_days(selected_symbol)
marketcap_history = fetch_marketcap_history(selected_symbol, correlation_limit)

current_day_df = build_day_dataframe(price_days.get("current_day"), sma_window) if price_days else pd.DataFrame()
previous_day_df = build_day_dataframe(price_days.get("previous_day"), sma_window) if price_days else pd.DataFrame()

day_options = []
if not current_day_df.empty:
    day_options.append("Today")
if not previous_day_df.empty:
    day_options.append("Previous Day")

selected_day_label = None
if day_options:
    default_index = 0
    if "Today" not in day_options and "Previous Day" in day_options:
        default_index = day_options.index("Previous Day")
    selected_day_label = st.radio("Select session to view", day_options, index=default_index, horizontal=True)

selected_day_df = pd.DataFrame()
if selected_day_label == "Today":
    selected_day_df = current_day_df
elif selected_day_label == "Previous Day":
    selected_day_df = previous_day_df
elif not day_options:
    st.warning("No intraday price data is available yet for this symbol.")

if data and data.get("price"):
    chart_label = None
    chart_df = pd.DataFrame()
    if not selected_day_df.empty:
        chart_df = selected_day_df
        chart_label = selected_day_label or "Today"
    elif not current_day_df.empty:
        chart_df = current_day_df
        chart_label = "Today"
    elif not previous_day_df.empty:
        chart_df = previous_day_df
        chart_label = "Previous Day"
    
    # Display key metrics at the top
    col1, col2, col3, col4 = st.columns(4)
    
    latest_price = data.get("price")
    if latest_price:
        with col1:
            st.metric("Latest Close Price", f"${latest_price['close']}")
        with col2:
            st.metric("Volume", f"{latest_price['volume']:,}")
        with col3:
            market_cap = data.get("market_cap")
            if market_cap:
                st.metric("Market Cap", f"${market_cap/1e9:.2f}B")
            else:
                st.metric("Market Cap", "N/A")
        with col4:
            if not chart_df.empty and 'SMA' in chart_df:
                latest_sma = chart_df['SMA'].iloc[-1]
                if pd.notna(latest_sma):
                    st.metric(f"SMA ({sma_window})", f"{latest_sma:.2f}")
                else:
                    st.metric(f"SMA ({sma_window})", "N/A")
            else:
                st.metric(f"SMA ({sma_window})", "N/A")
    
    st.markdown("---")
    
    # === VISUALIZATION 1: Price History with Candlestick Chart ===
    st.subheader("📊 Visualization 1: Price Movement & Volume Analysis")
    
    if price_days and price_days.get("market_status") == "closed" and not current_day_df.empty:
        st.info("Market is closed. Showing the full set of today's candles.")
    
    if not chart_df.empty:
        df = chart_df.copy()
        
        fig1 = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.03,
            row_heights=[0.7, 0.3],
            subplot_titles=(f'{selected_symbol} Price Action ({chart_label})', 'Trading Volume')
        )
        
        # Candlestick
        fig1.add_trace(
            go.Candlestick(
                x=df['timestamp_local'],
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'],
                name='Price'
            ),
            row=1, col=1
        )
        
        # SMA line
        fig1.add_trace(
            go.Scatter(
                x=df['timestamp_local'],
                y=df['SMA'],
                mode='lines',
                name=f"SMA ({sma_window})",
                line=dict(color='orange', width=2)
            ),
            row=1, col=1
        )
        
        # Volume bars
        colors = ['red' if close < open else 'green' 
                  for close, open in zip(df['close'], df['open'])]
        fig1.add_trace(
            go.Bar(
                x=df['timestamp_local'],
                y=df['volume'],
                name='Volume',
                marker_color=colors,
                showlegend=False
            ),
            row=2, col=1
        )
        
        fig1.update_layout(
            height=600,
            xaxis_rangeslider_visible=False,
            hovermode='x unified'
        )
        fig1.update_xaxes(title_text="Time", row=2, col=1)
        fig1.update_yaxes(title_text="Price ($)", row=1, col=1)
        fig1.update_yaxes(title_text="Volume", row=2, col=1)
        
        st.plotly_chart(fig1, use_container_width=True)
    else:
        st.warning("No price history available for the selected day yet.")
    
    st.markdown("---")
    
    # === VISUALIZATION 2: Market Cap History ===
    st.subheader("📊 Visualization 2: Market Cap Tracking")
    
    if marketcap_history and len(marketcap_history) > 5:
        mcap_df = pd.DataFrame(marketcap_history)
        mcap_df['timestamp'] = pd.to_datetime(mcap_df['timestamp'])
        mcap_df['market_cap_billions'] = mcap_df['market_cap'] / 1e9
        
        # Create market cap chart
        fig2 = go.Figure()
        
        # Market cap line
        fig2.add_trace(
            go.Scatter(
                x=mcap_df['timestamp'],
                y=mcap_df['market_cap_billions'],
                mode='lines+markers',
                name='Market Cap',
                line=dict(color='green', width=3),
                marker=dict(size=6),
                hovertemplate='<b>Market Cap:</b> $%{y:.2f}B<extra></extra>'
            )
        )
        
        fig2.update_xaxes(title_text="Time")
        fig2.update_yaxes(title_text="<b>Market Cap ($ Billions)</b>", title_font=dict(color='green'))
        
        fig2.update_layout(
            title=f"{selected_symbol} Market Cap History",
            height=450,
            hovermode='x unified'
        )
        
        st.plotly_chart(fig2, use_container_width=True)
        
        # Calculate statistics
        if len(mcap_df) > 1:
            # Market cap change
            mcap_change = mcap_df['market_cap'].iloc[-1] - mcap_df['market_cap'].iloc[0]
            mcap_change_pct = (mcap_change / mcap_df['market_cap'].iloc[0]) * 100
            
            # Display metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Current Market Cap", f"${mcap_df['market_cap_billions'].iloc[-1]:.2f}B")
            
            with col2:
                st.metric("Change", f"${mcap_change/1e9:+.2f}B", f"{mcap_change_pct:+.2f}%")
            
            with col3:
                st.metric("Average", f"${mcap_df['market_cap_billions'].mean():.2f}B")
            
            with col4:
                st.metric("Data Points", len(mcap_df))
        
    else:
        st.info("""
        📊 **Collecting Market Cap Data...**
        
        This visualization requires at least 5 market cap snapshots to display trends.
        
        Market cap data is fetched from the Fundamental Service and stored over time,
        allowing us to track the company's valuation changes.
        
        Keep the dashboard running to accumulate data points for analysis.
        """)
    
    st.markdown("---")
    
    # === VISUALIZATION 3: Multi-Stock Comparison Dashboard ===
    st.subheader("📊 Visualization 3: Cross-Stock Performance Comparison")

    if all_prices and all_prices.get("data"):
        comparison_data = []
        
        for symbol_payload in all_prices["data"]:
            symbol = symbol_payload.get("symbol")
            current_candles = (symbol_payload.get("current_day") or {}).get("candles", []) or []
            previous_candles = (symbol_payload.get("previous_day") or {}).get("candles", []) or []
            
            latest = current_candles[-1] if current_candles else (previous_candles[-1] if previous_candles else None)
            if not latest or not symbol:
                continue

            # Fetch fused/analysis data
            fused = fetch_fused_data(symbol)

            comparison_data.append({
                "Symbol": symbol,
                "Price": latest.get("close"),
                "Volume": latest.get("volume"),
                "Market Cap": fused.get("market_cap") if fused else None
            })
        
        if comparison_data:
            comp_df = pd.DataFrame(comparison_data)
            
            # Create 1x3 subplot
            fig3 = make_subplots(
                rows=1, cols=3,
                subplot_titles=(
                    'Current Stock Prices',
                    'Trading Volume Comparison',
                    'Market Capitalization'
                ),
                specs=[[{'type': 'bar'}, {'type': 'bar'}, {'type': 'bar'}]]
            )
            
            # Price comparison
            fig3.add_trace(
                go.Bar(
                    x=comp_df['Symbol'],
                    y=comp_df['Price'],
                    name='Price',
                    marker_color='lightblue',
                    text=[f"${p:.2f}" for p in comp_df['Price']],
                    textposition='auto'
                ),
                row=1, col=1
            )
            
            # Volume comparison
            fig3.add_trace(
                go.Bar(
                    x=comp_df['Symbol'],
                    y=comp_df['Volume'],
                    name='Volume',
                    marker_color='lightgreen',
                    text=comp_df['Volume'],
                    textposition='auto'
                ),
                row=1, col=2
            )
            
            # Market Cap comparison with None handling
            market_caps_billions = []
            mcap_text = []
            
            for mcap in comp_df['Market Cap']:
                if mcap is not None:
                    market_caps_billions.append(mcap / 1e9)
                    mcap_text.append(f"{mcap/1e9:.2f}")
                else:
                    market_caps_billions.append(0)
                    mcap_text.append('N/A')
            
            fig3.add_trace(
                go.Bar(
                    x=comp_df['Symbol'],
                    y=market_caps_billions,
                    name='Market Cap',
                    marker_color='coral',
                    text=mcap_text,
                    textposition='auto'
                ),
                row=1, col=3
            )
            
            fig3.update_xaxes(title_text="Stock Symbol", row=1, col=1)
            fig3.update_xaxes(title_text="Stock Symbol", row=1, col=2)
            fig3.update_xaxes(title_text="Stock Symbol", row=1, col=3)
            
            fig3.update_yaxes(title_text="Price ($)", row=1, col=1)
            fig3.update_yaxes(title_text="Volume", row=1, col=2)
            fig3.update_yaxes(title_text="Market Cap ($B)", row=1, col=3)
            
            fig3.update_layout(
                height=500,
                showlegend=False,
                title_text="Multi-Stock Performance Dashboard"
            )
            
            st.plotly_chart(fig3, use_container_width=True)
            
            # Data table - handle None values
            st.subheader("📋 Detailed Comparison Table")
            display_df = comp_df.copy()
            
            # Format Market Cap, handle None
            display_df['Market Cap'] = display_df['Market Cap'].apply(
                lambda x: f"{x/1e9:.2f}" if x is not None else 'N/A'
            )
            
            display_df.columns = ['Symbol', 'Price ($)', 'Volume', 'Market Cap ($B)']
            st.dataframe(display_df, use_container_width=True)

# Footer
st.markdown("---")
st.markdown("*Data updates every 5 minutes during market hours (9:30 AM - 4:00 PM ET)*")
st.markdown("*Historical data is persisted and available across restarts*")

# Auto-refresh logic at the end
if auto_refresh:
    time.sleep(30)
    st.rerun()