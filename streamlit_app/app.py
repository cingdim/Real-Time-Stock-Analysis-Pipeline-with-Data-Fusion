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
P
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

# Historical data range
st.sidebar.header("Historical Data")
history_limit = st.sidebar.slider("Data Points to Show", 10, 200, 50)

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
def fetch_rsi_history(symbol, limit=100):
    try:
        url = f"{ANALYSIS_SERVICE_URL}/rsi/history/{symbol}?limit={limit}"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json().get("history", [])
        return []
    except Exception as e:
        print(f"RSI history error: {e}")
        return []

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

@st.cache_data(ttl=30)
def fetch_complete_history(symbol, limit=100):
    try:
        url = f"{ANALYSIS_SERVICE_URL}/history/{symbol}?limit={limit}"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json().get("history", [])
        return []
    except Exception as e:
        print(f"Complete history error: {e}")
        return []

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
rsi_history = fetch_rsi_history(selected_symbol, history_limit)
marketcap_history = fetch_marketcap_history(selected_symbol, history_limit)
complete_history = fetch_complete_history(selected_symbol, history_limit)

if data and data.get("price"):
    # Display key metrics at the top
    col1, col2, col3, col4 = st.columns(4)
    
    latest_price = data.get("price")
    if latest_price:
        with col1:
            st.metric("Latest Close Price", f"${latest_price['close']}")
        with col2:
            st.metric("Volume", f"{latest_price['volume']:,}")
        with col3:
            rsi = data.get("indicator", {}).get("rsi14")
            st.metric("RSI (14)", f"{rsi:.2f}" if rsi else "N/A")
        with col4:
            market_cap = data.get("market_cap")
            if market_cap:
                st.metric("Market Cap", f"${market_cap/1e9:.2f}B")
            else:
                st.metric("Market Cap", "N/A")
    
    st.markdown("---")
    
    # === VISUALIZATION 1: Price History with Candlestick Chart ===
    st.subheader("📊 Visualization 1: Price Movement & Volume Analysis")
    
    price_history = data.get("price_history", [])
    if price_history and len(price_history) > 0:
        # Create DataFrame
        df = pd.DataFrame(price_history)
        df['timestamp_local'] = pd.to_datetime(df['timestamp_local'])
        
        # Create candlestick chart with volume
        fig1 = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.03,
            row_heights=[0.7, 0.3],
            subplot_titles=(f'{selected_symbol} Price Action', 'Trading Volume')
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
        st.warning("No price history available")
    
    st.markdown("---")
    
    # === VISUALIZATION 2: RSI Indicator with Historical Trend ===
    st.subheader("📉 Visualization 2: RSI Technical Indicator Analysis")
    
    if rsi_history and len(rsi_history) > 0:
        # Create DataFrame from historical RSI
        rsi_df = pd.DataFrame(rsi_history)
        rsi_df['timestamp'] = pd.to_datetime(rsi_df['timestamp'])
        
        fig2 = go.Figure()
        
        # RSI Line
        fig2.add_trace(go.Scatter(
            x=rsi_df['timestamp'],
            y=rsi_df['rsi'],
            mode='lines+markers',
            name='RSI (14)',
            line=dict(color='blue', width=2),
            marker=dict(size=4)
        ))
        
        # Overbought line (70)
        fig2.add_hline(y=70, line_dash="dash", line_color="red", 
                       annotation_text="Overbought (70)")
        
        # Oversold line (30)
        fig2.add_hline(y=30, line_dash="dash", line_color="green", 
                       annotation_text="Oversold (30)")
        
        # Neutral line (50)
        fig2.add_hline(y=50, line_dash="dot", line_color="gray", 
                       annotation_text="Neutral (50)")
        
        # Color zones
        fig2.add_hrect(y0=70, y1=100, fillcolor="red", opacity=0.1, 
                       annotation_text="Overbought Zone", annotation_position="top left")
        fig2.add_hrect(y0=0, y1=30, fillcolor="green", opacity=0.1, 
                       annotation_text="Oversold Zone", annotation_position="bottom left")
        
        fig2.update_layout(
            title=f"{selected_symbol} RSI Momentum Indicator - Historical Trend",
            xaxis_title="Time",
            yaxis_title="RSI Value",
            height=400,
            hovermode='x unified',
            yaxis=dict(range=[0, 100])
        )
        
        st.plotly_chart(fig2, use_container_width=True)
        
        # RSI Statistics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Current RSI", f"{rsi_df['rsi'].iloc[-1]:.2f}")
        with col2:
            st.metric("Average RSI", f"{rsi_df['rsi'].mean():.2f}")
        with col3:
            st.metric("RSI Range", f"{rsi_df['rsi'].min():.2f} - {rsi_df['rsi'].max():.2f}")
        
        # RSI Interpretation
        current_rsi = data.get("indicator", {}).get("rsi14")
        if current_rsi:
            if current_rsi > 70:
                st.warning(f"⚠️ **Overbought Signal**: RSI is {current_rsi:.2f} - Stock may be overvalued")
            elif current_rsi < 30:
                st.success(f"✅ **Oversold Signal**: RSI is {current_rsi:.2f} - Potential buying opportunity")
            else:
                st.info(f"ℹ️ **Neutral**: RSI is {current_rsi:.2f} - No strong signal")
    else:
        st.warning("Not enough RSI history data available")
    
    st.markdown("---")
    
    # === VISUALIZATION 3: Multi-Stock Comparison Dashboard ===
st.subheader("🔄 Visualization 3: Cross-Stock Performance Comparison")

if all_prices and all_prices.get("data"):
    comparison_data = []
    
    for stock_data in all_prices["data"]:
        if stock_data and len(stock_data) > 0:
            latest = stock_data[-1]
            symbol = latest.get("symbol")
            
            # Get fused data for this symbol
            fused = fetch_fused_data(symbol)
            
            comparison_data.append({
                "Symbol": symbol,
                "Price": latest.get("close"),
                "Volume": latest.get("volume"),
                "RSI": fused.get("indicator", {}).get("rsi14") if fused else None,
                "Market Cap": fused.get("market_cap") if fused else None
            })
    
    if comparison_data:
        comp_df = pd.DataFrame(comparison_data)
        
        # Create 2x2 subplot
        fig3 = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                'Current Stock Prices',
                'Trading Volume Comparison',
                'RSI Comparison',
                'Market Capitalization'
            ),
            specs=[[{'type': 'bar'}, {'type': 'bar'}],
                   [{'type': 'bar'}, {'type': 'bar'}]]
        )
        
        # Price comparison
        fig3.add_trace(
            go.Bar(
                x=comp_df['Symbol'],
                y=comp_df['Price'],
                name='Price',
                marker_color='lightblue',
                text=[f"{p:.2f}" for p in comp_df['Price']],
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
        
        # RSI comparison with None handling
        rsi_values = []
        rsi_colors = []
        rsi_text = []
        
        for rsi in comp_df['RSI']:
            if rsi is not None:
                rsi_values.append(rsi)
                rsi_text.append(f"{rsi:.2f}")
                if rsi > 70:
                    rsi_colors.append('red')
                elif rsi < 30:
                    rsi_colors.append('green')
                else:
                    rsi_colors.append('gray')
            else:
                rsi_values.append(0)
                rsi_text.append('N/A')
                rsi_colors.append('lightgray')
        
        fig3.add_trace(
            go.Bar(
                x=comp_df['Symbol'],
                y=rsi_values,
                name='RSI',
                marker_color=rsi_colors,
                text=rsi_text,
                textposition='auto'
            ),
            row=2, col=1
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
            row=2, col=2
        )
        
        fig3.update_xaxes(title_text="Stock Symbol", row=1, col=1)
        fig3.update_xaxes(title_text="Stock Symbol", row=1, col=2)
        fig3.update_xaxes(title_text="Stock Symbol", row=2, col=1)
        fig3.update_xaxes(title_text="Stock Symbol", row=2, col=2)
        
        fig3.update_yaxes(title_text="Price ($)", row=1, col=1)
        fig3.update_yaxes(title_text="Volume", row=1, col=2)
        fig3.update_yaxes(title_text="RSI Value", row=2, col=1)
        fig3.update_yaxes(title_text="Market Cap ($B)", row=2, col=2)
        
        fig3.update_layout(
            height=700,
            showlegend=False,
            title_text="Multi-Stock Performance Dashboard"
        )
        
        st.plotly_chart(fig3, width='stretch')  # Fixed deprecation warning
        
        # Data table - handle None values
        st.subheader("📋 Detailed Comparison Table")
        display_df = comp_df.copy()
        
        # Format Market Cap, handle None
        display_df['Market Cap'] = display_df['Market Cap'].apply(
            lambda x: f"{x/1e9:.2f}" if x is not None else 'N/A'
        )
        
        # Format RSI, handle None
        display_df['RSI'] = display_df['RSI'].apply(
            lambda x: f"{x:.2f}" if x is not None else 'N/A'
        )
        
        display_df.columns = ['Symbol', 'Price ($)', 'Volume', 'RSI', 'Market Cap ($B)']
        st.dataframe(display_df, width='stretch')  # Fixed deprecation warning
    

# Footer
st.markdown("---")
st.markdown("*Data updates every 5 minutes during market hours (9:30 AM - 4:00 PM ET)*")
st.markdown("*Historical data is persisted and available across restarts*")

# Auto-refresh logic at the end
if auto_refresh:
    time.sleep(30)
    st.rerun()
