import sys
import os
sys.path.append("../common/")
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore", category=ResourceWarning)
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode
from AlphaScreener import AlphaScreener

CUSTOM_CSS = """
<style>
    [data-testid="stSidebar"] {
        position: fixed;
        transition: transform 0.3s ease;
        z-index: 999;
    }
    [data-testid="stSidebar"][aria-expanded="false"] {
        transform: translateX(-100%);
    }
    .main .block-container {
        max-width: 100%;
        padding-top: 0.5rem;
        padding-left: 1rem;
        padding-right: 1rem;
    }
    .main .block-container > div:first-child {
        margin-top: -1rem;
    }
    .stMetric {
        background: #161b22;
        border-radius: 10px;
        padding: 15px;
        border: 1px solid #30363d;
    }
    .stMetric [data-testid="stMetricValue"] {
        color: #58a6ff;
    }
    .stMetric [data-testid="stMetricLabel"] {
        color: #8b949e;
    }
    .status-pill {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 12px;
        font-weight: 600;
    }
    div[data-testid="stColumn"] {
        background: transparent;
    }
    .chart-container {
        background: #0d1117;
        border-radius: 12px;
        padding: 1rem;
        border: 1px solid #30363d;
    }
    button[kind="header"] {
        background: transparent !important;
        border: none !important;
    }
    [data-testid="stHeader"] {
        background: transparent;
    }
    section[data-testid="stSidebar"] {
        background-color: #0d1117;
        border-right: 1px solid #30363d;
        position: fixed;
        z-index: 1000;
    }
    section[data-testid="stSidebar"][aria-expanded="true"] {
        transform: none;
        box-shadow: 4px 0 20px rgba(0,0,0,0.5);
    }
    section[data-testid="stSidebar"] .block-container {
        padding-top: 2rem;
    }
    [data-testid="stSidebarCollapsedControl"] {
        z-index: 1001;
    }
    .css-1r6slb0 {
        background-color: #21262d;
        border: 1px solid #30363d;
        color: #c9d1d9;
    }
    .css-1r6slb0:hover {
        background-color: #30363d;
        border-color: #8b949e;
    }
    div[role="radiogroup"] label {
        background-color: #161b22;
        border: 1px solid #30363d;
        border-radius: 6px;
        padding: 8px 12px;
        color: #c9d1d9;
    }
    div[role="radiogroup"] input[type="radio"]:checked + label {
        background-color: #1f6feb;
        border-color: #1f6feb;
        color: #ffffff;
    }
    .st-emotion-cache-1kyxreq {
        background-color: #238636;
        border-color: #238636;
    }
    [data-testid="stSubheader"] {
        color: #58a6ff;
        border-bottom: 1px solid #30363d;
        padding-bottom: 8px;
    }
    .st-emotion-cache-1jicflk {
        background-color: #0d1117;
    }
</style>
"""

st.set_page_config(
    page_title="AlphaStream 2026",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown(
    """
    <script>
    window.parent.document.querySelector('section').style.backgroundColor = '#0d1117';
    </script>
    """,
    unsafe_allow_html=True
)

st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

lookback = 20
status_lookback = 60

with st.sidebar:
    st.header("&#x2699; Settings", divider="gray")
    scan_mode = st.radio("Scan Mode", ["Alpha Screener", "Recovery Screener"], horizontal=True)
    
    if scan_mode == "Alpha Screener":
        lookback = st.number_input("Z-Score Lookback (days)", value=20, min_value=5, max_value=60, step=5)
        status_lookback = st.number_input("Status History Lookback (days)", value=60, min_value=20, max_value=200, step=10)
        min_rvol = st.number_input("Min RVOL Filter", value=1.0, min_value=0.0, step=0.1)
    else:
        min_drawdown = st.number_input("Min Drawdown %", value=30, min_value=20, max_value=80, step=5)
        max_drawdown = st.number_input("Max Drawdown %", value=70, min_value=30, max_value=90, step=5)
        min_recovery = st.number_input("Min Recovery %", value=15, min_value=5, max_value=50, step=5)
        max_recovery = st.number_input("Max Recovery %", value=60, min_value=20, max_value=80, step=5)
        min_upside = st.number_input("Min Upside to High %", value=30, min_value=10, max_value=100, step=5)
        min_rvol = 0

    chart_type = st.radio("Chart Type", ["Candlestick", "Line"], horizontal=True)
    show_sma = st.checkbox("Show SMA (20/50)", value=True)
    show_volume = st.checkbox("Show Volume", value=True)

    st.divider()
    decrypt_key = st.text_input("&#x1F511; Decryption Key", type="password", help="Enter the decryption key to access the data")
    st.info("&#x1F4E6; Data loaded from encrypted cache file")
    st.caption("Contact the administrator for the decryption key")

st.markdown(
    """
    <div style='display:flex; align-items:center; justify-content:space-between; padding:0; margin:0;'>
        <h1 style='color:#58a6ff; margin:0; font-size:1.5rem;'>&#x1F3AF; Alpha Screener</h1>
    </div>
    """,
    unsafe_allow_html=True
)

run_col1, run_col2 = st.columns([3, 1])
with run_col2:
    if st.button("&#x1F680; Run Scan", use_container_width=True, type="primary"):
        if not decrypt_key:
            st.error("Please enter the decryption key to access the data")
        else:
            st.session_state['run_scan'] = True
            st.session_state['selected_ticker'] = None
            st.session_state['scan_mode'] = scan_mode
            st.session_state['decrypt_key'] = decrypt_key
            force_refresh = st.session_state.pop('force_refresh', False)
            with st.spinner("Analyzing Market Cycles..."):
                try:
                    if scan_mode == "Alpha Screener":
                        sc_instance = AlphaScreener(lookback=lookback, status_lookback=status_lookback, decrypt_key=decrypt_key)
                        st.session_state['raw_results'] = sc_instance.run_scan()
                    else:
                        sc_instance = AlphaScreener(lookback=20, status_lookback=60, decrypt_key=decrypt_key)
                        st.session_state['raw_results'] = sc_instance.run_recovery_scan()
                    st.session_state['screener_instance'] = sc_instance
                except ValueError as e:
                    st.error(str(e))
                    st.session_state['run_scan'] = False

def build_stock_chart(df, ticker, chart_type="Candlestick", show_sma=True, show_volume=True, status_markers=None, screener=None, recovery_metrics=None):
    if df.empty or 'Close' not in df.columns:
        return None

    row_count = 2 if show_volume else 1
    row_heights = [0.75, 0.25] if show_volume else [1.0]

    fig = make_subplots(
        rows=row_count, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=row_heights,
        subplot_titles=(f"{ticker} - Daily", "Volume")
    )

    if chart_type == "Candlestick":
        fig.add_trace(
            go.Candlestick(
                x=df.index,
                open=df['Open'],
                high=df['High'],
                low=df['Low'],
                close=df['Close'],
                name="OHLC",
                increasing_line_color='#26a69a',
                decreasing_line_color='#ef5350',
                increasing_fillcolor='#26a69a',
                decreasing_fillcolor='#ef5350'
            ),
            row=1, col=1
        )
    else:
        fig.add_trace(
            go.Scatter(
                x=df.index, y=df['Close'],
                name="Close",
                line=dict(color='#58a6ff', width=2)
            ),
            row=1, col=1
        )

    if show_sma:
        if 'SMA_20' in df.columns:
            fig.add_trace(
                go.Scatter(x=df.index, y=df['SMA_20'], name="SMA 20",
                          line=dict(color='#f0b90b', width=1.5, dash='dot')),
                row=1, col=1
            )
        if 'SMA_50' in df.columns:
            fig.add_trace(
                go.Scatter(x=df.index, y=df['SMA_50'], name="SMA 50",
                          line=dict(color='#e91e63', width=1.5, dash='dot')),
                row=1, col=1
            )

    if show_volume and 'Volume' in df.columns:
        colors = ['#26a69a' if df['Close'].iloc[i] >= df['Open'].iloc[i] else '#ef5350'
                  for i in range(len(df))]
        fig.add_trace(
            go.Bar(x=df.index, y=df['Volume'], name="Volume",
                  marker_color=colors, opacity=0.5),
            row=2, col=1
        )
        if 'Volume_MA' in df.columns:
            fig.add_trace(
                go.Scatter(x=df.index, y=df['Volume_MA'], name="Vol MA 10",
                          line=dict(color='#58a6ff', width=1.5)),
                row=2, col=1
            )

    status_colors = {
        'FRESH BREAKOUT': '#26a69a',
        'COILING': '#f0b90b',
        'EXTENDED (AVOID)': '#ef5350',
        'PULLBACK': '#8b949e'
    }

    status_shapes = {
        'FRESH BREAKOUT': 'triangle-up',
        'COILING': 'diamond',
        'EXTENDED (AVOID)': 'triangle-down',
        'PULLBACK': 'x'
    }

    if status_markers:
        for m in status_markers:
            color = status_colors.get(m['to_status'], '#58a6ff')
            shape = status_shapes.get(m['to_status'], 'circle')
            label = f"{m['from_status']} → {m['to_status']}"

            fig.add_trace(
                go.Scatter(
                    x=[m['date']],
                    y=[m['price']],
                    mode='markers',
                    marker=dict(
                        symbol=shape,
                        size=12,
                        color=color,
                        line=dict(width=1.5, color='#ffffff')
                    ),
                    name=label,
                    hovertemplate=f"<b>{label}</b><br>Date: %{{x|%b %d, %Y}}<br>Price: $%{{y:.2f}}<extra></extra>",
                    showlegend=False
                ),
                row=1, col=1
            )

    if screener:
        sr = screener._support_resist.get(ticker, {})
        support = sr.get('support')
        resist = sr.get('resist')

        if support:
            fig.add_hline(
                y=support,
                line=dict(color='#26a69a', width=1.5, dash='dash'),
                annotation_text=f"Support: ${support:.2f}",
                annotation_position="bottom left",
                annotation_font_size=11,
                annotation_font_color='#26a69a',
                row=1, col=1
            )
        if resist:
            fig.add_hline(
                y=resist,
                line=dict(color='#ef5350', width=1.5, dash='dash'),
                annotation_text=f"Resistance: ${resist:.2f}",
                annotation_position="top left",
                annotation_font_size=11,
                annotation_font_color='#ef5350',
                row=1, col=1
            )

    if recovery_metrics:
        prior_high = recovery_metrics.get('Prior_High')
        crash_low = recovery_metrics.get('Crash_Low')
        mm_50 = recovery_metrics.get('MM_Target_50')
        mm_100 = recovery_metrics.get('MM_Target_100')
        fib_level = recovery_metrics.get('Fib_Level')
        drawdown = recovery_metrics.get('Max_Drawdown_%')
        recovery = recovery_metrics.get('Recovery_%')
        status = recovery_metrics.get('Status')

        if prior_high:
            fig.add_hline(
                y=prior_high, line=dict(color='#ef5350', width=2, dash='dash'),
                annotation_text=f"PRIOR HIGH: ${prior_high:.2f}",
                annotation_position="top left", annotation_font_size=10,
                annotation_font_color='#ef5350', row=1, col=1
            )

        if crash_low:
            fig.add_hline(
                y=crash_low, line=dict(color='#26a69a', width=2, dash='dash'),
                annotation_text=f"CRASH LOW: ${crash_low:.2f}",
                annotation_position="bottom left", annotation_font_size=10,
                annotation_font_color='#26a69a', row=1, col=1
            )

        if mm_50:
            fig.add_hline(
                y=mm_50, line=dict(color='#58a6ff', width=1.5, dash='dot'),
                annotation_text=f"MM 50%: ${mm_50:.2f}",
                annotation_position="right", annotation_font_size=9,
                annotation_font_color='#58a6ff', row=1, col=1
            )

        if mm_100:
            fig.add_hline(
                y=mm_100, line=dict(color='#58a6ff', width=1.5, dash='dash'),
                annotation_text=f"MM 100%: ${mm_100:.2f}",
                annotation_position="right", annotation_font_size=9,
                annotation_font_color='#58a6ff', row=1, col=1
            )

        if drawdown is not None and recovery is not None and status:
            fig.add_annotation(
                x=0.02, y=0.95, xref="paper", yref="paper",
                text=f"DD: {drawdown:.1f}% | Recovery: {recovery:.1f}% | {status}",
                showarrow=False, font=dict(size=11, color='#f0b90b'),
                bgcolor='rgba(13,17,23,0.85)', bordercolor='#f0b90b',
                borderwidth=1, row=1, col=1
            )

    fig.update_layout(
        template="plotly_dark",
        height=600,
        margin=dict(l=50, r=20, t=40, b=50),
        xaxis_rangeslider_visible=False,
        legend=dict(orientation="h", y=1.08, x=0),
        paper_bgcolor='#0e1117',
        plot_bgcolor='#0e1117',
        font=dict(color='#c9d1d9')
    )

    fig.update_xaxes(
        range=[df.index[0], df.index[-1]],
        gridcolor='#21262d',
        zeroline=False
    )
    fig.update_yaxes(gridcolor='#21262d', zeroline=False)

    return fig

def display_interactive_grid(df, key_suffix=""):
    gb = GridOptionsBuilder.from_dataframe(df)

    gb.configure_selection(selection_mode="single", use_checkbox=False)
    gb.configure_default_column(
        resizable=True,
        filterable=True,
        sortable=True,
        cellStyle={
            "backgroundColor": "transparent",
            "color": "#c9d1d9",
            "borderRight": "1px solid #21262d",
        },
    )

    gb.configure_column("Ticker", pinned='left', width=80)
    gb.configure_column("Company", width=180)
    gb.configure_column("Sector", width=80)
    gb.configure_column("Price", type=["numericColumn"], precision=2, width=80)
    gb.configure_column("Batting_Avg", headerName="Bat", width=50)
    gb.configure_column("Z_Score_5D", headerName="Z", width=60)
    gb.configure_column("RVOL", width=60)
    gb.configure_column("Ext_20MA_%", headerName="Ext%", width=60)
    gb.configure_column("Support", type=["numericColumn"], precision=2, width=80)
    gb.configure_column("Resist", type=["numericColumn"], precision=2, width=80)

    status_jscode = """
    function(params) {
        var val = params.value;
        if (val.includes('FRESH BREAKOUT')) return { 'color': '#0e1117', 'backgroundColor': '#26a69a', 'fontWeight': 'bold' };
        if (val.includes('COILING')) return { 'color': '#0e1117', 'backgroundColor': '#f0b90b', 'fontWeight': 'bold' };
        if (val.includes('EXTENDED')) return { 'color': '#0e1117', 'backgroundColor': '#ef5350', 'fontWeight': 'bold' };
        return { 'color': '#c9d1d9', 'backgroundColor': '#30363d' };
    };
    """
    gb.configure_column("Status", cellStyle=JsCode(status_jscode))

    rvol_jscode = """
    function(params) {
        if (params.value >= 2.0) {
            return { 'color': '#0e1117', 'backgroundColor': '#26a69a', 'fontWeight': 'bold' };
        }
        return null;
    };
    """
    gb.configure_column("RVOL", cellStyle=JsCode(rvol_jscode))

    zone_jscode = """
    function(params) {
        var val = params.value;
        if (val === 'ABOVE RESIST') return { 'color': '#0e1117', 'backgroundColor': '#26a69a', 'fontWeight': 'bold' };
        if (val === 'BELOW SUPPORT') return { 'color': '#0e1117', 'backgroundColor': '#ef5350', 'fontWeight': 'bold' };
        if (val === 'IN RANGE') return { 'color': '#c9d1d9', 'backgroundColor': '#30363d' };
        return { 'color': '#8b949e', 'backgroundColor': '#21262d' };
    };
    """
    gb.configure_column("Price Zone", cellStyle=JsCode(zone_jscode), width=110)

    grid_options = gb.build()

    grid_options['domLayout'] = 'normal'
    grid_options['headerHeight'] = 40
    grid_options['rowHeight'] = 36

    response = AgGrid(
        df,
        gridOptions=grid_options,
        height=550,
        width='100%',
        theme="streamlit",
        update_on=['selectionChanged', 'modelUpdated'],
        columns_auto_size_mode="FIT_ALL_COLUMNS_TO_VIEW",
        allow_unsafe_jscode=True,
        enable_enterprise_modules=False,
        key=f"alpha_grid_{key_suffix}",
        fit_columns_on_grid_load=True,
        custom_css={
            ".ag-root-wrapper": {"background-color": "#0d1117"},
            ".ag-header": {"background-color": "#161b22", "color": "#58a6ff", "font-weight": "600", "text-transform": "uppercase", "font-size": "11px", "letter-spacing": "0.5px"},
            ".ag-header-cell": {"background-color": "#161b22", "color": "#58a6ff"},
            ".ag-row": {"background-color": "#0d1117", "color": "#c9d1d9"},
            ".ag-row-odd": {"background-color": "#111820"},
            ".ag-row-even": {"background-color": "#0d1117"},
            ".ag-row-hover": {"background-color": "#161b22"},
            ".ag-row-selected": {"background-color": "rgba(31, 111, 235, 0.15)", "border-left": "3px solid #58a6ff"},
            ".ag-cell": {"background-color": "transparent", "color": "#c9d1d9"},
            ".ag-cell-value": {"color": "#c9d1d9"},
            ".ag-paging-panel": {"background-color": "#161b22", "color": "#8b949e"},
            ".ag-menu": {"background-color": "#161b22", "color": "#c9d1d9"},
            ".ag-filter-panel": {"background-color": "#161b22", "color": "#c9d1d9"},
        }
    )

    return response

if st.session_state.get('run_scan', False) and 'raw_results' in st.session_state:
    raw_results = st.session_state['raw_results']
    screener = st.session_state.get('screener_instance')
    scan_mode = st.session_state.get('scan_mode', 'Alpha Screener')

    if scan_mode == "Alpha Screener":
        final_results = raw_results[raw_results['RVOL'] >= min_rvol].reset_index(drop=True)

        def extract_status_pair(status_str):
            pair = status_str.split('(')[0].strip().rstrip('/')
            return pair

        unique_status_pairs = final_results['Status'].apply(extract_status_pair).unique()
        unique_status_pairs = sorted(set(unique_status_pairs))
        status_filter_options = ['All'] + unique_status_pairs

        status_filter = st.selectbox("Filter by Status", status_filter_options, index=0)

        unique_zones = sorted(final_results['Price Zone'].unique())
        zone_filter_options = ['All'] + unique_zones
        zone_filter = st.selectbox("Filter by Price Zone", zone_filter_options, index=0)

        filtered_results = final_results

        if status_filter != 'All':
            mask = filtered_results['Status'].apply(
                lambda s: s.startswith(status_filter + ' (') or s == status_filter
            )
            filtered_results = filtered_results[mask].reset_index(drop=True)

        if zone_filter != 'All':
            mask = filtered_results['Price Zone'] == zone_filter
            filtered_results = filtered_results[mask].reset_index(drop=True)
    else:
        final_results = raw_results

        unique_statuses = sorted(final_results['Status'].unique())
        status_filter_options = ['All'] + unique_statuses
        status_filter = st.selectbox("Filter by Status", status_filter_options, index=0)

        filtered_results = final_results
        if status_filter != 'All':
            mask = filtered_results['Status'] == status_filter
            filtered_results = filtered_results[mask].reset_index(drop=True)

    selected_rows = None

    grid_response = display_interactive_grid(filtered_results, key_suffix="main")

    csv = final_results.to_csv(index=False).encode('utf-8')
    st.download_button(
        "&#x1F4C2; Export to CSV",
        data=csv,
        file_name="alpha_screen.csv",
        mime="text/csv",
        use_container_width=True
    )

    selected_rows = grid_response.get("selected_rows")

    if selected_rows is not None and len(selected_rows) > 0:
        selected_ticker = selected_rows.iloc[0]['Ticker']
        st.session_state['selected_ticker'] = selected_ticker
    else:
        selected_ticker = st.session_state.get('selected_ticker')

    if selected_ticker and screener:
        chart_days = 550 if scan_mode == "Recovery Screener" else 365
        chart_df = screener.get_chart_data(selected_ticker, days=chart_days)

        if not chart_df.empty:
            markers = screener.get_status_markers(selected_ticker, days=chart_days)

            recovery_metrics = None
            if scan_mode == "Recovery Screener" and hasattr(screener, '_recovery_results'):
                match = screener._recovery_results[screener._recovery_results['Ticker'] == selected_ticker]
                if not match.empty:
                    recovery_metrics = match.iloc[0].to_dict()

            fig = build_stock_chart(
                chart_df,
                selected_ticker,
                chart_type=chart_type,
                show_sma=show_sma,
                show_volume=show_volume,
                status_markers=markers,
                screener=screener,
                recovery_metrics=recovery_metrics
            )
            if fig:
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning(f"No chart data available for {selected_ticker}")
    else:
        st.info("&#x2B05; Select a row from the table to view its chart")

else:
    st.info("&#x2B05; Click 'Run Scan' in the sidebar to begin.")
