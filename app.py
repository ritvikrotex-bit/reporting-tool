"""
MT5 P&L Studio – Manager API Edition
──────────────────────────────────────
Two report modes:

  Deal P&L    – PnL per trade = Profit + Commission + Swap
  Equity P&L  – Net P&L = Closing Equity − Opening Equity − Net D/W − Net Credit − Bonus
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from io import BytesIO

from mt5_connector import (
    connect_mt5,
    disconnect_mt5,
    get_deals,
    get_users,
    deals_to_dicts,
    get_daily_reports,
    daily_to_dicts,
)
from calculations import (
    build_deals_dataframe,
    compute_client_report,
    compute_group_summary,
    compute_symbol_summary,
    get_top_gainers,
    get_top_losers,
    compute_kpis,
    compute_equity_report,
    compute_equity_group_summary,
    compute_equity_kpis,
)

# ═══════════════════════════════════════════════════════════
# PAGE CONFIG & GLOBAL CSS
# ═══════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Reporting Tool",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
<style>
/* ── Base ── */
body, .main, [data-testid="stAppViewContainer"]{
  background: #f0f4f8 !important;
  color: #0f172a;
  font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, Arial, sans-serif;
}
.block-container{max-width:1500px; padding-top:1.2rem; padding-bottom:3rem;}
h1,h2,h3,h4{color:#0f172a;}

/* ── Hero banner ── */
.hero{
  border-radius:20px;
  padding:1.4rem 1.8rem;
  background: linear-gradient(135deg, #0b1220 0%, #1e293b 55%, #0b1220 100%);
  color:#f8fafc;
  border: 1px solid rgba(255,255,255,0.10);
  box-shadow: 0 16px 48px rgba(15,23,42,0.30);
  position:relative; overflow:hidden; margin-bottom:1rem;
}
.hero:before{
  content:""; position:absolute; inset:-100px -100px auto auto;
  width:220px; height:220px;
  background: radial-gradient(circle, rgba(99,102,241,.40), transparent);
}
.hero:after{
  content:""; position:absolute; inset:auto auto -120px -120px;
  width:260px; height:260px;
  background: radial-gradient(circle, rgba(16,185,129,.30), transparent);
}
.hero-badge{
  display:inline-flex; align-items:center; gap:.5rem;
  padding:.22rem .75rem; border-radius:999px; font-size:.78rem;
  color:rgba(248,250,252,.85);
  border:1px solid rgba(255,255,255,.25);
  background:rgba(255,255,255,.08);
  margin-bottom:.6rem;
}
.hero-title{margin:.5rem 0 .25rem 0; font-size:1.9rem; font-weight:800; line-height:1.2;}
.hero-sub{margin:0; color:rgba(248,250,252,.75); max-width:900px; line-height:1.6; font-size:.95rem;}

/* ── Section card ── */
.section{
  margin-top:1rem; padding:1.1rem 1.2rem;
  background:#ffffff;
  border:1px solid #e2e8f0;
  border-radius:16px;
  box-shadow: 0 4px 16px rgba(2,6,23,0.06);
}
.section-title{display:flex; align-items:center; justify-content:space-between; gap:1rem; margin-bottom:.5rem;}
.section-title h2{margin:0; font-size:1.1rem; font-weight:700; color:#0f172a;}
.pill{
  display:inline-flex; align-items:center; gap:.4rem;
  padding:.2rem .7rem; border-radius:999px;
  background:rgba(99,102,241,0.10);
  border:1px solid rgba(99,102,241,0.25);
  color:#4338ca; font-size:.78rem; font-weight:600;
}
.pill-green{
  display:inline-flex; align-items:center; gap:.4rem;
  padding:.2rem .7rem; border-radius:999px;
  background:rgba(5,150,105,0.10);
  border:1px solid rgba(5,150,105,0.25);
  color:#059669; font-size:.78rem; font-weight:600;
}

/* ── KPI Metric card ── */
.metric{
  background:#ffffff;
  border:1px solid #e2e8f0;
  border-radius:14px; padding:1rem 1.1rem;
  box-shadow: 0 4px 14px rgba(2,6,23,0.06);
}
.metric .k{font-size:.7rem; letter-spacing:.10em; text-transform:uppercase; color:#64748b; font-weight:600;}
.metric .v{font-size:1.45rem; font-weight:800; color:#0f172a; margin-top:.25rem; line-height:1.2;}
.metric .s{font-size:.76rem; color:#94a3b8; margin-top:.25rem;}
.metric .v.green{color:#059669 !important;}
.metric .v.red{color:#dc2626 !important;}
.metric .v.blue{color:#2563eb !important;}

/* ── Sidebar ── */
[data-testid="stSidebar"]{
  background: linear-gradient(180deg, #0b1220 0%, #111827 100%) !important;
  border-right: 1px solid rgba(255,255,255,0.07) !important;
}
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] div,
[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3,
[data-testid="stSidebar"] small{
  color: #f1f5f9 !important;
}
[data-testid="stSidebar"] .stTextInput > div > div > input{
  background: #ffffff !important;
  border: 1px solid #cbd5e1 !important;
  color: #0f172a !important;
  border-radius: 10px !important;
  caret-color: #0f172a !important;
}
[data-testid="stSidebar"] .stTextInput > div > div > input::placeholder{
  color: #94a3b8 !important;
}
[data-testid="stSidebar"] .stNumberInput input{
  background: #ffffff !important;
  border: 1px solid #cbd5e1 !important;
  color: #0f172a !important;
  border-radius: 10px !important;
  caret-color: #0f172a !important;
}
[data-testid="stSidebar"] .stDateInput input{
  background: #ffffff !important;
  border: 1px solid #cbd5e1 !important;
  color: #0f172a !important;
  border-radius: 10px !important;
  caret-color: #0f172a !important;
}
[data-testid="stSidebar"] .stSlider [data-baseweb="slider"]{
  background: rgba(255,255,255,0.15) !important;
}
[data-testid="stSidebar"] .stButton > button{
  background: linear-gradient(135deg, #6366f1, #4f46e5) !important;
  color: #ffffff !important;
  border: none !important;
  border-radius: 12px !important;
  font-weight: 700 !important;
  padding: .6rem 1rem !important;
  box-shadow: 0 4px 14px rgba(99,102,241,.40) !important;
}
[data-testid="stSidebar"] .stButton > button:hover{
  background: linear-gradient(135deg, #4f46e5, #4338ca) !important;
}
[data-testid="stSidebar"] hr{border-color: rgba(255,255,255,0.12) !important;}

/* ── File uploader dropzone: white bg, dark text ── */
[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"]{
  background: #ffffff !important;
  border: 1px solid #cbd5e1 !important;
  border-radius: 10px !important;
}
[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] span,
[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] p,
[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] div,
[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] small,
[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] button{
  color: #0f172a !important;
}
/* ── Uploaded filename row: keep text white ── */
[data-testid="stSidebar"] [data-testid="stFileUploader"] [data-testid="stFileUploaderFileName"],
[data-testid="stSidebar"] [data-testid="stFileUploader"] [class*="uploadedFile"] span,
[data-testid="stSidebar"] [data-testid="stFileUploader"] [class*="uploadedFile"] div,
[data-testid="stSidebar"] [data-testid="stFileUploader"] [class*="fileName"]{
  color: #f1f5f9 !important;
}

/* ── DataFrames ── */
[data-testid="stDataFrame"]{
  border-radius:12px; overflow:hidden;
  border:1px solid #e2e8f0;
  box-shadow: 0 4px 14px rgba(2,6,23,0.05);
}

/* ── Main area inputs ── */
.stTextInput > div > div > input,
.stNumberInput input,
.stDateInput input{
  border-radius:10px !important;
  border: 1px solid #cbd5e1 !important;
  background: #ffffff !important;
  color: #0f172a !important;
}
.stSelectbox > div > div{border-radius:10px !important;}
.stButton > button{
  border-radius:12px !important;
  font-weight:700 !important;
}
</style>
""",
    unsafe_allow_html=True,
)


# ═══════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## MT5 Connection")
    st.caption("Enter your MT5 Manager API credentials.")
    st.divider()

    server = st.text_input(
        "Server (IP:Port)",
        placeholder="e.g. 188.240.63.240:443",
        help="MT5 server address with port",
    )
    login = st.number_input(
        "Manager Login",
        min_value=0,
        step=1,
        value=0,
        help="Your MT5 Manager login ID",
    )
    password = st.text_input(
        "Password",
        type="password",
        help="MT5 Manager password",
    )

    st.divider()
    st.markdown("### Report Type")
    report_type = st.radio(
        "Select report",
        ["Deal P&L", "Equity P&L"],
        label_visibility="collapsed",
    )

    st.divider()

    if report_type == "Deal P&L":
        st.markdown("### Date Range")
        from_date = st.date_input(
            "From Date",
            value=datetime.utcnow().date() - timedelta(days=1),
        )
        to_date = st.date_input(
            "To Date",
            value=datetime.utcnow().date(),
        )
        st.divider()
        st.markdown("### Options")
        top_n = st.slider("Top N accounts", 5, 50, 10)
        # Equity placeholders (not used)
        oe_date = ce_date = None
        account_file = None

    else:  # Equity P&L
        st.markdown("### Equity Dates")
        oe_date = st.date_input(
            "Opening Equity Date",
            value=datetime.utcnow().date() - timedelta(days=7),
            help="End-of-day equity snapshot for this date is used as opening equity.",
        )
        ce_date = st.date_input(
            "Closing Equity Date",
            value=datetime.utcnow().date(),
            help="End-of-day equity snapshot for this date is used as closing equity.",
        )
        st.divider()
        st.markdown("### Account Filter *(optional)*")
        account_file = st.file_uploader(
            "Upload account list",
            type=["csv", "xlsx", "xls"],
            help="CSV or Excel with a 'Login' column (or logins in the first column). "
                 "If omitted, all accounts are included.",
        )
        st.divider()
        top_n = st.slider("Top N accounts", 5, 50, 10)
        # Deal placeholders (not used)
        from_date = to_date = None

    st.divider()
    generate = st.button("Generate Report", use_container_width=True)


# ═══════════════════════════════════════════════════════════
# HERO HEADER
# ═══════════════════════════════════════════════════════════
if report_type == "Deal P&L":
    hero_sub = (
        "Connect to MT5 → fetch closed deals → deal-based P&amp;L report.<br>"
        "<b>Formula:</b> Net PnL = Profit + Commission + Swap &nbsp;|&nbsp; "
        "Hit Ratio = Wins / (Wins + Losses) × 100"
    )
else:
    hero_sub = (
        "Connect to MT5 → fetch daily equity snapshots → equity-based P&amp;L.<br>"
        "<b>Formula:</b> Net P&amp;L = Closing Equity − Opening Equity − Net D/W − Net Credit − Bonus"
    )

st.markdown(
    f"""
<div class="hero">
  <div class="hero-badge">MT5 Manager API &nbsp;|&nbsp; {report_type}</div>
  <div class="hero-title">Reporting Tool</div>
  <p class="hero-sub">{hero_sub}</p>
</div>
""",
    unsafe_allow_html=True,
)


# ═══════════════════════════════════════════════════════════
# HELPER: load account filter from uploaded file
# ═══════════════════════════════════════════════════════════
def _load_account_filter(uploaded_file) -> list:
    """Parse uploaded CSV/Excel and return list of login integers."""
    if uploaded_file is None:
        return None
    try:
        if uploaded_file.name.endswith((".xlsx", ".xls")):
            df = pd.read_excel(uploaded_file)
        else:
            df = pd.read_csv(uploaded_file)

        # Accept column named Login / login / ACCOUNT / account, else first column
        col = None
        for candidate in ("Login", "login", "ACCOUNT", "account", "AccountID"):
            if candidate in df.columns:
                col = candidate
                break
        if col is None:
            col = df.columns[0]

        logins = pd.to_numeric(df[col], errors="coerce").dropna().astype(int).tolist()
        return logins if logins else None
    except Exception as e:
        st.warning(f"Could not parse account file: {e}")
        return None


# ═══════════════════════════════════════════════════════════
# GENERATE REPORT
# ═══════════════════════════════════════════════════════════
if generate:

    # ── Shared validation ──
    if not server:
        st.error("Please enter the MT5 Server address (IP:Port).")
        st.stop()
    if login == 0:
        st.error("Please enter a valid Manager Login.")
        st.stop()
    if not password:
        st.error("Please enter the Manager Password.")
        st.stop()

    # ── Connect ──
    with st.spinner("Connecting to MT5 Manager API…"):
        manager, err = connect_mt5(server, int(login), password)
    if err:
        st.error(f"❌ {err}")
        st.stop()
    st.success("Connected to MT5 Manager API")

    # ════════════════════════════════════════════════════════
    # DEAL P&L REPORT
    # ════════════════════════════════════════════════════════
    if report_type == "Deal P&L":

        if from_date > to_date:
            st.error("'From Date' must be before 'To Date'.")
            disconnect_mt5(manager)
            st.stop()

        dt_from = datetime.combine(from_date, datetime.min.time())
        dt_to   = datetime.combine(to_date,   datetime.max.time())

        try:
            with st.spinner(f"Fetching deals {from_date} → {to_date}…"):
                deals_raw, err = get_deals(manager, dt_from, dt_to)
            if err:
                st.warning(f"⚠️ {err}")

            if not deals_raw:
                st.warning("No trade deals found in the selected date range.")
                disconnect_mt5(manager)
                st.stop()

            deal_dicts  = deals_to_dicts(deals_raw, {})
            deal_logins = list({d["Login"] for d in deal_dicts if d.get("Login")})

            with st.spinner(f"Fetching group data for {len(deal_logins):,} logins…"):
                user_map, err = get_users(manager, deal_logins)
            if err:
                st.warning(f"⚠️ {err}")

            for d in deal_dicts:
                d["Group"] = user_map.get(d["Login"], "")

            df_deals = build_deals_dataframe(deal_dicts)

            with st.spinner("Crunching numbers…"):
                report     = compute_client_report(df_deals)
                group_df   = compute_group_summary(report)
                symbol_df  = compute_symbol_summary(df_deals)
                kpis       = compute_kpis(report)

            # ── KPI Overview ──
            st.markdown(
                """<div class="section"><div class="section-title">
                <h2>Overview</h2><span class="pill">Deal P&L KPIs</span>
                </div></div>""",
                unsafe_allow_html=True,
            )

            pnl_color = "green" if kpis["total_pnl"] >= 0 else "red"
            k1, k2, k3, k4 = st.columns(4)
            with k1:
                st.markdown(
                    f'<div class="metric"><div class="k">Clients</div>'
                    f'<div class="v">{kpis["total_clients"]:,}</div>'
                    f'<div class="s">Unique logins with trades</div></div>',
                    unsafe_allow_html=True,
                )
            with k2:
                st.markdown(
                    f'<div class="metric"><div class="k">Net Client PnL</div>'
                    f'<div class="v {pnl_color}">${kpis["total_pnl"]:,.2f}</div>'
                    f'<div class="s">Profit + Commission + Swap</div></div>',
                    unsafe_allow_html=True,
                )
            with k3:
                st.markdown(
                    f'<div class="metric"><div class="k">Closed Lots</div>'
                    f'<div class="v">{kpis["total_lots"]:,.2f}</div>'
                    f'<div class="s">Sum of deal volumes</div></div>',
                    unsafe_allow_html=True,
                )
            with k4:
                st.markdown(
                    f'<div class="metric"><div class="k">Total Trades</div>'
                    f'<div class="v">{kpis["total_trades"]:,}</div>'
                    f'<div class="s">Avg hit ratio: {kpis["avg_hit_ratio"]:.1f}%</div></div>',
                    unsafe_allow_html=True,
                )

            k5, k6, k7, k8 = st.columns(4)
            with k5:
                st.markdown(
                    f'<div class="metric"><div class="k">Total Profit</div>'
                    f'<div class="v green">${kpis["total_profit"]:,.2f}</div>'
                    f'<div class="s">Accounts with PnL &gt; 0</div></div>',
                    unsafe_allow_html=True,
                )
            with k6:
                st.markdown(
                    f'<div class="metric"><div class="k">Total Loss</div>'
                    f'<div class="v red">${kpis["total_loss"]:,.2f}</div>'
                    f'<div class="s">Accounts with PnL &lt; 0</div></div>',
                    unsafe_allow_html=True,
                )
            with k7:
                st.markdown(
                    f'<div class="metric"><div class="k">Volume USD</div>'
                    f'<div class="v">${kpis["total_volume"]:,.0f}</div>'
                    f'<div class="s">Lots × 100,000</div></div>',
                    unsafe_allow_html=True,
                )
            with k8:
                st.markdown(
                    f'<div class="metric"><div class="k">Date Range</div>'
                    f'<div class="v" style="font-size:1.1rem;">{from_date} → {to_date}</div>'
                    f'<div class="s">Report period</div></div>',
                    unsafe_allow_html=True,
                )

            chart_data = pd.DataFrame(
                {"Side": ["Profit", "Loss"], "Amount": [kpis["total_profit"], abs(kpis["total_loss"])]}
            ).set_index("Side")
            st.bar_chart(chart_data, height=240)

            # ── Top Gainers / Losers ──
            st.markdown(
                f"""<div class="section"><div class="section-title">
                <h2>Top {top_n} Accounts</h2><span class="pill">Leaders</span>
                </div></div>""",
                unsafe_allow_html=True,
            )

            display_cols = ["Login", "Group", "Closed Lots", "NET PNL USD", "Total Trades", "Wins", "Losses", "Hit Ratio %"]
            t1, t2 = st.columns(2)
            with t1:
                st.markdown(f"**Top {top_n} Gainers**")
                st.dataframe(
                    get_top_gainers(report, top_n)[display_cols].style.format(
                        {"NET PNL USD": "${:,.2f}", "Closed Lots": "{:,.2f}", "Hit Ratio %": "{:.1f}%"}
                    ),
                    use_container_width=True,
                )
            with t2:
                st.markdown(f"**Top {top_n} Losers**")
                st.dataframe(
                    get_top_losers(report, top_n)[display_cols].style.format(
                        {"NET PNL USD": "${:,.2f}", "Closed Lots": "{:,.2f}", "Hit Ratio %": "{:.1f}%"}
                    ),
                    use_container_width=True,
                )

            # ── Group Summary ──
            if not group_df.empty:
                st.markdown(
                    """<div class="section"><div class="section-title">
                    <h2>Group Summary</h2><span class="pill">Aggregated</span>
                    </div></div>""",
                    unsafe_allow_html=True,
                )
                g1, g2 = st.columns(2)
                with g1:
                    st.markdown("**Top profit groups**")
                    st.dataframe(group_df.sort_values("NET_PNL_USD", ascending=False).head(top_n), use_container_width=True)
                with g2:
                    st.markdown("**Top loss groups**")
                    st.dataframe(group_df.sort_values("NET_PNL_USD", ascending=True).head(top_n), use_container_width=True)

            # ── Symbol Breakdown ──
            if not symbol_df.empty:
                st.markdown(
                    """<div class="section"><div class="section-title">
                    <h2>Symbol Breakdown</h2><span class="pill">Instruments</span>
                    </div></div>""",
                    unsafe_allow_html=True,
                )
                st.dataframe(
                    symbol_df.head(30).style.format({"NET_PNL_USD": "${:,.2f}", "Closed_Lots": "{:,.2f}"}),
                    use_container_width=True,
                )

            # ── Full Client Table ──
            st.markdown(
                """<div class="section"><div class="section-title">
                <h2>Full Client Report</h2><span class="pill">All Accounts</span>
                </div></div>""",
                unsafe_allow_html=True,
            )
            all_groups = ["All"] + sorted(report["Group"].dropna().unique().tolist())
            selected_group = st.selectbox("Filter by Group", all_groups)
            filtered = report if selected_group == "All" else report[report["Group"] == selected_group]
            st.dataframe(
                filtered.style.format({
                    "NET PNL USD": "${:,.2f}", "Volume USD": "${:,.0f}",
                    "Closed Lots": "{:,.2f}", "Hit Ratio %": "{:.1f}%",
                    "Commission": "${:,.2f}", "Swap": "${:,.2f}",
                }),
                use_container_width=True,
            )
            st.caption(f"Showing {len(filtered):,} of {len(report):,} accounts")

            # ── Export ──
            st.markdown(
                """<div class="section"><div class="section-title">
                <h2>Export</h2><span class="pill">Download</span>
                </div></div>""",
                unsafe_allow_html=True,
            )
            exp1, exp2 = st.columns(2)
            with exp1:
                st.download_button(
                    "Download CSV",
                    data=report.to_csv(index=False),
                    file_name=f"DealPnL_{from_date}_to_{to_date}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
            with exp2:
                buf = BytesIO()
                with pd.ExcelWriter(buf, engine="openpyxl") as w:
                    report.to_excel(w, index=False, sheet_name="Client PnL")
                    group_df.to_excel(w, index=False, sheet_name="Group Summary")
                    symbol_df.to_excel(w, index=False, sheet_name="Symbol Breakdown")
                buf.seek(0)
                st.download_button(
                    "Download Excel",
                    data=buf,
                    file_name=f"DealPnL_{from_date}_to_{to_date}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )

        except Exception as e:
            st.error(f"❌ Error generating report: {e}")
            import traceback
            st.code(traceback.format_exc())
        finally:
            disconnect_mt5(manager)
            st.info("MT5 connection closed.")

    # ════════════════════════════════════════════════════════
    # EQUITY P&L REPORT
    # ════════════════════════════════════════════════════════
    else:
        if oe_date > ce_date:
            st.error("Opening Equity Date must be before (or equal to) Closing Equity Date.")
            disconnect_mt5(manager)
            st.stop()

        account_filter = _load_account_filter(account_file)
        if account_filter is not None:
            st.info(f"Account filter loaded: {len(account_filter):,} logins")

        try:
            # Date boundaries
            oe_dt_from  = datetime.combine(oe_date, datetime.min.time())
            oe_dt_to    = datetime.combine(oe_date, datetime.max.time())
            ce_dt_from  = datetime.combine(ce_date, datetime.min.time())
            ce_dt_to    = datetime.combine(ce_date, datetime.max.time())

            # Summary period: day after OE through CE (captures all D/W between snapshots)
            sum_dt_from = datetime.combine(oe_date + timedelta(days=1), datetime.min.time())
            sum_dt_to   = datetime.combine(ce_date, datetime.max.time())
            summary_available = sum_dt_from <= sum_dt_to

            # ── Fetch Opening Equity snapshots ──
            with st.spinner(f"Fetching Opening Equity snapshots ({oe_date})…"):
                oe_raw, err = get_daily_reports(manager, oe_dt_from, oe_dt_to)
            if err:
                st.warning(f"⚠️ Opening Equity fetch: {err}")
            oe_dicts = daily_to_dicts(oe_raw) if oe_raw else []

            # ── Fetch Closing Equity snapshots ──
            with st.spinner(f"Fetching Closing Equity snapshots ({ce_date})…"):
                ce_raw, err = get_daily_reports(manager, ce_dt_from, ce_dt_to)
            if err:
                st.warning(f"⚠️ Closing Equity fetch: {err}")
            ce_dicts = daily_to_dicts(ce_raw) if ce_raw else []

            # ── Fetch summary period (D/W, credit, bonus) ──
            summary_dicts = []
            if summary_available:
                with st.spinner(f"Fetching activity {oe_date + timedelta(days=1)} → {ce_date}…"):
                    sm_raw, err = get_daily_reports(manager, sum_dt_from, sum_dt_to)
                if err:
                    st.warning(f"⚠️ Summary period fetch: {err}")
                summary_dicts = daily_to_dicts(sm_raw) if sm_raw else []

            if not oe_dicts and not ce_dicts:
                st.warning("No daily equity data found for the selected dates. "
                           "Ensure the MT5 server has Daily Reports enabled and the dates are valid.")
                disconnect_mt5(manager)
                st.stop()

            # ── Fetch group data ──
            all_logins = set()
            for dicts in (oe_dicts, ce_dicts, summary_dicts):
                all_logins.update(d["Login"] for d in dicts)
            if account_filter:
                all_logins = all_logins.intersection(set(account_filter))

            logins_list = list(all_logins)
            with st.spinner(f"Fetching group data for {len(logins_list):,} logins…"):
                user_map, err = get_users(manager, logins_list)
            if err:
                st.warning(f"⚠️ {err}")

            # ── Compute equity report ──
            with st.spinner("Computing equity P&L…"):
                eq_report  = compute_equity_report(
                    oe_dicts, ce_dicts, summary_dicts, user_map, account_filter
                )
                eq_groups  = compute_equity_group_summary(eq_report)
                eq_kpis    = compute_equity_kpis(eq_report)

            if eq_report.empty:
                st.warning("No equity data found for the selected accounts and dates.")
                disconnect_mt5(manager)
                st.stop()

            # ════════════════════════════════
            # KPI OVERVIEW
            # ════════════════════════════════
            st.markdown(
                """<div class="section"><div class="section-title">
                <h2>Overview</h2><span class="pill">Equity P&L KPIs</span>
                </div></div>""",
                unsafe_allow_html=True,
            )

            pnl_color = "green" if eq_kpis["total_net_pnl"] >= 0 else "red"
            k1, k2, k3, k4 = st.columns(4)
            with k1:
                st.markdown(
                    f'<div class="metric"><div class="k">Accounts</div>'
                    f'<div class="v">{eq_kpis["total_accounts"]:,}</div>'
                    f'<div class="s">In report</div></div>',
                    unsafe_allow_html=True,
                )
            with k2:
                st.markdown(
                    f'<div class="metric"><div class="k">Net P&L</div>'
                    f'<div class="v {pnl_color}">${eq_kpis["total_net_pnl"]:,.2f}</div>'
                    f'<div class="s">CE − OE − D/W − Credit − Bonus</div></div>',
                    unsafe_allow_html=True,
                )
            with k3:
                st.markdown(
                    f'<div class="metric"><div class="k">Opening Equity</div>'
                    f'<div class="v blue">${eq_kpis["total_oe"]:,.2f}</div>'
                    f'<div class="s">{oe_date}</div></div>',
                    unsafe_allow_html=True,
                )
            with k4:
                st.markdown(
                    f'<div class="metric"><div class="k">Closing Equity</div>'
                    f'<div class="v blue">${eq_kpis["total_ce"]:,.2f}</div>'
                    f'<div class="s">{ce_date}</div></div>',
                    unsafe_allow_html=True,
                )

            k5, k6, k7, k8 = st.columns(4)
            with k5:
                st.markdown(
                    f'<div class="metric"><div class="k">Net D/W</div>'
                    f'<div class="v">${eq_kpis["total_net_dw"]:,.2f}</div>'
                    f'<div class="s">Deposits + Withdrawals</div></div>',
                    unsafe_allow_html=True,
                )
            with k6:
                st.markdown(
                    f'<div class="metric"><div class="k">Net Credit</div>'
                    f'<div class="v">${eq_kpis["total_net_credit"]:,.2f}</div>'
                    f'<div class="s">Credit In + Credit Out</div></div>',
                    unsafe_allow_html=True,
                )
            with k7:
                st.markdown(
                    f'<div class="metric"><div class="k">Bonus</div>'
                    f'<div class="v">${eq_kpis["total_bonus"]:,.2f}</div>'
                    f'<div class="s">Total bonus added</div></div>',
                    unsafe_allow_html=True,
                )
            with k8:
                st.markdown(
                    f'<div class="metric"><div class="k">Profitable / Losing</div>'
                    f'<div class="v" style="font-size:1.2rem;">'
                    f'<span style="color:#059669">{eq_kpis["profitable_accounts"]}</span>'
                    f' / <span style="color:#dc2626">{eq_kpis["losing_accounts"]}</span></div>'
                    f'<div class="s">Accounts by Net P&amp;L sign</div></div>',
                    unsafe_allow_html=True,
                )

            # Equity change chart
            chart_data = pd.DataFrame({
                "Metric": ["Opening Equity", "Closing Equity"],
                "Value":  [eq_kpis["total_oe"], eq_kpis["total_ce"]],
            }).set_index("Metric")
            st.bar_chart(chart_data, height=240)

            # ════════════════════════════════
            # TOP GAINERS / LOSERS
            # ════════════════════════════════
            st.markdown(
                f"""<div class="section"><div class="section-title">
                <h2>Top {top_n} Accounts</h2><span class="pill">Leaders</span>
                </div></div>""",
                unsafe_allow_html=True,
            )

            eq_display = ["Login", "Group", "Opening Equity", "Closing Equity", "Net D/W", "Net Credit", "Bonus", "Net P&L"]
            eq_fmt = {c: "${:,.2f}" for c in ["Opening Equity", "Closing Equity", "Net D/W", "Net Credit", "Bonus", "Net P&L"]}

            t1, t2 = st.columns(2)
            with t1:
                st.markdown(f"**Top {top_n} Gainers**")
                st.dataframe(
                    eq_report.sort_values("Net P&L", ascending=False).head(top_n)[eq_display].style.format(eq_fmt),
                    use_container_width=True,
                )
            with t2:
                st.markdown(f"**Top {top_n} Losers**")
                st.dataframe(
                    eq_report.sort_values("Net P&L", ascending=True).head(top_n)[eq_display].style.format(eq_fmt),
                    use_container_width=True,
                )

            # ════════════════════════════════
            # GROUP SUMMARY
            # ════════════════════════════════
            if not eq_groups.empty:
                st.markdown(
                    """<div class="section"><div class="section-title">
                    <h2>Group Summary</h2><span class="pill">Aggregated</span>
                    </div></div>""",
                    unsafe_allow_html=True,
                )
                st.dataframe(
                    eq_groups.style.format({
                        "Opening_Equity": "${:,.2f}", "Closing_Equity": "${:,.2f}",
                        "Net_DW": "${:,.2f}", "Net_Credit": "${:,.2f}",
                        "Bonus": "${:,.2f}", "Net_PnL": "${:,.2f}",
                    }),
                    use_container_width=True,
                )

            # ════════════════════════════════
            # FULL ACCOUNT TABLE
            # ════════════════════════════════
            st.markdown(
                """<div class="section"><div class="section-title">
                <h2>Full Account Report</h2><span class="pill">All Accounts</span>
                </div></div>""",
                unsafe_allow_html=True,
            )

            money_cols = ["Opening Equity", "Closing Equity", "Difference",
                          "Deposits", "Withdrawals", "Net D/W",
                          "Credit In", "Credit Out", "Net Credit", "Bonus", "Net P&L"]
            fmt = {c: "${:,.2f}" for c in money_cols}

            all_groups_list = ["All"] + sorted(eq_report["Group"].dropna().unique().tolist())
            sel_group = st.selectbox("Filter by Group", all_groups_list)
            filtered_eq = eq_report if sel_group == "All" else eq_report[eq_report["Group"] == sel_group]

            st.dataframe(filtered_eq.style.format(fmt), use_container_width=True)
            st.caption(f"Showing {len(filtered_eq):,} of {len(eq_report):,} accounts")

            # ════════════════════════════════
            # EXPORT
            # ════════════════════════════════
            st.markdown(
                """<div class="section"><div class="section-title">
                <h2>Export</h2><span class="pill">Download</span>
                </div></div>""",
                unsafe_allow_html=True,
            )
            exp1, exp2 = st.columns(2)
            with exp1:
                st.download_button(
                    "Download CSV",
                    data=eq_report.to_csv(index=False),
                    file_name=f"EquityPnL_{oe_date}_to_{ce_date}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
            with exp2:
                buf = BytesIO()
                with pd.ExcelWriter(buf, engine="openpyxl") as w:
                    eq_report.to_excel(w, index=False, sheet_name="Equity PnL")
                    if not eq_groups.empty:
                        eq_groups.to_excel(w, index=False, sheet_name="Group Summary")
                buf.seek(0)
                st.download_button(
                    "Download Excel",
                    data=buf,
                    file_name=f"EquityPnL_{oe_date}_to_{ce_date}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )

        except Exception as e:
            st.error(f"❌ Error generating equity report: {e}")
            import traceback
            st.code(traceback.format_exc())
        finally:
            disconnect_mt5(manager)
            st.info("MT5 connection closed.")


# ═══════════════════════════════════════════════════════════
# DEFAULT STATE (before generate is clicked)
# ═══════════════════════════════════════════════════════════
else:
    if report_type == "Deal P&L":
        st.markdown(
            """
<div class="section"><div class="section-title">
<h2>Getting Started — Deal P&L</h2><span class="pill">Guide</span>
</div></div>
""",
            unsafe_allow_html=True,
        )
        st.markdown(
            """
**How it works:**

1. Enter MT5 Manager credentials in the sidebar
2. Select a **From / To date range** for closed deals
3. Click **Generate Report**
4. View KPIs, top gainers/losers, group and symbol breakdowns
5. Export as CSV or Excel

**Formula:** PnL = Profit + Commission + Swap
**Hit Ratio:** Wins / (Wins + Losses) × 100
**Volume USD:** Closed Lots × 100,000 (standard forex lot)
"""
        )
    else:
        st.markdown(
            """
<div class="section"><div class="section-title">
<h2>Getting Started — Equity P&L</h2><span class="pill-green">Guide</span>
</div></div>
""",
            unsafe_allow_html=True,
        )
        st.markdown(
            """
**How it works:**

1. Enter MT5 Manager credentials in the sidebar
2. Select an **Opening Equity Date** and **Closing Equity Date**
3. Optionally upload an **account list** (CSV/Excel with a Login column) to filter specific accounts
4. Click **Generate Report**
5. View equity-based P&L per account with full D/W and credit breakdown
6. Export as CSV or Excel

**Formula:**
`Net P&L = Closing Equity − Opening Equity − Net D/W − Net Credit − Bonus`

**Data source:** MT5 Daily Reports (`DailyRequestByGroupNumPy`)
- **Opening / Closing Equity** = `ProfitEquity` on the selected date (end-of-day equity incl. floating)
- **Net D/W** = sum of `DailyBalance` between OE+1 and CE date
- **Net Credit** = sum of `DailyCredit` between OE+1 and CE date
- **Bonus** = sum of `DailyBonus` between OE+1 and CE date

**Account filter:** If no file is uploaded, all accounts with daily data are included.
"""
        )
