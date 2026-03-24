"""
Calculations Engine
───────────────────
All report metrics: PnL, volume, hit ratio, group aggregation.

Core formula:
    PnL = deal.Profit + deal.Commission + deal.Storage (swap)
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Optional


def build_deals_dataframe(deal_dicts: List[Dict]) -> pd.DataFrame:
    """Convert list of deal dicts into a clean DataFrame."""
    if not deal_dicts:
        return pd.DataFrame(
            columns=[
                "Login", "Symbol", "Volume", "Profit",
                "Commission", "Swap", "PnL", "Group", "Time",
            ]
        )

    df = pd.DataFrame(deal_dicts)

    for col in ["Volume", "Profit", "Commission", "Swap", "PnL"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df["Login"] = pd.to_numeric(df["Login"], errors="coerce").astype("Int64")

    return df


def compute_client_report(df: pd.DataFrame) -> pd.DataFrame:
    """
    Per-login aggregation:
        - NET PNL USD    = sum of (Profit + Commission + Swap)
        - Closed Lots    = sum of Volume
        - Volume USD     = sum of Volume * 100000 (forex standard lot)
        - Total Trades   = count of deals
        - Wins           = count where PnL > 0
        - Losses         = count where PnL < 0
        - Hit Ratio %    = wins / (wins + losses) * 100
    """
    if df.empty:
        return pd.DataFrame(
            columns=[
                "Login", "Group", "Closed Lots", "Volume USD",
                "NET PNL USD", "Total Trades", "Wins", "Losses",
                "Hit Ratio %", "Commission", "Swap",
            ]
        )

    agg = (
        df.groupby("Login", as_index=False)
        .agg(
            Group=("Group", "first"),
            Closed_Lots=("Volume", "sum"),
            NET_PNL_USD=("PnL", "sum"),
            Total_Trades=("PnL", "count"),
            Commission=("Commission", "sum"),
            Swap=("Swap", "sum"),
        )
    )

    wins = df[df["PnL"] > 0].groupby("Login").size().rename("Wins")
    losses = df[df["PnL"] < 0].groupby("Login").size().rename("Losses")

    agg = agg.merge(wins, on="Login", how="left")
    agg = agg.merge(losses, on="Login", how="left")

    agg["Wins"] = agg["Wins"].fillna(0).astype(int)
    agg["Losses"] = agg["Losses"].fillna(0).astype(int)

    total_wl = agg["Wins"] + agg["Losses"]
    agg["Hit Ratio %"] = np.where(total_wl > 0, (agg["Wins"] / total_wl) * 100.0, 0.0)

    agg["Volume USD"] = agg["Closed_Lots"] * 100_000

    agg = agg.rename(columns={
        "Closed_Lots":  "Closed Lots",
        "NET_PNL_USD":  "NET PNL USD",
        "Total_Trades": "Total Trades",
    })

    col_order = [
        "Login", "Group", "Closed Lots", "Volume USD",
        "NET PNL USD", "Total Trades", "Wins", "Losses",
        "Hit Ratio %", "Commission", "Swap",
    ]

    return agg[col_order].sort_values("Login").reset_index(drop=True)


def compute_group_summary(report: pd.DataFrame) -> pd.DataFrame:
    """Group-level aggregation."""
    if report.empty:
        return pd.DataFrame()

    return (
        report.groupby("Group", as_index=False)
        .agg(
            Accounts=("Login", "nunique"),
            Closed_Lots=("Closed Lots", "sum"),
            Volume_USD=("Volume USD", "sum"),
            NET_PNL_USD=("NET PNL USD", "sum"),
            Total_Trades=("Total Trades", "sum"),
        )
        .sort_values("NET_PNL_USD", ascending=False)
        .reset_index(drop=True)
    )


def compute_symbol_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Symbol-level aggregation for instrument breakdown."""
    if df.empty:
        return pd.DataFrame()

    return (
        df.groupby("Symbol", as_index=False)
        .agg(
            Trades=("PnL", "count"),
            Closed_Lots=("Volume", "sum"),
            NET_PNL_USD=("PnL", "sum"),
        )
        .sort_values("NET_PNL_USD", ascending=False)
        .reset_index(drop=True)
    )


def get_top_gainers(report: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """Top N accounts by highest PnL."""
    return report.sort_values("NET PNL USD", ascending=False).head(n).reset_index(drop=True)


def get_top_losers(report: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """Top N accounts by lowest PnL."""
    return report.sort_values("NET PNL USD", ascending=True).head(n).reset_index(drop=True)


def compute_kpis(report: pd.DataFrame) -> Dict:
    """Calculate headline KPI values."""
    if report.empty:
        return {
            "total_clients": 0,
            "total_pnl": 0.0,
            "total_volume": 0.0,
            "total_trades": 0,
            "total_lots": 0.0,
            "total_profit": 0.0,
            "total_loss": 0.0,
            "avg_hit_ratio": 0.0,
        }

    return {
        "total_clients": int(report["Login"].nunique()),
        "total_pnl": float(report["NET PNL USD"].sum()),
        "total_volume": float(report["Volume USD"].sum()),
        "total_trades": int(report["Total Trades"].sum()),
        "total_lots": float(report["Closed Lots"].sum()),
        "total_profit": float(report.loc[report["NET PNL USD"] > 0, "NET PNL USD"].sum()),
        "total_loss": float(report.loc[report["NET PNL USD"] < 0, "NET PNL USD"].sum()),
        "avg_hit_ratio": float(report["Hit Ratio %"].mean()),
    }


# ══════════════════════════════════════════════════════════════
# EQUITY-BASED P&L REPORT
# ══════════════════════════════════════════════════════════════

def compute_equity_report(
    oe_dicts: List[Dict],
    ce_dicts: List[Dict],
    summary_dicts: List[Dict],
    group_map: Dict[int, str],
    account_filter: Optional[List[int]] = None,
) -> pd.DataFrame:
    """
    Equity-based P&L report.

    Formula:
        Net P&L = Closing Equity − Opening Equity − Net D/W − Net Credit − Bonus

    Parameters:
        oe_dicts       – daily records for Opening Equity date
        ce_dicts       – daily records for Closing Equity date
        summary_dicts  – daily records spanning oe_date+1 → ce_date
                         (for D/W, credit, bonus aggregation)
        group_map      – {login: group_string}
        account_filter – if provided, only include these logins
    """
    def _to_df(dicts):
        if not dicts:
            return pd.DataFrame()
        df = pd.DataFrame(dicts)
        df["Login"] = pd.to_numeric(df["Login"], errors="coerce").astype("Int64")
        return df

    oe_df = _to_df(oe_dicts)
    ce_df = _to_df(ce_dicts)
    sm_df = _to_df(summary_dicts)

    # Collect all logins present in any dataset
    all_logins: set = set()
    for df in (oe_df, ce_df, sm_df):
        if not df.empty and "Login" in df.columns:
            all_logins.update(df["Login"].dropna().astype(int).tolist())

    if account_filter is not None:
        all_logins = all_logins.intersection(set(account_filter))

    if not all_logins:
        return pd.DataFrame()

    # Pre-index for fast lookup
    oe_idx = oe_df.set_index("Login") if (not oe_df.empty and "Login" in oe_df.columns) else None
    ce_idx = ce_df.set_index("Login") if (not ce_df.empty and "Login" in ce_df.columns) else None
    sm_grp = sm_df.groupby("Login") if (not sm_df.empty and "Login" in sm_df.columns) else None

    rows = []
    for login in sorted(all_logins):
        # Opening Equity
        oe_val = 0.0
        if oe_idx is not None and login in oe_idx.index:
            rec = oe_idx.loc[login]
            oe_val = float(rec["ProfitEquity"].iloc[-1] if isinstance(rec, pd.DataFrame) else rec["ProfitEquity"])

        # Closing Equity
        ce_val = 0.0
        if ce_idx is not None and login in ce_idx.index:
            rec = ce_idx.loc[login]
            ce_val = float(rec["ProfitEquity"].iloc[-1] if isinstance(rec, pd.DataFrame) else rec["ProfitEquity"])

        # Rule: negative equity → zero out OE, CE and Net P&L
        negative_equity = oe_val < 0 or ce_val < 0
        if negative_equity:
            oe_val = 0.0
            ce_val = 0.0

        # Aggregations over summary period
        deposits = withdrawals = credit_in = credit_out = bonus = 0.0
        if sm_grp is not None and login in sm_grp.groups:
            sm_rec = sm_grp.get_group(login)
            dbal = sm_rec["DailyBalance"]
            deposits     = float(dbal[dbal > 0].sum())
            withdrawals  = float(dbal[dbal < 0].sum())
            dcred = sm_rec["DailyCredit"]
            credit_in    = float(dcred[dcred > 0].sum())
            credit_out   = float(dcred[dcred < 0].sum())
            bonus        = float(sm_rec["DailyBonus"].sum())

        net_dw     = deposits + withdrawals
        net_credit = credit_in + credit_out
        difference = ce_val - oe_val
        net_pnl    = difference - net_dw - net_credit - bonus
        if negative_equity:
            net_pnl = 0.0

        rows.append({
            "Login":          login,
            "Group":          group_map.get(login, ""),
            "Opening Equity": oe_val,
            "Closing Equity": ce_val,
            "Difference":     difference,
            "Deposits":       deposits,
            "Withdrawals":    withdrawals,
            "Net D/W":        net_dw,
            "Credit In":      credit_in,
            "Credit Out":     credit_out,
            "Net Credit":     net_credit,
            "Bonus":          bonus,
            "Net P&L":        net_pnl,
        })

    return pd.DataFrame(rows)


def compute_equity_group_summary(eq_report: pd.DataFrame) -> pd.DataFrame:
    """Group-level aggregation for equity report."""
    if eq_report.empty:
        return pd.DataFrame()

    return (
        eq_report.groupby("Group", as_index=False)
        .agg(
            Accounts=("Login", "nunique"),
            Opening_Equity=("Opening Equity", "sum"),
            Closing_Equity=("Closing Equity", "sum"),
            Net_DW=("Net D/W", "sum"),
            Net_Credit=("Net Credit", "sum"),
            Bonus=("Bonus", "sum"),
            Net_PnL=("Net P&L", "sum"),
        )
        .sort_values("Net_PnL", ascending=False)
        .reset_index(drop=True)
    )


def compute_equity_kpis(eq_report: pd.DataFrame) -> Dict:
    """Headline KPIs for equity report."""
    if eq_report.empty:
        return {
            "total_accounts": 0,
            "total_oe": 0.0,
            "total_ce": 0.0,
            "total_net_dw": 0.0,
            "total_net_credit": 0.0,
            "total_bonus": 0.0,
            "total_net_pnl": 0.0,
            "profitable_accounts": 0,
            "losing_accounts": 0,
        }

    return {
        "total_accounts":     int(eq_report["Login"].nunique()),
        "total_oe":           float(eq_report["Opening Equity"].sum()),
        "total_ce":           float(eq_report["Closing Equity"].sum()),
        "total_net_dw":       float(eq_report["Net D/W"].sum()),
        "total_net_credit":   float(eq_report["Net Credit"].sum()),
        "total_bonus":        float(eq_report["Bonus"].sum()),
        "total_net_pnl":      float(eq_report["Net P&L"].sum()),
        "profitable_accounts": int((eq_report["Net P&L"] > 0).sum()),
        "losing_accounts":    int((eq_report["Net P&L"] < 0).sum()),
    }
