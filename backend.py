"""
MT5 P&L Studio — FastAPI Backend
---------------------------------
Exposes MT5 data over HTTP so the Streamlit frontend can run anywhere
(Streamlit Cloud) while this backend runs on a Windows machine with MT5Manager.

Run locally:
  uvicorn backend:app --host 0.0.0.0 --port 8000

On Render (Windows VPS / Docker-Windows only — MT5Manager is Windows-only):
  Start command: uvicorn backend:app --host 0.0.0.0 --port $PORT
"""

import os
from datetime import datetime
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from mt5_connector import (
    connect_mt5,
    disconnect_mt5,
    get_deals,
    get_users,
    deals_to_dicts,
    get_daily_reports,
    daily_to_dicts,
)

app = FastAPI(title="MT5 P&L Studio API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────────────────────
# Request / Response Models
# ─────────────────────────────────────────────────────────────

class MT5Credentials(BaseModel):
    server: str
    login: int
    password: str


class DealsRequest(MT5Credentials):
    from_date: str   # ISO format: "2024-01-01T00:00:00"
    to_date: str


class DailyRequest(MT5Credentials):
    from_date: str
    to_date: str


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _parse_dt(s: str) -> datetime:
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    raise ValueError(f"Cannot parse date: {s}")


def _connect(server, login, password):
    manager, err = connect_mt5(server, login, password)
    if err:
        raise HTTPException(status_code=400, detail=err)
    return manager


# ─────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "mt5manager_available": _check_mt5manager()}


def _check_mt5manager():
    try:
        import MT5Manager
        return True
    except ImportError:
        return False


@app.post("/deals")
def fetch_deals(req: DealsRequest):
    """
    Returns list of deal dicts (Login, Symbol, Volume, Profit, Commission, Swap, PnL, Group, Time).
    Each request connects → fetches → disconnects (stateless).
    """
    from_dt = _parse_dt(req.from_date)
    to_dt   = _parse_dt(req.to_date)

    manager = _connect(req.server, req.login, req.password)
    try:
        deals_raw, err = get_deals(manager, from_dt, to_dt)
        if err:
            raise HTTPException(status_code=500, detail=err)

        logins = list({int(d["Login"]) for d in deals_raw} if deals_raw and isinstance(deals_raw[0], dict)
                      else set())
        # numpy records — extract logins differently
        if deals_raw and not isinstance(deals_raw[0], dict):
            from mt5_connector import _field
            logins = list({int(_field(d, "Login", "login")) for d in deals_raw
                           if _field(d, "Login", "login") is not None})

        user_map, _ = get_users(manager, logins)
        rows = deals_to_dicts(deals_raw, user_map)
    finally:
        disconnect_mt5(manager)

    # Convert any numpy types to Python native for JSON serialisation
    clean = []
    for row in rows:
        clean.append({k: (int(v) if hasattr(v, "item") else v) for k, v in row.items()})

    return {"count": len(clean), "deals": clean}


@app.post("/daily-reports")
def fetch_daily_reports(req: DailyRequest):
    """
    Returns list of daily equity snapshot dicts
    (Login, Datetime, ProfitEquity, Balance, DailyBalance, DailyCredit, DailyBonus).
    """
    from_dt = _parse_dt(req.from_date)
    to_dt   = _parse_dt(req.to_date)

    manager = _connect(req.server, req.login, req.password)
    try:
        records_raw, err = get_daily_reports(manager, from_dt, to_dt)
        if err:
            raise HTTPException(status_code=500, detail=err)
        rows = daily_to_dicts(records_raw)
    finally:
        disconnect_mt5(manager)

    clean = []
    for row in rows:
        clean.append({k: (int(v) if hasattr(v, "item") else v) for k, v in row.items()})

    return {"count": len(clean), "daily_reports": clean}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("backend:app", host="0.0.0.0", port=port, reload=False)
