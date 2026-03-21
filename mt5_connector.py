"""
MT5 Manager API Connector
─────────────────────────
C++ SDK signatures (from MT5APIManager.h):

  DealRequest(login, from, to, IMTDealArray*)
  DealRequestByGroup(group, from, to, IMTDealArray*)
  UserGet(login, IMTUser*)
  UserGetByLogins / UserRequestByLogins(logins, total, IMTUserArray*)

Python NumPy variants = same args MINUS the output array param,
and they RETURN the numpy array instead:

  DealRequestByGroupNumPy("*", from_ts, to_ts)  → numpy array (all groups)
  UserGetByLoginsNumPy([login, ...])             → numpy array
"""

import calendar
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any


def _to_unix(dt: datetime) -> int:
    """datetime → UTC Unix timestamp integer."""
    return int(calendar.timegm(dt.timetuple()))


def _field(record, *names, default=None):
    """
    Read a field from a numpy void record or attribute-style object.
    Tries each name as provided, then lowercase.
    """
    for name in names:
        for variant in (name, name.lower()):
            try:
                val = record[variant]
                if val is not None:
                    return val
            except (KeyError, ValueError, TypeError, IndexError):
                pass
            try:
                val = getattr(record, variant, None)
                if val is not None:
                    return val
            except Exception:
                pass
    return default


# ─────────────────────────────────────────────────────────────
# CONNECTION
# ─────────────────────────────────────────────────────────────

def connect_mt5(server: str, login: int, password: str) -> Tuple[Any, Optional[str]]:
    """Connect to MT5 Manager API. Returns (manager, None) or (None, error)."""
    try:
        import MT5Manager
        manager = MT5Manager.ManagerAPI()
        ret = manager.Connect(
            server, int(login), password,
            MT5Manager.ManagerAPI.EnPumpModes.PUMP_MODE_USERS,
            300000,
        )
        # ret == 0 (MT_RET_OK) or True → success
        if ret not in (0, True):
            return None, f"Connection failed (code {ret}) – check server, login, or password."
        return manager, None
    except ImportError:
        return None, "MT5Manager SDK not found. Ensure it is installed on this machine."
    except Exception as e:
        return None, f"Connection error: {e}"


def disconnect_mt5(manager: Any) -> None:
    try:
        if manager is not None:
            manager.Disconnect()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
# DEALS
# ─────────────────────────────────────────────────────────────

def get_deals(
    manager: Any, from_date: datetime, to_date: datetime
) -> Tuple[List, Optional[str]]:
    """
    Fetch all deals for the date range.

    Uses DealRequestByGroupNumPy("*", from_ts, to_ts):
      - group "*" = all groups
      - Python NumPy variant returns numpy array directly (no output-array param)
      - MT_RET_OK = 0  (success)
      - MT_RET_OK_NONE = 1 (success, no data)
    """
    from_ts = _to_unix(from_date)
    to_ts   = _to_unix(to_date)

    try:
        result = manager.DealRequestByGroupNumPy("*", from_ts, to_ts)
    except Exception as e:
        return [], f"DealRequestByGroupNumPy error: {e}"

    # A bool return means wrong args or retcode — handle gracefully
    if isinstance(result, (bool, int)):
        if result in (0, True):
            return [], None   # MT_RET_OK but no data
        return [], f"DealRequestByGroupNumPy returned code: {result}"

    if result is None or (hasattr(result, "__len__") and len(result) == 0):
        return [], None

    return list(result), None


# ─────────────────────────────────────────────────────────────
# USERS
# ─────────────────────────────────────────────────────────────

def get_users(
    manager: Any, logins: Optional[List[int]] = None
) -> Tuple[Dict[int, str], Optional[str]]:
    """
    Build {login: group_string} map.
    Uses UserGetByLoginsNumPy([logins]) → numpy array with Login + Group fields.
    Falls back to UserGet(login) per-login if batch call fails.
    """
    if not logins:
        return {}, None

    user_map: Dict[int, str] = {}

    # ── Attempt 1: batch numpy call ──
    try:
        result = manager.UserGetByLoginsNumPy(logins)
        if result is not None and not isinstance(result, (bool, int)):
            for row in result:
                lgn   = _field(row, "Login",  "login")
                group = _field(row, "Group",  "group", default="")
                if lgn is not None:
                    user_map[int(lgn)] = str(group or "")
            return user_map, None
    except Exception:
        pass

    # ── Attempt 2: per-login UserGet ──
    for lgn in logins:
        try:
            user = manager.UserGet(int(lgn))
            if user is None or isinstance(user, (bool, int)):
                user_map[int(lgn)] = ""
            else:
                group = _field(user, "Group", "group", default="")
                user_map[int(lgn)] = str(group or "")
        except Exception:
            user_map[int(lgn)] = ""

    return user_map, None


# ─────────────────────────────────────────────────────────────
# NORMALISE DEALS → DICTS
# ─────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────
# DAILY REPORTS (equity snapshots)
# ─────────────────────────────────────────────────────────────

def get_daily_reports(
    manager: Any, from_date: datetime, to_date: datetime
) -> Tuple[List, Optional[str]]:
    """
    Fetch MT5 Daily Report records for the date range.

    Tries known method name variants in order; falls back to introspection
    if none match so the caller can see the real available names.

    Each record contains per-login daily equity snapshots:
      ProfitEquity  – end-of-day equity (balance + floating)
      Balance       – settled balance
      DailyBalance  – net deposits/withdrawals for that day
      DailyCredit   – net credit change for that day
      DailyBonus    – bonus added that day
    """
    from_ts = _to_unix(from_date)
    to_ts   = _to_unix(to_date)

    # Candidate method names to try (all-groups NumPy variant)
    candidates = [
        "DailyRequestByGroupNumPy",
        "DailyRequestNumPy",
        "DailyGetByGroupNumPy",
        "DailyGetNumPy",
        "DailyRequestByGroup",
        "DailyRequest",
    ]

    for name in candidates:
        if not hasattr(manager, name):
            continue
        try:
            method = getattr(manager, name)
            # Try with group wildcard + timestamps first
            try:
                result = method("*", from_ts, to_ts)
            except TypeError:
                # Maybe no group arg — try timestamps only
                result = method(from_ts, to_ts)
        except Exception as e:
            return [], f"{name} error: {e}"

        if isinstance(result, (bool, int)):
            if result in (0, True):
                return [], None
            return [], f"{name} returned code: {result}"

        if result is None or (hasattr(result, "__len__") and len(result) == 0):
            return [], None

        return list(result), None

    # None of the candidates exist — introspect and report available daily methods
    all_attrs = [a for a in dir(manager) if "daily" in a.lower() or "Daily" in a]
    if all_attrs:
        return [], (
            "No known Daily method found. Available daily-related methods: "
            + ", ".join(all_attrs)
        )
    return [], (
        "No Daily methods found on the MT5Manager object. "
        "This server may not support Daily Reports via the Manager API."
    )


def daily_to_dicts(records: List) -> List[Dict]:
    """Convert daily report numpy records to plain dicts for pandas."""
    rows = []
    for r in records:
        login = _field(r, "Login", "login")
        if login is None:
            continue
        rows.append({
            "Login":        int(login),
            "Datetime":     _field(r, "Datetime", "datetime"),
            "ProfitEquity": float(_field(r, "ProfitEquity", "profitequity", default=0) or 0),
            "Balance":      float(_field(r, "Balance",      "balance",      default=0) or 0),
            "DailyBalance": float(_field(r, "DailyBalance", "dailybalance", default=0) or 0),
            "DailyCredit":  float(_field(r, "DailyCredit",  "dailycredit",  default=0) or 0),
            "DailyBonus":   float(_field(r, "DailyBonus",   "dailybonus",   default=0) or 0),
        })
    return rows


def deals_to_dicts(deals: List, user_map: Dict[int, str]) -> List[Dict]:
    """
    Convert list of numpy records to plain dicts for pandas.

    Action filter: 0=BUY ✅  1=SELL ✅  else ❌
    PnL = Profit + Commission + Storage (swap)
    Volume: MT5 integer units ÷ 10 000 → standard lots
    """
    rows = []
    for d in deals:
        login = _field(d, "Login", "login")
        if login is None:
            continue

        action = _field(d, "Action", "action")
        if action is not None and int(action) not in (0, 1):
            continue

        raw_vol    = float(_field(d, "Volume",     "volume",     default=0) or 0)
        profit     = float(_field(d, "Profit",     "profit",     default=0) or 0)
        commission = float(_field(d, "Commission", "commission", default=0) or 0)
        storage    = float(_field(d, "Storage",    "storage",    default=0) or 0)
        symbol     = str(_field(d,  "Symbol",      "symbol",     default="") or "")

        volume = raw_vol / 10_000.0
        pnl    = profit + commission + storage
        group  = user_map.get(int(login), "")

        rows.append({
            "Login":      int(login),
            "Symbol":     symbol,
            "Volume":     volume,
            "Profit":     profit,
            "Commission": commission,
            "Swap":       storage,
            "PnL":        pnl,
            "Group":      group,
            "Time":       _field(d, "Time", "time"),
        })
    return rows
