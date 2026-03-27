# MT5 Client P&L Studio – System Logic

---

## Objective

Build a Streamlit-based dashboard that:

- Connects to MT5 Manager API
- Fetches trade data (Deals)
- Calculates client-level P&L
- Displays analytics (KPIs, Top Gainers/Losers, Group Summary)
- Allows export (CSV / Excel)

---

## System Architecture

```
Streamlit UI (app.py)
        |
MT5 Connector (mt5_connector.py)
        |
Data Processing (calculations.py)
        |
Dashboard Output
```

---

## 1. MT5 Connection Logic

### Input Required
- Server (IP:Port)
- Manager Login
- Password

### Connection Method
```python
manager.Connect(
    server,
    login,
    password,
    PUMP_MODE_USERS,
    timeout=300000
)
```

### Output
- Manager session object
- Error handling if connection fails

---

## 2. Data Fetching

### 2.1 Deals (Core Data)

```python
manager.DealsRequest(from_date, to_date)
```

Used for:
- PnL calculation
- Volume
- Trades
- Hit ratio

### 2.2 Users

```python
manager.UsersRequest()
```

Used for:
- Mapping Login → Group

---

## 3. Data Filtering

Inside `deals_to_dicts()` — only include TRADE deals:

| Action | Meaning  | Include |
|--------|----------|---------|
| 0      | BUY      | Yes     |
| 1      | SELL     | Yes     |
| 2      | BALANCE  | No      |
| 6      | CREDIT   | No      |

---

## 4. Core Calculation Logic

### Main Formula (Mandatory)

```
PnL = Profit + Commission + Swap
```

Where:
- Profit → trade result
- Commission → broker fee
- Swap → overnight charge

---

## 5. Data Transformation

### Step 1: Convert MT5 objects to dict

Each deal becomes:

```python
{
    "Login":      ...,
    "Symbol":     ...,
    "Volume":     ...,
    "Profit":     ...,
    "Commission": ...,
    "Swap":       ...,
    "PnL":        ...,
    "Group":      ...,
    "Time":       ...,
}
```

### Step 2: Convert to DataFrame

```python
df = pd.DataFrame(deal_dicts)
```

---

## 6. Client-Level Metrics

For each `Login`:

### Aggregations

```
Closed Lots   = sum(Volume)
NET PNL USD   = sum(PnL)
Total Trades  = count(deals)
Commission    = sum(Commission)
Swap          = sum(Swap)
```

### Volume Calculation

```
Volume USD = Closed Lots x 100,000
```

(Standard Forex Lot)

### Win / Loss

```
Wins   = count(PnL > 0)
Losses = count(PnL < 0)
```

### Hit Ratio

```
Hit Ratio % = Wins / (Wins + Losses) x 100
```

---

## 7. Group-Level Metrics

Grouped by `Group`:

- Accounts (unique logins)
- Closed Lots
- Volume USD
- NET PNL USD
- Total Trades

---

## 8. Symbol-Level Metrics

Grouped by `Symbol`:

- Trades
- Closed Lots
- NET PNL USD

---

## 9. KPI Metrics

```
Total Clients   = unique logins
Total PnL       = sum(NET PNL USD)
Total Volume    = sum(Volume USD)
Total Trades    = sum(Total Trades)
Total Lots      = sum(Closed Lots)

Total Profit    = sum(PnL > 0)
Total Loss      = sum(PnL < 0)

Avg Hit Ratio   = mean(Hit Ratio %)
```

---

## 10. Top Accounts

### Gainers
```
Sort by NET PNL USD DESC → Top N
```

### Losers
```
Sort by NET PNL USD ASC → Top N
```

---

## 11. UI Logic (Streamlit)

### Inputs
- Server
- Login
- Password
- From Date
- To Date
- Top N selector

### Flow

```
User clicks "Generate Report"
        |
Validate inputs
        |
Connect to MT5
        |
Fetch Deals + Users
        |
Process Data
        |
Display Dashboard
```

---

## 12. Output Sections

1. KPI Overview
2. Profit vs Loss Chart
3. Top Gainers / Losers
4. Group Summary
5. Symbol Breakdown
6. Full Client Table
7. Export Options

---

## 13. Export Logic

### CSV
```python
report.to_csv()
```

### Excel (Multi-sheet)

| Sheet            | Contents             |
|------------------|----------------------|
| Client PnL       | Per-login report     |
| Group Summary    | Group aggregation    |
| Symbol Breakdown | Per-symbol metrics   |

---

## 14. Important Rules

### Do
- Always filter trade deals only (action 0 and 1)
- Always use deal-based PnL
- Always apply date filtering

### Do Not
- Use Opening/Closing equity in deal-based mode
- Use Net DP/WD in deal-based mode
- Mix Excel-based logic with API-based logic
- Include balance operations in PnL

---

## 15. Performance Considerations

- Use `groupby` instead of loops
- Cache data where possible:

```python
@st.cache_data(ttl=60)
def cached_deals(manager, dt_from, dt_to):
    return get_deals(manager, dt_from, dt_to)
```

- Limit date range for large servers

---

## 16. Security

- Do NOT store credentials anywhere
- Use session-only variables
- Mask password input (`type="password"`)

---

## Final Result

System provides:

- Accurate MT5 PnL (deal-based)
- Real-time reporting via API
- Scalable dashboard
- Exportable reports
- Broker-grade analytics

---

## Dependencies

```
streamlit>=1.30.0
pandas>=2.0.0
numpy>=1.24.0
openpyxl>=3.1.0
```

Run:
```bash
pip install -r requirements.txt
streamlit run app.py
```

---

## Summary

This system replaces:

- Manual Excel reporting
- Error-prone reconciliation

With:

- Automated MT5 data extraction
- Accurate deal-based calculations
- Real-time analytics dashboard
