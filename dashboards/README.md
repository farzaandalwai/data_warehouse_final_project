# Dashboard Design Plan

This document outlines the planned dashboard layout for the Wholesale Electricity Market
Stress and Price Spike Forecasting project. Dashboards will be built in Tableau or Power BI,
connecting to Snowflake via the `DASHBOARD_READONLY_ROLE` service account.

**Primary data source:** `ELECTRICITY_MARKET_DB.MART.MART_PRICE_SPIKE_DASHBOARD`

---

## 1. Market Overview

**Purpose:** Provide a high-level summary of current and recent CAISO market conditions
across all three trading hubs (NP15, SP15, ZP26).

**Key visuals:**
- Hourly LMP trend line for each trading hub (last 7 days)
- Current LMP vs. 95th percentile threshold (gauge or KPI card)
- Daily average LMP by trading hub (bar chart)
- LMP component breakdown: energy vs. congestion vs. loss (stacked area)
- Data freshness indicator (last loaded timestamp)

**Filters:** Trading hub, date range, market type (RTM / DAM)

---

## 2. Stress Monitor

**Purpose:** Surface active and historical price spike events to support real-time
market monitoring and post-event analysis.

**Key visuals:**
- Spike event calendar heatmap (hour × day, colour = stress severity)
- Price spike flag time series with severity colour coding (NORMAL / ELEVATED / SPIKE / EXTREME)
- Top 10 highest LMP hours (table with hub, timestamp, LMP, severity)
- Congestion component contribution during spike events (scatter plot)
- Demand vs. net generation during spike hours (dual-axis line chart)
- Spike frequency by hour of day and day of week (heatmap)

**Filters:** Trading hub, date range, stress severity level, is_weekend

---

## 3. Forecasting / Model Output

**Purpose:** Display model predictions and evaluate forecast accuracy once the
ML forecasting pipeline is integrated into the MART layer.

**Key visuals:**
- Predicted vs. actual LMP (line chart, next 24 hours)
- Spike probability score by hour (bar or waterfall chart)
- Feature importance summary (horizontal bar chart from model artefacts)
- Forecast error distribution (histogram: actual − predicted LMP)
- Lag feature trend panel: lmp_lag_1h, lmp_lag_24h, lmp_lag_168h overlaid

**Note:** This section will be populated after the scikit-learn classification
model is trained and integrated into the MART.ML schema output.

---

## 4. Recommendations

**Purpose:** Translate analytical findings into actionable insights for energy
procurement, risk management, and operations teams.

**Key visuals / narrative sections:**
- Summary of recurring stress patterns (peak hours, seasonal trends, high-risk hubs)
- Correlation between demand forecast error and spike events
- Cost impact estimate: average cost premium during spike vs. non-spike hours
- Suggested hedging windows based on historical spike frequency
- Highlighted anomalies or notable events from the analysis period

**Note:** This tab will be narrative-heavy, combining text annotations with
supporting charts. Updated monthly as part of the reporting cycle.
