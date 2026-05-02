# Final Report Outline
## Wholesale Electricity Market Stress and Price Spike Forecasting

---

## Abstract

A brief (200–300 word) summary of the project, the problem addressed, the approach taken,
and the key findings. Written last, after all analysis is complete.

---

## 1. Introduction

- Background on wholesale electricity markets and price volatility
- Motivation for building an automated analytics and forecasting system
- Overview of the California ISO (CAISO) market as the primary focus
- High-level description of the end-to-end pipeline built in this project
- Report structure overview

---

## 2. Problem Statement

- Definition of price spikes in the wholesale electricity market context
- Financial and operational risks posed by undetected or unforecasted spikes
- Limitations of manual monitoring and reactive decision-making
- Research questions this project aims to answer:
  - Which hours and hubs are most susceptible to price spikes?
  - Can short-term LMP forecasting be achieved with lag features and basic ML?
  - How does grid stress (demand-supply gap) correlate with price spikes?

---

## 3. Dataset Description

### 3.1 CAISO OASIS — 5-Minute LMP Data
- Source URL and API details
- Fields available: interval, hub, LMP, energy/congestion/loss components
- Date range covered
- Volume and coverage statistics (rows, hubs, interval counts)
- Known data quality issues (missing intervals, outliers)

### 3.2 EIA Open Data API — Hourly Grid Operations
- Source URL and API details
- Fields available: demand, forecast, generation, interchange
- Balancing authorities included
- Date range covered
- Notes on data lag and revision policy

---

## 4. System Architecture

- Architecture diagram (data sources → Airflow → Snowflake RAW → dbt → MART → dashboard)
- Rationale for each technology choice
- Data flow narrative: how data moves from API to dashboard
- Environment and deployment notes

---

## 5. Snowflake Data Warehouse Design

- Database and schema layout (RAW, STAGING, INTERMEDIATE, MART, ML, ADHOC)
- Table definitions for RAW.EIA_HOURLY_OPS and RAW.CAISO_LMP_5MIN
- Role-based access control design (DASHBOARD_READONLY_ROLE)
- Naming conventions and column standards

---

## 6. Airflow ETL Pipeline

- DAG overview: caiso_lmp_etl_dag, eia_hourly_etl_dag, dbt_run_dag
- Task design and dependency graph
- Scheduling rationale (time offsets between ETL and dbt DAGs)
- Error handling, retry logic, and alerting strategy
- Lessons learned and pipeline reliability notes

---

## 7. dbt ELT Pipeline

- dbt project structure (staging → intermediate → marts)
- Staging layer: source definitions, cleaning logic, hourly aggregation
- Intermediate layer: join logic, feature engineering approach
- Mart layer: spike classification methodology, dashboard model design
- dbt tests and data quality checks implemented
- Model run times and performance observations

---

## 8. Feature Engineering

- Lag features: rationale for 1h, 24h, and 168h lookback periods
- Calendar features: hour of day, day of week, is_weekend, month
- Grid stress features: demand-supply gap, forecast error
- LMP component features: congestion and loss as stress proxies
- Correlation analysis: which features show the strongest relationship with spikes
- Feature importance findings from the ML model

---

## 9. Forecasting / Classification Methodology

- Problem framing: binary classification (spike / no spike) vs. regression (LMP value)
- Model selection rationale (e.g. logistic regression, random forest, gradient boosting)
- Training and validation dataset construction
- Train/test split strategy (time-based split to prevent data leakage)
- Evaluation metrics: precision, recall, F1, AUC-ROC
- Baseline comparison (naive model: previous hour LMP > threshold)
- Model results and performance summary
- Limitations of the current modelling approach

---

## 10. Dashboard

- Dashboard overview and target audience
- Section-by-section walkthrough (Market Overview, Stress Monitor, Forecasting, Recommendations)
- Key insights surfaced by the dashboard
- Screenshots or embedded visuals (to be added)
- User feedback and iteration notes

---

## 11. Analysis and Recommendations

- Summary of price spike patterns discovered (peak hours, seasonal effects, hub differences)
- Correlation between grid stress indicators and spike events
- Highest-risk trading hub and time windows identified
- Recommended hedging or procurement strategies based on findings
- Suggested monitoring thresholds for operational use
- Next steps for improving forecast accuracy

---

## 12. Limitations

- CAISO OASIS API data latency and gaps
- EIA data revision policy (values may change post-publication)
- Static 95th percentile threshold does not adapt to market regime changes
- XCom size limits in Airflow for large DataFrames (serialisation overhead)
- Model trained on historical data only — regime shifts may degrade performance
- Dashboard refresh frequency limited by Airflow schedule cadence
- Scope limited to CAISO; results may not generalise to other ISOs

---

## 13. Conclusion

- Summary of what was built and what was learned
- Degree to which the research questions were answered
- Business value delivered by the system
- Recommendations for future enhancements:
  - Expand to additional ISOs (ERCOT, PJM, MISO)
  - Add weather data as a feature
  - Implement a rolling threshold for spike detection
  - Deploy a real-time inference endpoint
  - Automate report generation

---

## 14. References

- EIA Open Data API documentation
- CAISO OASIS API documentation and user guide
- Snowflake documentation
- Apache Airflow documentation
- dbt documentation
- Relevant academic papers or industry reports on electricity price forecasting
- Python package documentation (pandas, scikit-learn, snowflake-connector-python)
