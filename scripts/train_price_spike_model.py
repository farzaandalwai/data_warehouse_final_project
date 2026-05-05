"""
train_price_spike_model.py
--------------------------
Trains a logistic regression model to predict whether a CAISO trading hub
will experience a price spike in the next hour.

Source  : ELECTRICITY_MARKET_DB.MART.MART_HOURLY_MARKET_STRESS
Outputs : ELECTRICITY_MARKET_DB.ML.PRICE_SPIKE_PREDICTIONS
          ELECTRICITY_MARKET_DB.ML.PRICE_SPIKE_MODEL_METRICS
Tracking: MLflow experiment 'electricity_market_price_spike'
"""

import os
from datetime import datetime, timezone

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE_TABLE   = "ELECTRICITY_MARKET_DB.MART.MART_HOURLY_MARKET_STRESS"
PRED_TABLE     = "PRICE_SPIKE_PREDICTIONS"
METRICS_TABLE  = "PRICE_SPIKE_MODEL_METRICS"
ML_SCHEMA      = "ML"
ML_DATABASE    = "ELECTRICITY_MARKET_DB"

MLFLOW_URI     = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5001")
EXPERIMENT     = "electricity_market_price_spike"

NUMERIC_CANDIDATES = [
    "LMP_AVG",
    "LMP_LAG_1H",
    "LMP_LAG_24H",
    "LMP_LAG_168H",
    "LMP_STDDEV",
    "DEMAND_MWH",
    "DEMAND_FORECAST_MWH",
    "NET_GENERATION_MWH",
    "INTERCHANGE_MWH",
    "HOUR_OF_DAY",
    "DAY_OF_WEEK",
    "IS_WEEKEND",
]

CATEGORICAL_CANDIDATES = ["TRADING_HUB"]

# Timestamp columns that must be stringified before write_pandas
_TS_COLS = ["HOUR_UTC", "PREDICTED_AT", "TRAINED_AT"]


# ---------------------------------------------------------------------------
# Snowflake helpers
# ---------------------------------------------------------------------------

def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    account   = os.environ["SNOWFLAKE_ACCOUNT"]
    user      = os.environ["SNOWFLAKE_USER"]
    # Docker Compose escapes "$" as "$$" in env_file values; unescape here so
    # the connector receives the literal password regardless of runtime.
    password  = os.getenv("SNOWFLAKE_PASSWORD", "")
    if password:
        password = password.replace("$$", "$")
    role      = os.environ.get("SNOWFLAKE_ROLE", "")
    warehouse = os.environ["SNOWFLAKE_WAREHOUSE"]
    database  = os.environ["SNOWFLAKE_DATABASE"]

    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        role=role,
        warehouse=warehouse,
        database=database,
    )
    cur = conn.cursor()
    cur.execute(f'USE WAREHOUSE "{warehouse}"')
    cur.execute(f'USE DATABASE "{database}"')
    cur.close()
    print(f"[Snowflake] Connected as {user} @ {account}")
    return conn


def remove_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect and remove duplicate column names, keeping the first occurrence.
    Prints a warning listing any duplicates found.
    """
    dupes = df.columns[df.columns.duplicated()].tolist()
    if dupes:
        print(f"[Warning] Duplicate columns detected and removed: {dupes}")
        df = df.loc[:, ~df.columns.duplicated()]
    return df


def read_training_data(conn: snowflake.connector.SnowflakeConnection) -> pd.DataFrame:
    sql = f"""
        SELECT *
        FROM {SOURCE_TABLE}
        ORDER BY TRADING_HUB, HOUR_UTC
    """
    cur = conn.cursor()
    cur.execute(sql)
    cols = [desc[0].upper() for desc in cur.description]
    rows = cur.fetchall()
    cur.close()
    df = pd.DataFrame(rows, columns=cols)
    df = remove_duplicate_columns(df)
    print(f"[Data] Read {len(df):,} rows from {SOURCE_TABLE}")
    return df


def _delete_existing_model_rows(conn: snowflake.connector.SnowflakeConnection,
                                 model_version: str) -> None:
    cur = conn.cursor()
    for table in [PRED_TABLE, METRICS_TABLE]:
        sql = (
            f"DELETE FROM {ML_DATABASE}.{ML_SCHEMA}.{table} "
            f"WHERE MODEL_VERSION = %s"
        )
        cur.execute(sql, (model_version,))
        print(f"[Snowflake] Deleted existing rows for {model_version} from {table}")
    cur.close()


def _prepare_for_write(df: pd.DataFrame) -> pd.DataFrame:
    """Convert timestamp columns to strings so write_pandas loads them correctly."""
    df = df.copy()
    for col in _TS_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d %H:%M:%S")
    return df


def write_ml_table(conn: snowflake.connector.SnowflakeConnection,
                   df: pd.DataFrame,
                   table_name: str) -> None:
    df_out = _prepare_for_write(df)
    df_out.columns = [c.upper() for c in df_out.columns]
    success, nchunks, nrows, _ = write_pandas(
        conn,
        df_out,
        table_name=table_name,
        database=ML_DATABASE,
        schema=ML_SCHEMA,
        auto_create_table=False,
        overwrite=False,
    )
    print(f"[Snowflake] Wrote {nrows:,} rows to {ML_DATABASE}.{ML_SCHEMA}.{table_name}  "
          f"(success={success}, chunks={nchunks})")


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------

def build_features(df: pd.DataFrame):
    """
    Add the next-hour spike target, filter to available features, and
    return (X, y, numeric_features, categorical_features).
    """
    # Uppercase all column names for consistent access
    df.columns = [c.upper() for c in df.columns]
    df = remove_duplicate_columns(df)

    print(f"[Debug] Duplicate columns: {df.columns[df.columns.duplicated()].tolist()}")

    # Build the target: PRICE_SPIKE_FLAG shifted back one hour per hub
    df = df.sort_values(["TRADING_HUB", "HOUR_UTC"]).copy()
    df["NEXT_HOUR_PRICE_SPIKE_FLAG"] = (
        df.groupby("TRADING_HUB")["PRICE_SPIKE_FLAG"].shift(-1)
    )
    df = df.dropna(subset=["NEXT_HOUR_PRICE_SPIKE_FLAG"]).copy()
    df["NEXT_HOUR_PRICE_SPIKE_FLAG"] = df["NEXT_HOUR_PRICE_SPIKE_FLAG"].astype(int)

    print(f"[Features] Rows after dropping null target: {len(df):,}")

    # Resolve which candidate features are actually present in the DataFrame
    num_features = [c for c in NUMERIC_CANDIDATES    if c in df.columns]
    cat_features = [c for c in CATEGORICAL_CANDIDATES if c in df.columns]

    # Drop numeric features that are entirely NaN — SimpleImputer(strategy='median')
    # cannot compute a median from all-NaN values and passes NaN downstream, which
    # causes StandardScaler to raise a SIGBUS / NaN validation error.
    all_null = [c for c in num_features if df[c].isna().all()]
    if all_null:
        print(f"[Features] Dropping all-NaN numeric columns (no usable values): {all_null}")
        num_features = [c for c in num_features if c not in all_null]

    print(f"[Features] Numeric  features used : {num_features}")
    print(f"[Features] Categoric features used: {cat_features}")

    # Snowflake returns numeric columns as Python decimal.Decimal objects.
    # Coerce all numeric features to float64 before sklearn sees them;
    # non-convertible values (e.g. Decimal NaN) become np.nan.
    for col in num_features:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    # Fill remaining NaN after coercion (partial-NaN columns like lag features)
    df[num_features] = df[num_features].fillna(df[num_features].median())
    df[cat_features] = df[cat_features].fillna("UNKNOWN")

    # Guard: minimum row count
    if len(df) < 50:
        print(f"[WARNING] Only {len(df)} rows available — model may be unreliable.")

    target_counts = df["NEXT_HOUR_PRICE_SPIKE_FLAG"].value_counts()
    print(f"[Features] Target distribution:\n{target_counts.to_string()}")

    if target_counts.shape[0] < 2:
        raise ValueError(
            "Target column NEXT_HOUR_PRICE_SPIKE_FLAG has only one class. "
            "Need both 0 and 1 to train a binary classifier. "
            "Load more data covering both spike and non-spike hours."
        )

    # Build X including metadata passthrough columns; avoid duplicating any column
    # that is already in num_features or cat_features (e.g. LMP_AVG, DEMAND_MWH,
    # TRADING_HUB are common to both lists — adding them again would cause sklearn
    # ColumnTransformer to raise "columns are not unique in dataframe").
    already_selected = set(num_features + cat_features)
    meta_extra = [
        c for c in ["HOUR_UTC", "TRADING_HUB", "LMP_AVG", "DEMAND_MWH"]
        if c not in already_selected and c in df.columns
    ]
    X = df[num_features + cat_features + meta_extra]
    y = df["NEXT_HOUR_PRICE_SPIKE_FLAG"]

    return X, y, num_features, cat_features


# ---------------------------------------------------------------------------
# Main training function
# ---------------------------------------------------------------------------

def train_and_log_model() -> None:
    print("=" * 65)
    print("Price Spike Prediction — Logistic Regression training")
    print(f"Started : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    conn = get_snowflake_connection()
    raw_df = read_training_data(conn)

    X_full, y_full, num_features, cat_features = build_features(raw_df)

    feature_cols = num_features + cat_features
    X_model = X_full[feature_cols]

    # Pull metadata for the prediction output from X_full (columns are guaranteed
    # present because build_features guards with `if c in df.columns`).
    meta_cols = [c for c in ["HOUR_UTC", "TRADING_HUB", "LMP_AVG", "DEMAND_MWH"]
                 if c in X_full.columns]

    # Decide whether stratification is feasible
    min_class_count = y_full.value_counts().min()
    stratify_arg = y_full if min_class_count >= 2 else None
    if stratify_arg is None:
        print("[Split] Skipping stratify — a class has fewer than 2 rows.")

    X_train, X_test, y_train, y_test = train_test_split(
        X_model, y_full,
        test_size=0.25,
        random_state=42,
        stratify=stratify_arg,
    )

    # Matching metadata rows for the test set
    meta_test = X_full.loc[X_test.index, meta_cols].copy()

    print(f"[Split] Train rows: {len(X_train):,}  |  Test rows: {len(X_test):,}")

    # ------------------------------------------------------------------
    # Build sklearn pipeline
    # ------------------------------------------------------------------
    # Numeric pipeline: impute first (handles columns that are fully/mostly NaN,
    # e.g. lag features with insufficient history), then scale.
    numeric_pipeline = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler",  StandardScaler()),
    ])

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_pipeline, num_features),
            ("cat", OneHotEncoder(handle_unknown="ignore"), cat_features),
        ],
        remainder="drop",
    )

    pipeline = Pipeline(steps=[
        ("preprocessor", preprocessor),
        ("classifier",   LogisticRegression(max_iter=1000, class_weight="balanced")),
    ])

    print(f"[Debug] X_train shape: {X_train.shape}, dtypes:\n{X_train.dtypes.to_string()}")
    print(f"[Debug] NaN in X_train: {X_train.isna().sum().sum()}")
    print(f"[Debug] NaN in X_test : {X_test.isna().sum().sum()}")

    pipeline.fit(X_train, y_train)
    print("[Model] Training complete.")

    # ------------------------------------------------------------------
    # Evaluate
    # ------------------------------------------------------------------
    y_pred  = pipeline.predict(X_test)
    y_proba = pipeline.predict_proba(X_test)[:, 1]

    accuracy  = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, zero_division=0)
    recall    = recall_score(y_test, y_pred, zero_division=0)
    f1        = f1_score(y_test, y_pred, zero_division=0)

    test_classes = np.unique(y_test)
    roc_auc = roc_auc_score(y_test, y_proba) if len(test_classes) == 2 else None

    print("\n[Metrics]")
    print(f"  Accuracy  : {accuracy:.4f}")
    print(f"  Precision : {precision:.4f}")
    print(f"  Recall    : {recall:.4f}")
    print(f"  F1        : {f1:.4f}")
    print(f"  ROC-AUC   : {roc_auc:.4f}" if roc_auc is not None else "  ROC-AUC  : N/A (single class in test set)")

    # ------------------------------------------------------------------
    # MLflow logging (optional — skipped gracefully if server is unavailable)
    # ------------------------------------------------------------------
    model_version = datetime.now(timezone.utc).strftime("price_spike_logreg_%Y%m%d_%H%M%S")

    try:
        mlflow.set_tracking_uri(MLFLOW_URI)
        mlflow.set_experiment(EXPERIMENT)

        with mlflow.start_run(run_name=model_version):
            mlflow.log_param("model_type",            "LogisticRegression")
            mlflow.log_param("numeric_features",      str(num_features))
            mlflow.log_param("categorical_features",  str(cat_features))
            mlflow.log_param("row_count",             len(X_full))

            mlflow.log_metric("accuracy",  accuracy)
            mlflow.log_metric("precision", precision)
            mlflow.log_metric("recall",    recall)
            mlflow.log_metric("f1_score",  f1)
            if roc_auc is not None:
                mlflow.log_metric("roc_auc", roc_auc)

            mlflow.sklearn.log_model(pipeline, artifact_path="model")
            print(f"[MLflow] Run logged: experiment='{EXPERIMENT}', run='{model_version}'")
    except Exception as mlflow_err:
        print(f"[MLflow] WARNING: Could not log to MLflow ({MLFLOW_URI}): {mlflow_err}")
        print("[MLflow] Continuing — Snowflake writes will still proceed.")

    # ------------------------------------------------------------------
    # Build DataFrames to write to Snowflake
    # ------------------------------------------------------------------
    predicted_at = datetime.now(timezone.utc).replace(tzinfo=None)

    predictions_df = pd.DataFrame({
        "HOUR_UTC":                     meta_test["HOUR_UTC"].values,
        "TRADING_HUB":                  meta_test["TRADING_HUB"].values,
        "ACTUAL_PRICE_SPIKE_FLAG":      y_test.values,
        "PREDICTED_PRICE_SPIKE_FLAG":   y_pred,
        "PREDICTED_SPIKE_PROBABILITY":  y_proba,
        "LMP_AVG":                      meta_test["LMP_AVG"].values,
        "DEMAND_MWH":                   meta_test["DEMAND_MWH"].values,
        "MODEL_VERSION":                model_version,
        "PREDICTED_AT":                 predicted_at,
    })

    metrics_df = pd.DataFrame([{
        "MODEL_VERSION":      model_version,
        "TRAINING_ROW_COUNT": len(X_train),
        "TEST_ROW_COUNT":     len(X_test),
        "ACCURACY":           accuracy,
        "PRECISION_SCORE":    precision,
        "RECALL_SCORE":       recall,
        "F1_SCORE":           f1,
        "ROC_AUC":            roc_auc,
        "TRAINED_AT":         predicted_at,
    }])

    # ------------------------------------------------------------------
    # Write to Snowflake (idempotent: delete first)
    # ------------------------------------------------------------------
    _delete_existing_model_rows(conn, model_version)
    write_ml_table(conn, predictions_df, PRED_TABLE)
    write_ml_table(conn, metrics_df,     METRICS_TABLE)

    conn.close()

    print("\n" + "=" * 65)
    print(f"Model version : {model_version}")
    print(f"Predictions   : {len(predictions_df):,} rows → ML.{PRED_TABLE}")
    print(f"Metrics       : {len(metrics_df)} row  → ML.{METRICS_TABLE}")
    print("Training complete.")
    print("=" * 65)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    train_and_log_model()
