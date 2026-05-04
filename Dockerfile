FROM apache/airflow:2.10.5-python3.11

# Install project dependencies as the airflow user.
# The official Airflow image already has pip configured for the airflow user,
# so we do not need to switch to root for pure Python package installs.
# snowflake-connector-python[pandas] is a superset of the base connector
# and pulls in the write_pandas utility used by load_to_snowflake.py.
RUN pip install --no-cache-dir \
    pandas \
    requests \
    "snowflake-connector-python[pandas]" \
    python-dotenv \
    scikit-learn \
    dbt-core \
    dbt-snowflake \
    mlflow
