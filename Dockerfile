# Airflow + dbt-core + Great Expectations + e-Stat client libraries
#
# Design decisions:
#  - Airflow 2.9.x + Python 3.11 (compatible with dbt-core 1.8)
#  - dbt is co-located with Airflow and invoked via BashOperator
#    (Cosmos is listed under Future improvements in the README)
#  - Airflow-related dependencies are pinned with the constraints file for reproducibility

FROM apache/airflow:2.9.3-python3.11

USER root

# OS-level dependencies (occasionally needed to build dbt deps, plus curl for health checks)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install extra packages against the constraints that match this Airflow version
ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# dbt is outside the constraints file (versioned independently)
RUN pip install --no-cache-dir \
        "dbt-core==1.8.*" \
        "dbt-postgres==1.8.*"

# Airflow providers + data extraction and validation libraries
RUN pip install --no-cache-dir \
        --constraint "${CONSTRAINT_URL}" \
        "apache-airflow-providers-postgres" \
        "great-expectations==0.18.*" \
        "requests" \
        "pandas" \
        "pytest"

# protobuf: dbt-core 1.8 requires protobuf>=5.0,<6.0, while Airflow's constraints pin
# it to 4.25.3. Override it to 5.29.6 *after* the constrained install. Order matters:
# installed any earlier, a later constrained step pulls it back down.
RUN pip install --no-cache-dir "protobuf==5.29.6"

# The dbt project and DAGs are bind-mounted, not baked into the image
ENV DBT_PROFILES_DIR=/opt/airflow/dbt
ENV DBT_PROJECT_DIR=/opt/airflow/dbt
