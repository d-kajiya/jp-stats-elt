# Airflow + dbt-core + Great Expectations + e-Stat 連携用クライアント
#
# 設計判断:
#  - Airflow 2.9.x 系 + Python 3.11（dbt-core 1.8 とのバージョン整合）
#  - dbt は Airflow と同居させ BashOperator で起動する（Cosmos は Phase 2 で検討）
#  - constraints ファイルで Airflow 関連依存を固定し、再現性を担保

FROM apache/airflow:2.9.3-python3.11

USER root

# OSレベルの依存（dbt のコンパイルで稀に必要 + ヘルスチェック用 curl）
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Airflow と整合する constraints を使って追加パッケージをインストール
ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# dbt は constraints の対象外（独自バージョンで管理）
RUN pip install --no-cache-dir \
        "dbt-core==1.8.*" \
        "dbt-postgres==1.8.*"

# Airflow 拡張 + データ取得・検証ライブラリ
RUN pip install --no-cache-dir \
        --constraint "${CONSTRAINT_URL}" \
        "apache-airflow-providers-postgres" \
        "great-expectations==0.18.*" \
        "requests" \
        "pandas" \
        "pytest"

# protobuf: dbt-core 1.8 は protobuf>=5.0,<6.0 を要求。
# Airflow constraints は protobuf を 4.25.3 に固定するため、constrained install の
# 後で 5.29.6 に明示上書きする（順序が重要。前段で入れても後段 constraint が引き下げる）。
RUN pip install --no-cache-dir "protobuf==5.29.6"

# dbt プロジェクトと DAG はボリュームマウントで持ち込む（イメージには含めない）
ENV DBT_PROFILES_DIR=/opt/airflow/dbt
ENV DBT_PROJECT_DIR=/opt/airflow/dbt
