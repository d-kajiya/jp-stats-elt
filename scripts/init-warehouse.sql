-- PostgreSQL コンテナ初回起動時に実行される
-- Airflow メタDB とは別に、分析用の warehouse データベースを作成する

CREATE DATABASE warehouse;

\c warehouse

-- レイヤごとにスキーマを分離（dbt の慣習に合わせる）
CREATE SCHEMA IF NOT EXISTS raw;          -- e-Stat から取得した生データ
CREATE SCHEMA IF NOT EXISTS staging;      -- dbt staging モデル
CREATE SCHEMA IF NOT EXISTS intermediate; -- dbt intermediate モデル
CREATE SCHEMA IF NOT EXISTS marts;        -- dbt marts モデル

-- airflow ユーザーに全スキーマへの権限を付与
GRANT ALL PRIVILEGES ON DATABASE warehouse TO airflow;
GRANT ALL ON SCHEMA raw, staging, intermediate, marts TO airflow;
