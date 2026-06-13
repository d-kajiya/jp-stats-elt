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

-- raw スキーマに CPI 生データ用テーブル
CREATE TABLE IF NOT EXISTS raw.cpi (
    tab_code      TEXT        NOT NULL,   -- 表章事項（@tab）
    area_code     TEXT        NOT NULL,   -- 地域（@area）
    category_code TEXT        NOT NULL,   -- 品目分類（@cat01）
    time_code     TEXT        NOT NULL,   -- 時間軸（@time）
    value         NUMERIC,                -- 数値。欠損セルは NULL
    value_raw     TEXT,                   -- "-" や "***" 等の原文を保持
    unit          TEXT,                   -- 単位（@unit）
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_raw_cpi PRIMARY KEY (tab_code, area_code, category_code, time_code)
);