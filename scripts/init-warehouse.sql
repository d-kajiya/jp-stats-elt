-- Runs on the PostgreSQL container's first start (and in CI).
-- Creates the analytical warehouse database, separate from Airflow's metadata DB.

CREATE DATABASE warehouse;

\c warehouse

-- One schema per layer, following dbt convention
CREATE SCHEMA IF NOT EXISTS raw;          -- raw data fetched from e-Stat
CREATE SCHEMA IF NOT EXISTS staging;      -- dbt staging models
CREATE SCHEMA IF NOT EXISTS intermediate; -- dbt intermediate models
CREATE SCHEMA IF NOT EXISTS marts;        -- dbt marts models

-- Grant the airflow user access to every schema
GRANT ALL PRIVILEGES ON DATABASE warehouse TO airflow;
GRANT ALL ON SCHEMA raw, staging, intermediate, marts TO airflow;

-- Raw CPI table in the raw schema
CREATE TABLE IF NOT EXISTS raw.cpi (
    tab_code      TEXT        NOT NULL,   -- tabulation item (@tab)
    area_code     TEXT        NOT NULL,   -- area (@area)
    category_code TEXT        NOT NULL,   -- item category (@cat01)
    time_code     TEXT        NOT NULL,   -- time axis (@time)
    value         NUMERIC,                -- numeric value; NULL for missing cells
    value_raw     TEXT,                   -- original text, e.g. "-" or "***"
    unit          TEXT,                   -- unit (@unit)
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_raw_cpi PRIMARY KEY (tab_code, area_code, category_code, time_code)
);