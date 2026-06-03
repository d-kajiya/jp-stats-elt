# jp-stats-elt 開発タスクのショートカット
#
# 使い方:
#   make setup       # uv で .venv を作って依存をインストール
#   make lint        # ruff によるリント
#   make format      # ruff format によるフォーマット
#   make test        # pytest 実行
#   make up          # Docker スタック起動
#   make down        # Docker スタック停止
#   make clean       # 仮想環境とログを削除
#
# Airflow の依存解決は constraints ファイルが必須。
# Python と Airflow のバージョンは pyproject.toml / Dockerfile と一致させること。

AIRFLOW_VERSION := 2.9.3
PYTHON_VERSION := 3.11
CONSTRAINT_URL := https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-$(PYTHON_VERSION).txt

.PHONY: setup lint format test up down logs clean help

help:
	@echo "Targets: setup | lint | format | test | up | down | logs | clean"

# uv で仮想環境を作り、依存をインストール
# このプロジェクトは「アプリケーション」であり配布パッケージではないため、
# プロジェクト自体は editable install せず、依存ライブラリのみをインストールする。
setup:
	uv venv
	@if [ -f requirements.lock ]; then \
		echo "→ Installing from requirements.lock (reproducible)"; \
		uv pip install --requirement requirements.lock; \
	else \
		echo "→ requirements.lock not found, resolving from constraints"; \
		uv pip compile pyproject.toml --constraint "$(CONSTRAINT_URL)" --extra dev --output-file /tmp/req.txt --quiet && uv pip install --requirement /tmp/req.txt; \
	fi
	@echo ""
	@echo "✓ Done. Activate with: source .venv/bin/activate"

# 依存をロックファイルに固定（pyproject.toml を変更したら必ず実行してコミット）
lock:
	uv pip compile pyproject.toml \
		--constraint "$(CONSTRAINT_URL)" \
		--extra dev \
		-o requirements.lock
	@echo "✓ requirements.lock updated. Commit it."

lint:
	uv run ruff check .

format:
	uv run ruff format .
	uv run ruff check --fix .

test:
	uv run pytest

# ---------- Docker ----------

up:
	docker compose up -d
	@echo "Airflow UI: http://localhost:8080 (admin/admin)"

down:
	docker compose down

down-volumes:
	docker compose down -v

logs:
	docker compose logs -f --tail=100

build:
	docker compose build

# ---------- cleanup ----------

clean:
	rm -rf .venv .pytest_cache .ruff_cache .mypy_cache
	find . -type d -name __pycache__ -exec rm -rf {} +

init-dirs:
	mkdir -p airflow/logs/scheduler airflow/logs/dag_processor_manager
	sudo chown -R 50000:0 airflow/logs


	