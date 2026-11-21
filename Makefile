# Shortcuts for jp-stats-elt development tasks
#
# Usage:
#   make setup       # create .venv with uv and install dependencies
#   make lint        # lint with ruff
#   make format      # format with ruff format
#   make test        # run pytest
#   make up          # start the Docker stack
#   make down        # stop the Docker stack
#   make clean       # remove the virtualenv and logs
#
# Resolving Airflow's dependencies requires the official constraints file.
# Keep the Python and Airflow versions in sync with pyproject.toml and the Dockerfile.

AIRFLOW_VERSION := 2.9.3
PYTHON_VERSION := 3.11
CONSTRAINT_URL := https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-$(PYTHON_VERSION).txt

.PHONY: setup lint format test up down logs clean help

help:
	@echo "Targets: setup | lint | format | test | up | down | logs | clean"

# Create a virtualenv with uv and install dependencies.
# This project is an application, not a distributable package, so the project
# itself is not editable-installed; only its dependencies are.
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

# Pin dependencies into the lock file (run and commit after every pyproject.toml change)
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


	