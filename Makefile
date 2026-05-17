.PHONY: install install-dagster \
        run-ohlcv-daily-ingest run-spark-technical run-flink-alert \
        dagster-up dagster-down dagster-logs \
        test test-unit test-dagster

PYTHON := .venv/bin/python

install: ## Create venv and install job dependencies
	python3.12 -m venv .venv
	.venv/bin/pip install -r requirements.txt

install-dagster: ## Install dagster_project package into venv (run after install)
	.venv/bin/pip install -e dagster/

run-ohlcv-daily-ingest: ## Spark: derive OHLCV bars (optional: DATE=YYYY-MM-DD)
	PYTHONPATH=. $(PYTHON) main.py ohlcv-daily-ingest $(if $(DATE),--date $(DATE),)

run-spark-technical: ## Spark: compute SMA/RSI/MACD/Bollinger Bands
	PYTHONPATH=. $(PYTHON) main.py technical

run-flink-alert: ## Submit Flink price alert job to the Docker cluster
	docker exec flink-jobmanager flink run \
	  --python /opt/project/main.py \
	  -- flink-alert

dagster-up: ## Start Dagster webserver + daemon
	DAGSTER_HOME=$(PWD)/dagster PYTHONPATH=$(PWD):$(PWD)/dagster \
	  dagster dev -f dagster/workspace.yaml

dagster-down: ## Stop Dagster processes
	pkill -f "dagster dev" || true

dagster-logs: ## Tail Dagster daemon logs
	tail -f /tmp/dagster-daemon.log 2>/dev/null || echo "No daemon log found"

test: ## Run all tests
	PYTHONPATH=. $(PYTHON) -m pytest

test-unit: ## Run unit tests only (no Docker needed)
	PYTHONPATH=. $(PYTHON) -m pytest -m unit

test-dagster: ## Run Dagster asset tests (no Docker needed)
	PYTHONPATH=.:dagster $(PYTHON) -m pytest tests/dagster/
