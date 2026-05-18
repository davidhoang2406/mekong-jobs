.PHONY: install \
        run-ohlcv-daily-ingest run-spark-technical run-flink-alert \
        test test-unit

PYTHON := .venv/bin/python

install: ## Create venv and install job dependencies
	python3.12 -m venv .venv
	.venv/bin/pip install -r requirements.txt

run-ohlcv-daily-ingest: ## Spark: derive OHLCV bars (optional: DATE=YYYY-MM-DD)
	PYTHONPATH=. $(PYTHON) main.py ohlcv-daily-ingest $(if $(DATE),--date $(DATE),)

run-spark-technical: ## Spark: compute SMA/RSI/MACD/Bollinger Bands
	PYTHONPATH=. $(PYTHON) main.py technical

run-flink-alert: ## Submit Flink price alert job to the Docker cluster
	docker exec flink-jobmanager flink run \
	  --python /opt/project/main.py \
	  -- flink-alert

test: ## Run all tests
	PYTHONPATH=. $(PYTHON) -m pytest

test-unit: ## Run unit tests only (no Docker needed)
	PYTHONPATH=. $(PYTHON) -m pytest -m unit
