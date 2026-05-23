.PHONY: install \
        run-ohlcv-daily-ingest run-spark-technical run-flink-alert run-volatility-burst \
        run-digest run-screener \
        test test-unit

PYTHON     := .venv/bin/python
SPARK_EXEC := docker exec -e PYTHONPATH=/opt/project spark-master \
              /opt/spark/bin/spark-submit \
              --master spark://spark-master:7077 \
              --conf spark.executorEnv.PYTHONPATH=/opt/project \
              /opt/project/main.py

install: ## Create venv and install job dependencies (needed for tests only)
	python3.12 -m venv .venv
	.venv/bin/pip install -r requirements.txt

run-ohlcv-daily-ingest: ## Spark: derive OHLCV bars (optional: DATE=YYYY-MM-DD)
	$(SPARK_EXEC) ohlcv-daily-ingest $(if $(DATE),--date $(DATE),)

run-spark-technical: ## Spark: compute SMA/RSI/MACD/Bollinger Bands
	$(SPARK_EXEC) technical

run-digest: ## Spark: compute daily market digest (optional: DATE=YYYY-MM-DD)
	$(SPARK_EXEC) digest $(if $(DATE),--date $(DATE),)

run-screener: ## Spark: run weekly fundamental screener (optional: DATE=YYYY-MM-DD)
	$(SPARK_EXEC) screener $(if $(DATE),--date $(DATE),)

run-flink-alert: ## Submit Flink price alert job to the Docker cluster
	docker exec flink-jobmanager flink run \
	  --python /opt/project/main.py \
	  -- flink-alert

run-volatility-burst: ## Submit Flink volatility burst detection job to the Docker cluster
	docker exec flink-jobmanager flink run \
	  --python /opt/project/main.py \
	  -- volatility-burst

test: ## Run all tests
	PYTHONPATH=. $(PYTHON) -m pytest

test-unit: ## Run unit tests only (no Docker needed)
	PYTHONPATH=. $(PYTHON) -m pytest -m unit
