.PHONY: install dev lint test docker clean run
install: ; pip install -e .
dev: ; pip install -e ".[dev]"
lint: ; ruff check src tests
test: ; pytest --cov=firms --cov-report=term-missing
run: ; firms-pipeline --out data/firms.parquet
docker: ; docker build -t firms-pipeline:latest .
clean: ; rm -rf build dist *.egg-info .pytest_cache .ruff_cache data
