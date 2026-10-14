.PHONY: setup check test db-init backfill baseline demo demo-reset outliers model evaluate-outliers evaluate-model reset prepare-data erd

setup:
	uv sync --all-groups

check: test
	uv run ruff check .
	uv run python scripts/check_names.py --data data/prepared

test:
	uv run python -m unittest discover -s tests

prepare-data:
	uv run python local/prepare.py

db-init:
	uv run python -m saas_stream.db init

backfill:
	uv run python -m saas_stream.backfill

baseline:
	uv run python -m saas_stream.demo baseline

demo:
	uv run python -m saas_stream.demo demo

demo-reset:
	uv run python -m saas_stream.demo reset

outliers:
	uv run python -m saas_stream.outliers

model:
	uv run python -m saas_stream.model

evaluate-outliers:
	uv run python -m saas_stream.evaluate outliers

evaluate-model:
	uv run python -m saas_stream.evaluate model

reset:
	uv run python -m saas_stream.db reset

erd:
	uv run python scripts/erd.py
