.PHONY: ingest-gbfs ingest-gtfs ingest-viz ingest-gtfs-static ingest silver gold test lint

ingest-gbfs:
	ENV=local python -m multitudcsd.ingestion.gbfs

ingest-gtfs:
	ENV=local python -m multitudcsd.ingestion.gtfs_rt

ingest-viz:
	ENV=local python -m multitudcsd.ingestion.viz

ingest-gtfs-static:
	ENV=local python -m multitudcsd.ingestion.gtfs_static

ingest: ingest-gbfs ingest-gtfs ingest-viz

silver:
	ENV=local python -m multitudcsd.transforms.bronze_to_silver

gold:
	ENV=local python -m multitudcsd.transforms.silver_to_gold

test:
	ENV=local pytest -v

lint:
	ruff check src tests