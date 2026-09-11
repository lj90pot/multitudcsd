.PHONY: ingest-gbfs ingest-gtfs ingest-viz ingest-gtfs-static ingest \
        synthetic stream silver gold train predict ml all test lint build clean

ingest-gbfs:
	ENV=local python -m multitudcsd.ingestion.gbfs

ingest-gtfs:
	ENV=local python -m multitudcsd.ingestion.gtfs_rt

ingest-viz:
	ENV=local python -m multitudcsd.ingestion.viz

ingest-gtfs-static:
	ENV=local python -m multitudcsd.ingestion.gtfs_static

ingest:
	ENV=local python -m multitudcsd.orchestration.run_bronze

synthetic:
	ENV=local python -m multitudcsd.synthetic.mentions

stream:
	ENV=local python -m multitudcsd.orchestration.run_stream

silver:
	ENV=local python -m multitudcsd.orchestration.run_silver

gold:
	ENV=local python -m multitudcsd.orchestration.run_gold

train:
	ENV=local python -m multitudcsd.ml.train

predict:
	ENV=local python -m multitudcsd.ml.predict

ml:
	ENV=local python -m multitudcsd.orchestration.run_ml

test:
	ENV=local pytest -v

lint:
	ruff check src tests

build:
	python -m build

clean:
	rm -rf dist build src/*.egg-info