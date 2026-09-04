"""Fixtures compartidas por todos los tests."""

import json
from pathlib import Path

import pytest

from multitudcsd.config import get_spark_session

DIRECTORIO_FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def spark():
    """SparkSession local, compartida por toda la sesion de tests (arrancarla es lento)."""
    sesion = get_spark_session("tests")
    yield sesion
    sesion.stop()


@pytest.fixture
def gbfs_status_payload() -> dict:
    """Payload de ejemplo del feed station_status de Nextbike."""
    with open(DIRECTORIO_FIXTURES / "gbfs_station_status_sample.json", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def gbfs_discovery_payload() -> dict:
    """Payload de ejemplo del fichero de descubrimiento GBFS."""
    with open(DIRECTORIO_FIXTURES / "gbfs_discovery_sample.json", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def gtfs_rt_bytes() -> bytes:
    """Feed GTFS-RT de ejemplo en formato protobuf."""
    return (DIRECTORIO_FIXTURES / "gtfs_rt_sample.pb").read_bytes()