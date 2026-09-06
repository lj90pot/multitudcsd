"""Tests del parseo del feed GBFS de Nextbike."""

#Imports
import json

import pytest

from multitudcsd.ingestion.gbfs import find_feed_url, parse_station_status

#Funciones
def test_encuentra_la_url_del_feed(gbfs_discovery_payload):
    url = find_feed_url(gbfs_discovery_payload, "station_status")
    assert url.startswith("http")


def test_feed_inexistente_lanza_error(gbfs_discovery_payload):
    with pytest.raises(ValueError):
        find_feed_url(gbfs_discovery_payload, "feed_que_no_existe")


def test_parseo_devuelve_una_fila_por_estacion(gbfs_status_payload):
    filas = parse_station_status(gbfs_status_payload, "http://ejemplo")
    assert len(filas) == len(gbfs_status_payload["data"]["stations"])
    assert set(filas[0]) == {"station_id", "payload_json", "source_url", "feed_last_updated"}


def test_el_payload_guardado_sigue_siendo_json_valido(gbfs_status_payload):
    filas = parse_station_status(gbfs_status_payload, "http://ejemplo")
    recuperado = json.loads(filas[0]["payload_json"])
    assert "station_id" in recuperado


def test_estacion_sin_identificador_no_rompe():
    payload = {"last_updated": 1, "data": {"stations": [{"num_bikes_available": 3}]}}
    filas = parse_station_status(payload, "http://ejemplo")
    assert filas[0]["station_id"] == ""

def test_encuentra_la_url_de_station_information(gbfs_discovery_payload):
    url = find_feed_url(gbfs_discovery_payload, "station_information")
    assert url.startswith("http")