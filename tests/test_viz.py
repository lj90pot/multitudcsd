"""Tests de la descarga del feed de cortes de tráfico de VIZ Berlin."""

#Imports
import json

from multitudcsd.ingestion.viz import get_feed_url, parse_disruptions

#Funciones
def test_la_url_se_puede_sobrescribir_por_entorno(monkeypatch):
    monkeypatch.setenv("VIZ_DISRUPTIONS_URL", "https://ejemplo/otro.json")
    assert get_feed_url() == "https://ejemplo/otro.json"


def test_parseo_devuelve_una_fila_por_incidencia(viz_disruptions_payload):
    filas = parse_disruptions(viz_disruptions_payload, "http://ejemplo")
    assert len(filas) == len(viz_disruptions_payload["features"])
    assert set(filas[0]) == {"disruption_id", "payload_json", "source_url"}


def test_el_identificador_se_saca_de_properties(viz_disruptions_payload):
    """El id de VIZ no esta en la raiz del feature, sino dentro de properties."""
    filas = parse_disruptions(viz_disruptions_payload, "http://ejemplo")
    ids_esperados = {
        f["properties"]["id"] for f in viz_disruptions_payload["features"]
    }
    assert {fila["disruption_id"] for fila in filas} == ids_esperados


def test_el_payload_guardado_sigue_siendo_json_valido(viz_disruptions_payload):
    filas = parse_disruptions(viz_disruptions_payload, "http://ejemplo")
    recuperado = json.loads(filas[0]["payload_json"])
    assert "geometry" in recuperado


def test_incidencia_sin_identificador_no_rompe():
    payload = {"features": [{"type": "Feature", "properties": {}, "geometry": None}]}
    filas = parse_disruptions(payload, "http://ejemplo")
    assert filas[0]["disruption_id"] == ""


def test_feed_vacio_devuelve_lista_vacia():
    assert parse_disruptions({}, "http://ejemplo") == []