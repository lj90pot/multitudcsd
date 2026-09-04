"""Tests de la decodificacion del feed GTFS-RT de VBB."""

import json

from multitudcsd.ingestion.gtfs_rt import decode_feed, extract_trip_updates


def test_el_feed_de_ejemplo_se_decodifica(gtfs_rt_bytes):
    feed = decode_feed(gtfs_rt_bytes)
    assert len(feed.entity) > 0


def test_solo_se_extraen_trip_updates(gtfs_rt_bytes):
    feed = decode_feed(gtfs_rt_bytes)
    filas = extract_trip_updates(feed, "http://ejemplo")
    entidades_trip_update = [e for e in feed.entity if e.HasField("trip_update")]
    assert len(filas) == len(entidades_trip_update)


def test_el_payload_es_json_valido(gtfs_rt_bytes):
    feed = decode_feed(gtfs_rt_bytes)
    filas = extract_trip_updates(feed, "http://ejemplo")
    assert "trip" in json.loads(filas[0]["payload_json"])