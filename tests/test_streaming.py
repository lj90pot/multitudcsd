"""Tests de la ingesta incremental de menciones con Structured Streaming.

Es el modulo de test mas lento de la suite: cada test arranca una StreamingQuery real.
No toca la red ni ningun broker: el origen son ficheros JSON Lines en un directorio
temporal
"""

# Imports

import pytest

from multitudcsd.storage import read_delta
from multitudcsd.streaming.mentions_stream import (
    ESQUEMA_MENCION,
    TABLA_BRONZE,
    ingest_mentions_stream,
)
from multitudcsd.synthetic.mentions import generate_landing_files


# Funciones

@pytest.mark.slow
def test_el_stream_lee_los_ficheros_de_landing(spark, tmp_path, monkeypatch):
    """Dos lotes en landing acaban como filas en Bronze, con sus metadatos de ingesta."""
    monkeypatch.setenv("ENV", "local")
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))

    generate_landing_files(num_lotes=2, menciones_por_lote=10)
    ingest_mentions_stream(spark)

    bronze = read_delta(spark, "bronze", TABLA_BRONZE)

    assert bronze.count() == 20
    assert bronze.filter("source = 'synthetic'").count() == 20
    assert "ingest_date" in bronze.columns


@pytest.mark.slow
def test_la_segunda_pasada_solo_procesa_los_ficheros_nuevos(spark, tmp_path, monkeypatch):
    """La lectura incremental: el checkpoint evita reprocesar lo ya leido.
    """
    monkeypatch.setenv("ENV", "local")
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))

    generate_landing_files(num_lotes=2, menciones_por_lote=10)
    ingest_mentions_stream(spark)
    filas_primera_pasada = read_delta(spark, "bronze", TABLA_BRONZE).count()

    # Un lote mas en landing; los dos primeros ficheros ya estan en el checkpoint.
    generate_landing_files(num_lotes=3, menciones_por_lote=10)
    ingest_mentions_stream(spark)
    filas_segunda_pasada = read_delta(spark, "bronze", TABLA_BRONZE).count()

    assert filas_primera_pasada == 20
    assert filas_segunda_pasada == 30  # no 50: los lotes 1 y 2 no se releen


@pytest.mark.slow
def test_una_mencion_mal_formada_no_tumba_el_stream(spark, tmp_path, monkeypatch):
    """Con esquema explicito, un campo corrupto llega como nulo en vez de romper el job.
    """
    monkeypatch.setenv("ENV", "local")
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))

    carpeta_landing = tmp_path / "landing" / "mentions"
    carpeta_landing.mkdir(parents=True)
    (carpeta_landing / "mentions_9999.json").write_text(
        '{"mention_id": "men_999999", "lat": "no-es-un-numero"}\n',
        encoding="utf-8",
    )

    ingest_mentions_stream(spark)

    bronze = read_delta(spark, "bronze", TABLA_BRONZE)
    fila = bronze.collect()[0]

    assert bronze.count() == 1
    assert fila["mention_id"] == "men_999999"
    assert fila["lat"] is None


def test_el_esquema_del_stream_coincide_con_el_del_generador():
    """El esquema explicito es el contrato de Bronze: si el generador cambia, falla aqui.
    """
    campos_del_esquema = {campo.name for campo in ESQUEMA_MENCION.fields}

    assert campos_del_esquema == {
        "mention_id",
        "event_ts",
        "lat",
        "lon",
        "platform",
        "language",
        "sentiment",
        "has_media",
        "user_hash",
    }
