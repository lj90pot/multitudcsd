"""Tests de la capa de almacenamiento Delta."""

import pytest

from multitudcsd.storage import add_ingest_metadata, get_table_path, read_delta, write_bronze


def test_ruta_de_tabla(monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_ROOT", "/tmp/lh")
    assert get_table_path("bronze", "bronze_test") == "/tmp/lh/bronze/bronze_test"


def test_capa_invalida():
    with pytest.raises(ValueError):
        get_table_path("platino", "tabla")


def test_metadatos_de_ingesta(spark):
    df = spark.createDataFrame([("a",)], ["campo"])
    resultado = add_ingest_metadata(df, "real").collect()[0]
    assert resultado["source"] == "real"
    assert resultado["ingest_ts"] is not None


def test_origen_invalido(spark):
    df = spark.createDataFrame([("a",)], ["campo"])
    with pytest.raises(ValueError):
        add_ingest_metadata(df, "inventado")


def test_escritura_y_lectura_en_delta(spark, tmp_path, monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))
    df = spark.createDataFrame([("estacion_1",), ("estacion_2",)], ["station_id"])

    write_bronze(df, "bronze_prueba", source="real")
    leido = read_delta(spark, "bronze", "bronze_prueba")

    assert leido.count() == 2
    assert "ingest_date" in leido.columns

def test_escritura_silver_sobrescribe(spark, tmp_path, monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))
    df_v1 = spark.createDataFrame([("a",)], ["campo"])
    df_v2 = spark.createDataFrame([("b",), ("c",)], ["campo"])

    write_silver(df_v1, "silver_prueba")
    write_silver(df_v2, "silver_prueba")  # debe sustituir, no acumular

    leido = read_delta(spark, "silver", "silver_prueba")
    assert leido.count() == 2