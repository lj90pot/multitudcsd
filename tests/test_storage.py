"""Tests de la capa de almacenamiento Delta."""

#Imports
import pytest

#Funciones
from multitudcsd.storage import (
    add_ingest_metadata,
    get_table_path,
    read_delta,
    write_bronze,
    write_bronze_snapshot,
    write_silver,
    write_gold
)


def test_ruta_de_tabla(monkeypatch, tmp_path):
    monkeypatch.setenv("ENV", "local")
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))
    esperada = f"{tmp_path.resolve().as_posix()}/bronze/bronze_test"
    assert get_table_path("bronze", "bronze_test") == esperada

def test_capa_invalida(monkeypatch, tmp_path):
    monkeypatch.setenv("ENV", "local")
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))
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


def test_bronze_acumula_entre_ejecuciones(spark, tmp_path, monkeypatch):
    """Bronze es append-only: dos ingestas del mismo feed se suman"""
    monkeypatch.setenv("ENV", "local")
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))
    df = spark.createDataFrame([("a",), ("b",)], ["campo"])

    write_bronze(df, "bronze_acumula", source="real")
    write_bronze(df, "bronze_acumula", source="real")

    assert read_delta(spark, "bronze", "bronze_acumula").count() == 4


def test_bronze_snapshot_sobrescribe_la_foto_anterior(spark, tmp_path, monkeypatch):
    """El GTFS estatico es una foto completa: cada descarga sobrescribe a la anterior."""
    monkeypatch.setenv("ENV", "local")
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))
    foto_vieja = spark.createDataFrame([("a",), ("b",)], ["campo"])
    foto_nueva = spark.createDataFrame([("c",)], ["campo"])

    write_bronze_snapshot(foto_vieja, "bronze_foto", source="real")
    write_bronze_snapshot(foto_nueva, "bronze_foto", source="real")

    leido = read_delta(spark, "bronze", "bronze_foto")
    assert leido.count() == 1
    assert "ingest_date" in leido.columns  # el snapshot con metadatos de ingesta


def test_escritura_gold_sobrescribe(spark, tmp_path, monkeypatch):
    monkeypatch.setenv("ENV", "local")
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))
    write_gold(spark.createDataFrame([("a",)], ["campo"]), "gold_prueba")
    write_gold(spark.createDataFrame([("b",), ("c",)], ["campo"]), "gold_prueba")

    assert read_delta(spark, "gold", "gold_prueba").count() == 2


def test_gold_admite_un_esquema_distinto_al_anterior(spark, tmp_path, monkeypatch):
    """overwriteSchema"""
    monkeypatch.setenv("ENV", "local")
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))
    write_gold(spark.createDataFrame([("a",)], ["campo"]), "gold_esquema")
    write_gold(spark.createDataFrame([("a", 1)], ["campo", "num"]), "gold_esquema")

    assert "num" in read_delta(spark, "gold", "gold_esquema").columns