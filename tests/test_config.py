"""Comprueba que la configuracion resuelve rutas distintas segun el entorno."""

from multitudcsd.config import get_environment, get_lakehouse_root


def test_entorno_local_por_defecto(monkeypatch):
    monkeypatch.delenv("ENV", raising=False)
    assert get_environment() == "local"


def test_raiz_local(monkeypatch):
    monkeypatch.setenv("ENV", "local")
    monkeypatch.delenv("LAKEHOUSE_ROOT", raising=False)
    assert get_lakehouse_root().startswith("./data")


def test_raiz_databricks(monkeypatch):
    monkeypatch.setenv("ENV", "databricks")
    monkeypatch.delenv("LAKEHOUSE_ROOT", raising=False)
    assert get_lakehouse_root().startswith("dbfs:/")


def test_la_raiz_se_puede_sobrescribir(monkeypatch):
    monkeypatch.setenv("ENV", "local")
    monkeypatch.setenv("LAKEHOUSE_ROOT", "/tmp/otro_sitio")
    assert get_lakehouse_root() == "/tmp/otro_sitio"