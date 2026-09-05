"""Comprueba que la configuracion resuelve rutas distintas segun el entorno."""

from multitudcsd.config import get_environment, get_lakehouse_root


def test_entorno_local_por_defecto(monkeypatch):
    monkeypatch.delenv("ENV", raising=False)
    assert get_environment() == "local"


def test_raiz_local_es_absoluta_y_apunta_al_lakehouse(monkeypatch):
    monkeypatch.setenv("ENV", "local")
    monkeypatch.delenv("LAKEHOUSE_ROOT", raising=False)
    assert get_lakehouse_root().endswith("data/lakehouse")


def test_databricks_sin_variable_de_entorno_falla(monkeypatch):
    monkeypatch.setenv("ENV", "databricks")
    monkeypatch.delenv("LAKEHOUSE_ROOT", raising=False)
    with pytest.raises(RuntimeError):
        get_lakehouse_root()

def test_databricks_usa_la_ruta_del_cluster(monkeypatch):
    monkeypatch.setenv("ENV", "databricks")
    monkeypatch.setenv("LAKEHOUSE_ROOT", "/Volumes/cat/esq/vol/lakehouse")
    assert get_lakehouse_root() == "/Volumes/cat/esq/vol/lakehouse"

def test_la_raiz_se_puede_sobrescribir(monkeypatch):
    monkeypatch.setenv("ENV", "local")
    monkeypatch.setenv("LAKEHOUSE_ROOT", "/tmp/otro_sitio")
    assert get_lakehouse_root() == "/tmp/otro_sitio"