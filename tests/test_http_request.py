"""Tests de http_request. Ningun test toca la red"""

#Imports
import pytest
import requests

from multitudcsd.ingestion import http_request

#Funciones
class RespuestaFalsa:
    """Imita requests.Response que usa download_bytes."""

    def __init__(self, contenido: bytes):
        self.content = contenido

    def raise_for_status(self) -> None:
        return None


def test_descarga_correcta_devuelve_los_bytes(monkeypatch):
    monkeypatch.setattr(requests, "get", lambda url, **kwargs: RespuestaFalsa(b"hola"))
    assert http_request.download_bytes("http://ejemplo") == b"hola"


def test_se_manda_user_agent(monkeypatch):
    """VBB rechaza las peticiones sin User-Agent: si se pierde la cabecera, cae la ingesta."""
    cabeceras_enviadas = {}

    def get_falso(url, **kwargs):
        cabeceras_enviadas.update(kwargs["headers"])
        return RespuestaFalsa(b"ok")

    monkeypatch.setattr(requests, "get", get_falso)
    http_request.download_bytes("http://ejemplo")

    assert "User-Agent" in cabeceras_enviadas


def test_reintenta_y_acaba_saliendo_bien(monkeypatch):
    intentos = {"n": 0}

    def get_falso(url, **kwargs):
        intentos["n"] += 1
        if intentos["n"] == 1:
            raise requests.ConnectionError("caido")
        return RespuestaFalsa(b"ok")

    monkeypatch.setattr(requests, "get", get_falso)
    monkeypatch.setattr(http_request.time, "sleep", lambda segundos: None)

    assert http_request.download_bytes("http://ejemplo") == b"ok"
    assert intentos["n"] == 2


def test_al_agotar_los_intentos_lanza_runtimeerror(monkeypatch):
    def get_falso(url, **kwargs):
        raise requests.ConnectionError("caido")

    monkeypatch.setattr(requests, "get", get_falso)
    monkeypatch.setattr(http_request.time, "sleep", lambda segundos: None)

    with pytest.raises(RuntimeError):
        http_request.download_bytes("http://ejemplo")


def test_download_json_parsea_el_contenido(monkeypatch):
    monkeypatch.setattr(
        requests, "get", lambda url, **kwargs: RespuestaFalsa(b'{"clave": 1}')
    )
    assert http_request.download_json("http://ejemplo") == {"clave": 1}