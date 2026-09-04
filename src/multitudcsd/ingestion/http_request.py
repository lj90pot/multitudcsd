"""Cliente HTTP con reintentos. El proyecto conecta a los servidores de los servicios de transporte."""

import json
import time

import requests

TIMEOUT_SEGUNDOS = 60
MAX_INTENTOS = 2
ESPERA_ENTRE_INTENTOS = 30
#Esto hay que añadirlo porque vbb detecta si eres una maquina
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


def download_bytes(url: str) -> bytes:
    """Descarga el contenido de una URL como bytes"""
    # se descargan bytes y asi tengo puedo parsear los datos con el formato que vengan
    # ejmeplo, json, protobuff, xml...
    ultimo_error = None
    headers = {"User-Agent": USER_AGENT}

    for intento in range(1, MAX_INTENTOS + 1):
        try:
            respuesta = requests.get(url, headers=headers, timeout=TIMEOUT_SEGUNDOS)
            respuesta.raise_for_status()
            print(f"[http] OK {url} ({len(respuesta.content)} bytes)")
            return respuesta.content
        except requests.RequestException as error:
            ultimo_error = error
            print(f"[http] intento {intento}/{MAX_INTENTOS} fallido en {url}: {error}")
            if intento < MAX_INTENTOS:
                time.sleep(ESPERA_ENTRE_INTENTOS)
    raise RuntimeError(f"No se pudo descargar {url} tras {MAX_INTENTOS} intentos") from ultimo_error


def download_json(url: str) -> dict:
    """Descarga una URL y devuelve el JSON ya parseado."""
    return json.loads(download_bytes(url).decode("utf-8"))