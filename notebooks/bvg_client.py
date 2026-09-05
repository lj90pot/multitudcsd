"""Cliente sencillo para la API v6.bvg.transport.rest.

Esta API devuelve JSON (no protobuf) y ya incluye retrasos en tiempo real,
a diferencia del feed GTFS-RT de VBB que se decodifica en ingestion/gtfs_rt.py.

De momento es un script suelto para explorar el payload, NO forma parte
todavia del paquete multitudcsd. Si se decide usarla como fuente real,
se movera a src/multitudcsd/ingestion/bvg.py siguiendo el mismo patron que
gbfs.py y gtfs_rt.py (usar ingestion/http.py para las llamadas
para escribir en Bronze).
"""

import time

import requests

BASE_URL = "https://v6.bvg.transport.rest"
TIMEOUT_SEGUNDOS = 30
MAX_INTENTOS = 3
ESPERA_ENTRE_INTENTOS = 5


def obtener_payload(endpoint: str, params: dict | None = None) -> dict | list:
    """Hace un GET a un endpoint de la API y devuelve el JSON ya parseado.

    endpoint: ruta relativa, por ejemplo "/stops/900100003/departures"
    params: parametros de query, por ejemplo {"duration": 10}

    Reintenta ante fallos transitorios de red (timeout, 5xx), igual que
    ingestion/http.py en el resto del proyecto.
    """
    url = f"{BASE_URL}{endpoint}"
    ultimo_error = None

    for intento in range(1, MAX_INTENTOS + 1):
        try:
            respuesta = requests.get(url, params=params, timeout=TIMEOUT_SEGUNDOS)
            respuesta.raise_for_status()
            print(f"[bvg] OK {respuesta.url}")
            print(respuesta)

            return respuesta.json()
        except requests.RequestException as error:
            ultimo_error = error
            print(f"[bvg] intento {intento}/{MAX_INTENTOS} fallido en {url}: {error}")
            if intento < MAX_INTENTOS:
                time.sleep(ESPERA_ENTRE_INTENTOS)

    raise RuntimeError(f"No se pudo descargar {url} tras {MAX_INTENTOS} intentos") from ultimo_error


def buscar_parada(texto: str, resultados: int = 5) -> list:
    """Busca paradas/estaciones cuyo nombre coincide con 'texto'.

    Ejemplo: buscar_parada("Alexanderplatz")
    """
    return obtener_payload("/locations", params={"query": texto, "results": resultados})


def obtener_salidas(stop_id: str, duracion_minutos: int = 10) -> dict:
    """Devuelve las proximas salidas de una parada, con retraso en tiempo real.

    stop_id: identificador de la parada (lo da buscar_parada, campo "id")
    duracion_minutos: ventana de tiempo hacia adelante a consultar
    """
    return obtener_payload(
        f"/stops/{stop_id}/departures",
        params={"duration": duracion_minutos},
    )


if __name__ == "__main__":
    # Ejemplo manual: busca Alexanderplatz y muestra sus proximas salidas.
    paradas = buscar_parada("Alexanderplatz", resultados=1)
    print(paradas)

    if paradas:
        id_parada = paradas[0]["id"]
        salidas = obtener_salidas(id_parada, duracion_minutos=10)
        print(f"\nSalidas en {paradas[0]['name']} ({id_parada}):")
        for salida in salidas["departures"]:
            print(
                f"  {salida['line']['name']} -> {salida['direction']} "
                f"| previsto {salida['plannedWhen']} | retraso {salida.get('delay')}s"
            )