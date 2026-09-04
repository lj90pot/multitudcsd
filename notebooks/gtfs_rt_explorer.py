"""Explora el feed GTFS-Realtime de VBB (protobuf) para ver que forma tienen los datos.

Igual que bvg_client.py: es un script suelto. La logica de produccion ya esta en ingestion/gtfs_rt.py
esto es solo para mirar el payload antes de decidir.

Requiere: pip install requests gtfs-realtime-bindings
"""

import time

import requests
from google.protobuf.json_format import MessageToJson
from google.transit import gtfs_realtime_pb2

FEED_URL = "https://production.gtfsrt.vbb.de/data"
TIMEOUT_SEGUNDOS = 30
MAX_INTENTOS = 3
ESPERA_ENTRE_INTENTOS = 5

# VBB bloquea User-Agents genericos (el "python-requests/x.x" por defecto da 403).
# Piden uno identificable con datos de contacto: https://production.gtfsrt.vbb.de/
CABECERAS = {
    "User-Agent": "multitudcsd-tfm-ucm/0.1 (uso academico, contacto: osoalpueblo@gmail.com)"
}


def descargar_feed_crudo(url: str = FEED_URL) -> bytes:
    """Descarga los bytes del feed GTFS-RT, con reintentos ante fallos de red."""
    ultimo_error = None
    for intento in range(1, MAX_INTENTOS + 1):
        try:
            respuesta = requests.get(url, headers=CABECERAS, timeout=TIMEOUT_SEGUNDOS)
            respuesta.raise_for_status()
            print(f"[gtfs_rt] OK {url} ({len(respuesta.content)} bytes)")
            return respuesta.content
        except requests.RequestException as error:
            ultimo_error = error
            print(f"[gtfs_rt] intento {intento}/{MAX_INTENTOS} fallido: {error}")
            if intento < MAX_INTENTOS:
                time.sleep(ESPERA_ENTRE_INTENTOS)
    raise RuntimeError(f"No se pudo descargar {url} tras {MAX_INTENTOS} intentos") from ultimo_error


def decodificar_feed(contenido_protobuf: bytes) -> gtfs_realtime_pb2.FeedMessage:
    """Convierte los bytes crudos en un mensaje FeedMessage de GTFS-RT."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(contenido_protobuf)
    return feed


def resumir_tipos_de_entidad(feed: gtfs_realtime_pb2.FeedMessage) -> dict:
    """Cuenta cuantas entidades hay de cada tipo (trip_update / vehicle / alert).

    El feed mezcla los tres tipos en la misma lista `feed.entity`; esto ayuda
    a ver de un vistazo que proporcion hay de cada uno.
    """
    contador = {"trip_update": 0, "vehicle": 0, "alert": 0, "otro": 0}
    for entidad in feed.entity:
        if entidad.HasField("trip_update"):
            contador["trip_update"] += 1
        elif entidad.HasField("vehicle"):
            contador["vehicle"] += 1
        elif entidad.HasField("alert"):
            contador["alert"] += 1
        else:
            contador["otro"] += 1
    return contador


if __name__ == "__main__":
    crudo = descargar_feed_crudo()
    feed = decodificar_feed(crudo)

    print(f"\nCabecera: version={feed.header.gtfs_realtime_version}, "
          f"timestamp={feed.header.timestamp}")
    print(f"Total de entidades: {len(feed.entity)}")
    print(f"Por tipo: {resumir_tipos_de_entidad(feed)}")

    # Muestra la primera entidad de cada tipo, como JSON, para ver la forma real.
    tipos_ya_mostrados = set()
    for entidad in feed.entity:
        if entidad.HasField("trip_update") and "trip_update" not in tipos_ya_mostrados:
            print("\n--- Ejemplo trip_update ---")
            print(MessageToJson(entidad.trip_update, preserving_proto_field_name=True))
            tipos_ya_mostrados.add("trip_update")
        elif entidad.HasField("vehicle") and "vehicle" not in tipos_ya_mostrados:
            print("\n--- Ejemplo vehicle ---")
            print(MessageToJson(entidad.vehicle, preserving_proto_field_name=True))
            tipos_ya_mostrados.add("vehicle")
        elif entidad.HasField("alert") and "alert" not in tipos_ya_mostrados:
            print("\n--- Ejemplo alert ---")
            print(MessageToJson(entidad.alert, preserving_proto_field_name=True))
            tipos_ya_mostrados.add("alert")

        if {"trip_update", "vehicle", "alert"}.issubset(tipos_ya_mostrados):
            break

    # Guarda el crudo por si luego quieres usarlo como fixture de test
    # (igual que se hace en la guia de ingesta, paso 8).
    with open("gtfs_rt_sample.pb", "wb") as f:
        f.write(crudo)
    print("\nGuardado gtfs_rt_sample.pb con el payload crudo de esta descarga.")