"""Ingesta del GTFS estatico de VBB: paradas, horarios planificados y viajes.

A diferencia de gtfs_rt.py, este dato no es un evento en tiempo real, es una
foto completa que VBB actualiza dos veces por semana (miercoles y viernes).
Por eso aqui NO usamos storage.write_bronze (esa funcion siempre hace append,
pensada para eventos que se van acumulando). Cada vez que descargamos el GTFS
estatico, sobrescribimos la tabla entera con la version nueva.

IMPORTANTE: el GTFS estatico de VBB cubre to.do Berlin y Brandeburgo, no solo
la zona del CSD. stop_times.txt de toda la red tiene millones de filas, y
cargarlas todas en Python antes de mandarlas a Spark es lento y puede agotar
la memoria. Por eso filtramos por proximidad geografica al recorrido del CSD
ANTES de guardar nada: primero nos quedamos con las paradas cercanas, y luego
solo con los horarios/viajes/lineas que pasan por esas paradas.
"""

import csv
import io
import zipfile
import math

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from multitudcsd.config import get_lakehouse_root
from multitudcsd.ingestion.http_request import download_bytes
from multitudcsd.storage import write_bronze_snapshot

URL_GTFS_ESTATICO = "https://www.vbb.de/vbbgtfs"

ESQUEMA_BRONZE_STOPS = StructType([
    StructField("stop_id", StringType(), nullable=False),
    StructField("stop_code", StringType(), nullable=True),
    StructField("stop_name", StringType(), nullable=True),
    StructField("stop_desc", StringType(), nullable=True),
    StructField("stop_lat", StringType(), nullable=True),
    StructField("stop_lon", StringType(), nullable=True),
    StructField("location_type", StringType(), nullable=True),
    StructField("parent_station", StringType(), nullable=True),
    StructField("wheelchair_boarding", StringType(), nullable=True),
    StructField("platform_code", StringType(), nullable=True),
    StructField("zone_id", StringType(), nullable=True),
    StructField("level_id", StringType(), nullable=True),
])

ESQUEMA_BRONZE_STOP_TIMES = StructType([
    StructField("trip_id", StringType(), nullable=False),
    StructField("stop_id", StringType(), nullable=False),
    StructField("stop_sequence", StringType(), nullable=True),
    StructField("pickup_type", StringType(), nullable=True),
    StructField("drop_off_type", StringType(), nullable=True),
    StructField("stop_headsign", StringType(), nullable=True),
    StructField("arrival_time", StringType(), nullable=True),
    StructField("departure_time", StringType(), nullable=True),
])

ESQUEMA_BRONZE_TRIPS = StructType([
    StructField("route_id", StringType(), nullable=False),
    StructField("service_id", StringType(), nullable=False),
    StructField("trip_id", StringType(), nullable=False),
    StructField("trip_headsign", StringType(), nullable=True),
    StructField("trip_short_name", StringType(), nullable=True),
    StructField("direction_id", StringType(), nullable=True),
    StructField("block_id", StringType(), nullable=True),
    StructField("shape_id", StringType(), nullable=True),
    StructField("wheelchair_accessible", StringType(), nullable=True),
    StructField("bikes_allowed", StringType(), nullable=True),
])

ESQUEMA_BRONZE_ROUTES = StructType([
    StructField("route_id", StringType(), nullable=False),
    StructField("agency_id", StringType(), nullable=True),
    StructField("route_short_name", StringType(), nullable=True),
    StructField("route_long_name", StringType(), nullable=True),
    StructField("route_type", StringType(), nullable=True),
    StructField("route_color", StringType(), nullable=True),
    StructField("route_text_color", StringType(), nullable=True),
    StructField("route_desc", StringType(), nullable=True),
])

ESQUEMA_BRONZE_CALENDAR = StructType([
    StructField("service_id", StringType(), nullable=False),
    StructField("monday", StringType(), nullable=True),
    StructField("tuesday", StringType(), nullable=True),
    StructField("wednesday", StringType(), nullable=True),
    StructField("thursday", StringType(), nullable=True),
    StructField("friday", StringType(), nullable=True),
    StructField("saturday", StringType(), nullable=True),
    StructField("sunday", StringType(), nullable=True),
    StructField("start_date", StringType(), nullable=True),
    StructField("end_date", StringType(), nullable=True),
])

ESQUEMA_BRONZE_CALENDAR_DATES = StructType([
    StructField("service_id", StringType(), nullable=False),
    StructField("date", StringType(), nullable=False),
    StructField("exception_type", StringType(), nullable=False),
])

#TODO pasar estos puntos del recorrido a variables del proyecto
#Se calcula el area de estaciones con los puntos de interes del recorrido
# Puntos de referencia del recorrido del CSD 2026 (coordenadas de Wikipedia):
# Spittelmarkt -> Nollendorfplatz (Schoneberg) -> Puerta de Brandeburgo.
PUNTOS_DEL_RECORRIDO = [
    (52.5111, 13.4022),  # Spittelmarkt
    (52.4994, 13.3542),  # Nollendorfplatz
    (52.5163, 13.3777),  # Puerta de Brandeburgo
]

# Margen alrededor de esos puntos. Es una aproximacion simple, NO el trazado
# exacto de la calle (para eso haria falta la geometria real del recorrido)
#Este parametro se usa para calcular
# las estaciones de llegada y dispersion del publico.
MARGEN_KILOMETROS = 3.0

#Correccion de la latitud
# 1 grado de latitud son ~111.32 km en cualquier sitio, pero 1 grado de longitud
# son 111.32 km * cos(latitud): en Berlin se queda en ~67.8 km. Sin esta
# correccion la caja sale casi un 40% mas estrecha de lo que se pretende.
KILOMETROS_POR_GRADO_LATITUD = 111.32
LATITUD_DE_REFERENCIA = 52.51


def compute_recorrido_area(puntos: list, margen_km: float) -> tuple:
    """Devuelve (lat_min, lat_max, lon_min, lon_max) alrededor de una lista de puntos."""
    margen_latitud = margen_km / KILOMETROS_POR_GRADO_LATITUD
    margen_longitud = margen_km / (
        KILOMETROS_POR_GRADO_LATITUD * math.cos(math.radians(LATITUD_DE_REFERENCIA))
    )
    latitudes = [latitud for latitud, longitud in puntos]
    longitudes = [longitud for latitud, longitud in puntos]
    return (
        min(latitudes) - margen_latitud,
        max(latitudes) + margen_latitud,
        min(longitudes) - margen_longitud,
        max(longitudes) + margen_longitud,
    )


LATITUD_MINIMA, LATITUD_MAXIMA, LONGITUD_MINIMA, LONGITUD_MAXIMA = compute_recorrido_area(
    PUNTOS_DEL_RECORRIDO, MARGEN_KILOMETROS
)

def esta_cerca_del_csd(latitud: float, longitud: float) -> bool:
    """Dice si una coordenada cae dentro de la caja alrededor del recorrido del CSD."""
    dentro_de_latitud = LATITUD_MINIMA <= latitud <= LATITUD_MAXIMA
    dentro_de_longitud = LONGITUD_MINIMA <= longitud <= LONGITUD_MAXIMA
    return dentro_de_latitud and dentro_de_longitud


def descargar_zip_gtfs_estatico() -> bytes:
    """Descarga el zip completo del GTFS estatico de VBB."""
    return download_bytes(URL_GTFS_ESTATICO)


def abrir_fichero_del_zip(contenido_zip: bytes, nombre_fichero: str) -> csv.DictReader:
    """Abre un fichero de dentro del zip y devuelve un lector de csv, fila a fila.

    No carga el fichero entero en una lista aqui: eso lo decide quien llama a
    esta funcion, para poder descartar filas mientras se lee y no guardar en
    memoria lo que no hace falta.
    """
    zip_en_memoria = io.BytesIO(contenido_zip)
    zip_abierto = zipfile.ZipFile(zip_en_memoria)
    fichero_abierto = zip_abierto.open(nombre_fichero)
    texto_del_fichero = fichero_abierto.read().decode("utf-8-sig")
    return csv.DictReader(io.StringIO(texto_del_fichero))


def obtener_paradas_cerca_del_csd(contenido_zip: bytes) -> list[dict]:
    """Lee stops.txt y se queda solo con las paradas dentro del area del CSD."""
    lector = abrir_fichero_del_zip(contenido_zip, "stops.txt")

    paradas_cercanas = []
    for fila in lector:
        # Algunas filas de stops.txt no traen coordenadas. Las descartamos.
        if not fila.get("stop_lat") or not fila.get("stop_lon"):
            continue
        latitud = float(fila["stop_lat"])
        longitud = float(fila["stop_lon"])
        if esta_cerca_del_csd(latitud, longitud):
            paradas_cercanas.append(fila)

    print(f"[gtfs_static] stops.txt: {len(paradas_cercanas)} paradas cerca del CSD")
    return paradas_cercanas


def obtener_horarios_de_esas_paradas(contenido_zip: bytes, ids_de_paradas: set) -> list[dict]:
    """Lee stop_times.txt y se queda solo con las filas de las paradas relevantes.

    stop_times.txt de toda la red VBB tiene millones de filas.
    Se filtran las relevantes cerca del reccorido definidas en el area mas arriba
    """
    lector = abrir_fichero_del_zip(contenido_zip, "stop_times.txt")

    horarios_relevantes = [fila for fila in lector if fila["stop_id"] in ids_de_paradas]

    print(f"[gtfs_static] stop_times.txt: {len(horarios_relevantes)} filas relevantes")
    return horarios_relevantes


def obtener_viajes_relevantes(contenido_zip: bytes, ids_de_viajes: set) -> list[dict]:
    """Lee trips.txt y se queda solo con los viajes que pasan por las paradas relevantes."""
    lector = abrir_fichero_del_zip(contenido_zip, "trips.txt")

    viajes_relevantes = [fila for fila in lector if fila["trip_id"] in ids_de_viajes]

    print(f"[gtfs_static] trips.txt: {len(viajes_relevantes)} viajes relevantes")
    return viajes_relevantes


def obtener_lineas_relevantes(contenido_zip: bytes, ids_de_lineas: set) -> list[dict]:
    """Lee routes.txt y se queda solo con las lineas usadas por los viajes relevantes."""
    lector = abrir_fichero_del_zip(contenido_zip, "routes.txt")

    lineas_relevantes = [fila for fila in lector if fila["route_id"] in ids_de_lineas]

    print(f"[gtfs_static] routes.txt: {len(lineas_relevantes)} lineas relevantes")
    return lineas_relevantes

def obtener_calendario_viajes(contenido_zip: bytes, ids_de_servicios: set) -> list[dict]:
    """Lee calendar.txt y se queda solo con los calendarios de los servicios relevantes.

    Cada viaje de trips.txt apunta a un service_id que dice que dias de la semana
    circula y entre que fechas.
    """
    try:
        lector = abrir_fichero_del_zip(contenido_zip, "calendar.txt")
    except KeyError:
        # calendar.txt es opcional en GTFS: hay feeds que solo usan calendar_dates.txt.
        print("[gtfs_static] calendar.txt no esta en el zip, se devuelve vacio")
        return []

    calendarios_relevantes = [fila for fila in lector if fila["service_id"] in ids_de_servicios]

    print(f"[gtfs_static] calendar.txt: {len(calendarios_relevantes)} servicios relevantes")
    return calendarios_relevantes

def obtener_calendario_viaje_excepciones(contenido_zip: bytes, ids_de_servicios: set) -> list[dict]:
    """Lee calendar_dates.txt y se queda solo con las excepciones de los servicios relevantes.

    Aqui estan definidos los servicios de refuerzo en dias especiales o excepciones
    planificadas en los servicios.
    """
    try:
        lector = abrir_fichero_del_zip(contenido_zip, "calendar_dates.txt")
    except KeyError:
        print("[gtfs_static] calendar_dates.txt no esta en el zip, se devuelve vacio")
        return []

    excepciones_relevantes = [fila for fila in lector if fila["service_id"] in ids_de_servicios]

    print(f"[gtfs_static] calendar_dates.txt: {len(excepciones_relevantes)} excepciones relevantes")
    return excepciones_relevantes

def ingestar_gtfs_estatico(spark: SparkSession) -> None:
    """Descarga el GTFS estatico completo y guarda solo lo relevante para el CSD.

    Filtra en cascada: primero las paradas cercanas, despues solo los horarios
    de esas paradas, despues solo los viajes de esos horarios, despues solo las
    lineas de esos viajes, y finalmente sus horarios y refuerzos si existen.
    """
    contenido_zip = descargar_zip_gtfs_estatico()

    paradas = obtener_paradas_cerca_del_csd(contenido_zip)
    ids_de_paradas = {fila["stop_id"] for fila in paradas}

    horarios = obtener_horarios_de_esas_paradas(contenido_zip, ids_de_paradas)
    ids_de_viajes = {fila["trip_id"] for fila in horarios}

    viajes = obtener_viajes_relevantes(contenido_zip, ids_de_viajes)
    ids_de_lineas = {fila["route_id"] for fila in viajes}

    lineas = obtener_lineas_relevantes(contenido_zip, ids_de_lineas)

    ids_de_servicios = {fila["service_id"] for fila in viajes}
    calendarios = obtener_calendario_viajes(contenido_zip, ids_de_servicios)
    excepciones_de_calendario = obtener_calendario_viaje_excepciones(contenido_zip, ids_de_servicios)

    write_bronze_snapshot(
        spark.createDataFrame(paradas, schema=ESQUEMA_BRONZE_STOPS),
        "bronze_gtfs_static_stops", source="real",
    )
    write_bronze_snapshot(
        spark.createDataFrame(horarios, schema=ESQUEMA_BRONZE_STOP_TIMES),
        "bronze_gtfs_static_stop_times", source="real",
    )
    write_bronze_snapshot(
        spark.createDataFrame(viajes, schema=ESQUEMA_BRONZE_TRIPS),
        "bronze_gtfs_static_trips", source="real",
    )
    write_bronze_snapshot(
        spark.createDataFrame(lineas, schema=ESQUEMA_BRONZE_ROUTES),
        "bronze_gtfs_static_routes", source="real",
    )
    write_bronze_snapshot(
        spark.createDataFrame(calendarios, schema=ESQUEMA_BRONZE_CALENDAR),
        "bronze_gtfs_static_calendar", source="real",
    )
    write_bronze_snapshot(
        spark.createDataFrame(excepciones_de_calendario, schema=ESQUEMA_BRONZE_CALENDAR_DATES),
        "bronze_gtfs_static_calendar_dates", source="real",
    )

if __name__ == "__main__":
    # Permite ejecutar la ingesta directamente desde PyCharm con el boton Run.
    from multitudcsd.config import get_spark_session

    sesion = get_spark_session("ingest-gtfs-static")
    ingestar_gtfs_estatico(sesion)
    sesion.stop()