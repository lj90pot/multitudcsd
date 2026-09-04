"""Ingesta del GTFS estatico de VBB: paradas, horarios planificados y viajes.

A diferencia de gtfs_rt.py, este dato no es un evento en tiempo real, es una
foto completa que VBB actualiza dos veces por semana (miercoles y viernes).
Por eso aqui NO usamos storage.write_bronze (esa funcion siempre hace append,
pensada para eventos que se van acumulando). Cada vez que descargamos el GTFS
estatico, sobrescribimos la tabla entera con la version nueva.

IMPORTANTE: el GTFS estatico de VBB cubre TODO Berlin y Brandeburgo, no solo
la zona del CSD. stop_times.txt de toda la red tiene millones de filas, y
cargarlas todas en Python antes de mandarlas a Spark es lento y puede agotar
la memoria. Por eso filtramos por proximidad geografica al recorrido del CSD
ANTES de guardar nada: primero nos quedamos con las paradas cercanas, y luego
solo con los horarios/viajes/lineas que pasan por esas paradas.
"""

import csv
import io
import zipfile

from pyspark.sql import SparkSession

from multitudcsd.config import get_lakehouse_root
from multitudcsd.ingestion.http_request import download_bytes

URL_GTFS_ESTATICO = "https://www.vbb.de/vbbgtfs"

# Coordenadas para limiitar alrededor del recorrido del CSD 2026:
# Spittelmarkt -> Nollendorfplatz (Schoneberg) -> Puerta de Brandeburgo.
# Coordenadas de esos tres puntos sacadas de Wikipedia, con un margen de unos
# 1.5 km alrededor. Es una aproximacion simple, NO el trazado exacto de la
# calle (para eso haria falta la geometria real del recorrido, que no forma
# parte de este proyecto). Documentar esta decision en docs/decisiones.md.
LATITUD_MINIMA = 52.484
LATITUD_MAXIMA = 52.531
LONGITUD_MINIMA = 13.339
LONGITUD_MAXIMA = 13.419


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
    """Lee stops.txt y se queda solo con las paradas dentro de la caja del CSD."""
    lector = abrir_fichero_del_zip(contenido_zip, "stops.txt")

    paradas_cercanas = []
    for fila in lector:
        # Algunas filas de stops.txt no traen coordenadas (son entradas de
        # estacion, no paradas en si). Las descartamos si faltan.
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

    stop_times.txt de toda la red VBB tiene millones de filas. Filtramos fila
    a fila mientras leemos, asi que solo terminamos guardando en memoria las
    decenas de miles que de verdad tocan al CSD.
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


def guardar_tabla_gtfs_estatico(spark: SparkSession, nombre_tabla: str, filas: list[dict]) -> None:
    """Guarda una tabla del GTFS estatico en Bronze, sobrescribiendo la version anterior.

    IMPORTANTE: usamos mode("overwrite") a proposito, no append. El GTFS estatico
    es una foto que se sustituye entera cada vez que se descarga, no tiene sentido
    acumular versiones antiguas como hacemos con los eventos de GTFS-RT.
    """
    if not filas:
        print(f"[gtfs_static] {nombre_tabla}: 0 filas, no se escribe nada")
        return

    ruta_tabla = f"{get_lakehouse_root()}/bronze/{nombre_tabla}"
    dataframe = spark.createDataFrame(filas)
    dataframe.write.format("delta").mode("overwrite").save(ruta_tabla)
    print(f"[gtfs_static] guardado {nombre_tabla} en {ruta_tabla}")


def ingestar_gtfs_estatico(spark: SparkSession) -> None:
    """Descarga el GTFS estatico completo y guarda solo lo relevante para el CSD.

    Filtra en cascada: primero las paradas cercanas, luego solo los horarios
    de esas paradas, luego solo los viajes de esos horarios, luego solo las
    lineas de esos viajes. Asi cada fichero se lee una vez y se filtra con lo
    que ya sabemos del fichero anterior.
    """
    contenido_zip = descargar_zip_gtfs_estatico()

    paradas = obtener_paradas_cerca_del_csd(contenido_zip)
    ids_de_paradas = {fila["stop_id"] for fila in paradas}

    horarios = obtener_horarios_de_esas_paradas(contenido_zip, ids_de_paradas)
    ids_de_viajes = {fila["trip_id"] for fila in horarios}

    viajes = obtener_viajes_relevantes(contenido_zip, ids_de_viajes)
    ids_de_lineas = {fila["route_id"] for fila in viajes}

    lineas = obtener_lineas_relevantes(contenido_zip, ids_de_lineas)

    guardar_tabla_gtfs_estatico(spark, "bronze_gtfs_static_stops", paradas)
    guardar_tabla_gtfs_estatico(spark, "bronze_gtfs_static_stop_times", horarios)
    guardar_tabla_gtfs_estatico(spark, "bronze_gtfs_static_trips", viajes)
    guardar_tabla_gtfs_estatico(spark, "bronze_gtfs_static_routes", lineas)


if __name__ == "__main__":
    # Permite ejecutar la ingesta directamente desde PyCharm con el boton Run.
    from multitudcsd.config import get_spark_session

    sesion = get_spark_session("ingest-gtfs-static")
    ingestar_gtfs_estatico(sesion)
    sesion.stop()