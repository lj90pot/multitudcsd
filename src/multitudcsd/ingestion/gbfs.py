"""Ingesta de disponibilidad de bicicletas de Nextbike (GBFS) hacia la capa Bronze."""

import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from multitudcsd.ingestion.http_request import download_json
from multitudcsd.storage import write_bronze

TABLA_BRONZE = "bronze_nextbike_status"
TABLA_BRONZE_INFO = "bronze_nextbike_station_information"

# Esquema explicito: Bronze guarda el JSON crudo de cada estacion como texto.
ESQUEMA_BRONZE_GBFS = StructType([
    StructField("station_id", StringType(), nullable=True),
    StructField("payload_json", StringType(), nullable=False),
    StructField("source_url", StringType(), nullable=False),
    StructField("feed_last_updated", StringType(), nullable=True),
])


def get_discovery_url() -> str:
    """URL del fichero de descubrimiento GBFS, configurable por entorno."""
    return os.getenv(
        "NEXTBIKE_GBFS_DISCOVERY_URL",
        "https://gbfs.nextbike.net/maps/gbfs/v2/nextbike_bn/gbfs.json",
    )


def find_feed_url(discovery_payload: dict, feed_name: str, language: str = "en") -> str:
    """Busca la URL de un feed concreto dentro del fichero de descubrimiento GBFS.

    El fichero gbfs.json lista, por idioma, los feeds disponibles
    (station_information, station_status, free_bike_status...).
    """
    feeds_por_idioma = discovery_payload["data"]
    if language in feeds_por_idioma:
        feeds = feeds_por_idioma[language]["feeds"]
    else:
        # Algunos operadores solo publican un idioma; cogemos el primero que haya.
        primer_idioma = next(iter(feeds_por_idioma))
        feeds = feeds_por_idioma[primer_idioma]["feeds"]

    for feed in feeds:
        if feed["name"] == feed_name:
            return feed["url"]
    raise ValueError(f"El feed '{feed_name}' no existe en el descubrimiento GBFS")


def parse_station_status(status_payload: dict, source_url: str) -> list[dict]:
    """Convierte el payload de station_status en una lista de filas para Bronze."""
    last_updated = str(status_payload.get("last_updated", ""))
    estaciones = status_payload["data"]["stations"]

    filas = []
    for estacion in estaciones:
        filas.append({
            "station_id": str(estacion.get("station_id", "")),
            # Guardamos la estacion entera sin tocar: Bronze es crudo.
            "payload_json": json.dumps(estacion, ensure_ascii=False),
            "source_url": source_url,
            "feed_last_updated": last_updated,
        })
    return filas


def ingest_station_status(spark: SparkSession) -> int:
    """Descarga el estado de las estaciones y lo escribe en Bronze. Devuelve nº de filas."""
    descubrimiento = download_json(get_discovery_url())
    url_status = find_feed_url(descubrimiento, "station_status")

    payload = download_json(url_status)
    filas = parse_station_status(payload, url_status)
    print(f"[gbfs] {len(filas)} estaciones descargadas")

    df = spark.createDataFrame(filas, schema=ESQUEMA_BRONZE_GBFS)
    write_bronze(df, TABLA_BRONZE, source="real")
    return len(filas)

def ingest_station_information(spark: SparkSession) -> int:
    """Descarga la informacion estatica de las estaciones (lat, lon, capacidad).

    Reutiliza parse_station_status porque 'station_information' tiene la misma forma
    de payload que 'station_status' (lista de estaciones bajo data.stations). Solo
    cambia el feed que pedimos y la tabla Bronze de destino.
    """
    descubrimiento = download_json(get_discovery_url())
    url_info = find_feed_url(descubrimiento, "station_information")

    payload = download_json(url_info)
    filas = parse_station_status(payload, url_info)
    print(f"[gbfs] {len(filas)} estaciones (informacion estatica) descargadas")

    df = spark.createDataFrame(filas, schema=ESQUEMA_BRONZE_GBFS)
    write_bronze(df, TABLA_BRONZE_INFO, source="real")
    return len(filas)


if __name__ == "__main__":
    # para probar la ingesta en pycharm

    from multitudcsd.config import get_spark_session

    sesion = get_spark_session("ingest-gbfs")
    ingest_station_status(sesion)
    ingest_station_information(sesion)
    sesion.stop()