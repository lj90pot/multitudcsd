"""Ingesta del feed GTFS-Realtime de VBB (formato protobuf) en la capa Bronze."""

import os

from google.protobuf.json_format import MessageToJson
from google.transit import gtfs_realtime_pb2
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from multitudcsd.ingestion.http_request import download_bytes
from multitudcsd.storage import write_bronze

TABLA_BRONZE = "bronze_gtfs_tripupdates"

ESQUEMA_BRONZE_GTFS = StructType([
    StructField("entity_id", StringType(), nullable=True),
    StructField("payload_json", StringType(), nullable=False),
    StructField("feed_timestamp", StringType(), nullable=True),
    StructField("source_url", StringType(), nullable=False),
])


def get_feed_url() -> str:
    """URL del feed GTFS-RT de produccion de VBB."""
    
    return os.getenv("VBB_GTFS_RT_URL", "https://production.gtfsrt.vbb.de/data") #prod
    #return os.getenv("VBB_GTFS_RT_URL", "https://staging.gtfsrt.vbb.de/data") #preprod


def decode_feed(contenido_protobuf: bytes) -> gtfs_realtime_pb2.FeedMessage:
    """Decodifica los bytes del feed a un mensaje GTFS-RT."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(contenido_protobuf)
    return feed


def extract_trip_updates(feed: gtfs_realtime_pb2.FeedMessage, source_url: str) -> list[dict]:
    """Extrae las entidades de tipo trip_update como filas JSON crudas para Bronze."""
    feed_timestamp = str(feed.header.timestamp)

    filas = []
    for entidad in feed.entity:
        # El feed mezcla trip_update, vehicle y alert. Aqui solo queremos trip_update.
        if not entidad.HasField("trip_update"):
            continue
        filas.append({
            "entity_id": entidad.id,
            # MessageToJson conserva la estructura original del protobuf sin aplanarla.
            "payload_json": MessageToJson(entidad.trip_update, preserving_proto_field_name=True),
            "feed_timestamp": feed_timestamp,
            "source_url": source_url,
        })
    return filas


def ingest_trip_updates(spark: SparkSession) -> int:
    """Descarga el feed GTFS-RT y escribe los trip_update en Bronze."""
    url = get_feed_url()
    contenido = download_bytes(url)
    feed = decode_feed(contenido)
    filas = extract_trip_updates(feed, url)
    print(f"[gtfs_rt] {len(filas)} trip_updates extraidos de {len(feed.entity)} entidades")

    if not filas:
        print("[gtfs_rt] el feed no traia trip_updates, no se escribe nada")
        return 0

    df = spark.createDataFrame(filas, schema=ESQUEMA_BRONZE_GTFS)
    write_bronze(df, TABLA_BRONZE, source="real")
    return len(filas)


if __name__ == "__main__":
    from multitudcsd.config import get_spark_session

    sesion = get_spark_session("ingest-gtfs-rt")
    ingest_trip_updates(sesion)
    sesion.stop()