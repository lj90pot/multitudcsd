"""Ingesta de cortes de trafico y obras de VIZ Berlin hacia la capa Bronze."""

import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from multitudcsd.ingestion.http_request import download_json
from multitudcsd.storage import write_bronze

TABLA_BRONZE = "bronze_viz_disruptions"

ESQUEMA_BRONZE_VIZ = StructType([
    StructField("disruption_id", StringType(), nullable=True),
    StructField("payload_json", StringType(), nullable=False),
    StructField("source_url", StringType(), nullable=False),
])


def get_feed_url() -> str:
    """URL del feed de incidencias de trafico."""
    #datos enriquecidos con comentarios
    #url = os.getenv("VIZ_DISRUPTIONS_URL", "https://api.viz.berlin.de/daten/baustellen_sperrungen_viz.json")
    #datos en bruto
    url = os.getenv("VIZ_DISRUPTIONS_URL", "https://api.viz.berlin.de/tic3/baustellen_sperrungen_tic.json")
    if not url:
        raise RuntimeError("Define VIZ_DISRUPTIONS_URL en el .env antes de ejecutar la ingesta")
    return url


def parse_disruptions(payload: dict, source_url: str) -> list[dict]:
    """Convierte el payload de incidencias en filas para Bronze.

    """
    incidencias = payload.get("features", [])
    filas = []
    for incidencia in incidencias:
        #Corregido aqui porque el id viene dentro de properties
        propiedades = incidencia.get("properties", {})
        id_incidencia = propiedades.get("id","")
        filas.append({
            "disruption_id": str(id_incidencia),
            "payload_json": json.dumps(incidencia, ensure_ascii=False),
            "source_url": source_url,
        })
    return filas


def ingest_disruptions(spark: SparkSession) -> int:
    """Descarga las incidencias de trafico y las escribe en Bronze."""
    url = get_feed_url()
    payload = download_json(url)
    filas = parse_disruptions(payload, url)
    print(f"[viz] {len(filas)} incidencias descargadas")

    if not filas:
        return 0

    df = spark.createDataFrame(filas, schema=ESQUEMA_BRONZE_VIZ)
    write_bronze(df, TABLA_BRONZE, source="real")
    return len(filas)


if __name__ == "__main__":
    from multitudcsd.config import get_spark_session

    sesion = get_spark_session("ingest-viz")
    ingest_disruptions(sesion)
    sesion.stop()