"""Ingesta incremental de menciones sinteticas desde landing a Bronze."""

#Imports
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from multitudcsd.config import get_landing_root
from multitudcsd.storage import write_bronze_stream

TABLA_BRONZE = "bronze_csd_mentions"
CHECKPOINT = "bronze_csd_mentions"

# Esquema explicito: readStream no puede inferirlo bien
ESQUEMA_MENCION = StructType([
    StructField("mention_id", StringType()),
    StructField("event_ts", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("platform", StringType()),
    StructField("language", StringType()),
    StructField("sentiment", DoubleType()),
    StructField("has_media", BooleanType()),
    StructField("user_hash", StringType()),
])

#Functions
def read_mentions_stream(spark: SparkSession, carpeta_landing: str) -> DataFrame:
    """Abre el stream de lectura sobre la carpeta de menciones en landing."""
    return spark.readStream.schema(ESQUEMA_MENCION).json(carpeta_landing)


def ingest_mentions_stream(spark: SparkSession) -> None:
    """Lee los ficheros nuevos de landing y los escribe en Bronze. Se puede re-ejecutar.
    """
    carpeta_landing = f"{get_landing_root()}/mentions"
    print(f"[mentions_stream] leyendo de {carpeta_landing}")

    stream = read_mentions_stream(spark, carpeta_landing)
    write_bronze_stream(stream, TABLA_BRONZE, CHECKPOINT, source="synthetic")


if __name__ == "__main__":
    from multitudcsd.config import get_spark_session

    sesion = get_spark_session("stream-mentions")
    ingest_mentions_stream(sesion)
    sesion.stop()