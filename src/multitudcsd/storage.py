"""Lectura y escritura de tablas Delta en el Lakehosue. Ningun otro modulo escribe Delta."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from multitudcsd.config import get_lakehouse_root

CAPAS_VALIDAS = ("bronze", "silver", "gold")


def get_table_path(layer: str, table_name: str) -> str:
    """Devuelve la ruta completa de una tabla dentro del lakehouse."""
    if layer not in CAPAS_VALIDAS:
        raise ValueError(f"Capa no valida: {layer}. Usa una de {CAPAS_VALIDAS}")
    return f"{get_lakehouse_root()}/{layer}/{table_name}"


def add_ingest_metadata(df: DataFrame, source: str) -> DataFrame:
    """Anade las columnas de trazabilidad comunes a toda la capa Bronze.

    - source: 'real' o 'synthetic'. El pipeline es agnostico al origen.
    - ingest_ts / ingest_date: cuando entro el dato, no cuando se produjo.
    """
    if source not in ("real", "synthetic"):
        raise ValueError("source debe ser 'real' o 'synthetic'")
    return (
        df.withColumn("source", F.lit(source)) #nombre de la fuente
        .withColumn("ingest_ts", F.current_timestamp())  #momento de la ingesta
        .withColumn("ingest_date", F.current_date()) #para las particiones
    )


def write_bronze(df: DataFrame, table_name: str, source: str) -> None:
    """Escribe un DataFrame en Bronze: append-only y particionado por fecha de ingesta."""
    df_con_metadatos = add_ingest_metadata(df, source)
    ruta = get_table_path("bronze", table_name)
    (
        df_con_metadatos.write.format("delta")
        .mode("append")
        .partitionBy("ingest_date")
        .save(ruta)
    )
    print(f"[storage] escritas {df_con_metadatos.count()} filas en {ruta}")


def read_delta(spark: SparkSession, layer: str, table_name: str) -> DataFrame:
    """Lee una tabla Delta del lakehouse."""
    return spark.read.format("delta").load(get_table_path(layer, table_name))

def write_silver(df: DataFrame, table_name: str) -> None:
    """Escribe una tabla Silver, reconstruyendola por completo en cada ejecucion.
    """
    ruta = get_table_path("silver", table_name)
    df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(ruta)
    print(f"[storage] escritas {df.count()} filas en {ruta}")


def write_gold(df: DataFrame, table_name: str) -> None:
    """Escribe una tabla Gold, reconstruyendola por completo en cada ejecucion."""
    ruta = get_table_path("gold", table_name)
    df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(ruta)
    print(f"[storage] escritas {df.count()} filas en {ruta}")