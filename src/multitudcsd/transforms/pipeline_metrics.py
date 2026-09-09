"""Inventario de las tablas del lakehouse: filas, columnas y versiones Delta, en una tabla Gold."""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from multitudcsd.storage import get_table_path, read_delta

# Inventario del pipeline en orden Medallion.
TABLAS_DEL_PIPELINE = [
    ("bronze", "bronze_gtfs_tripupdates"),
    ("bronze", "bronze_gtfs_static_stops"),
    ("bronze", "bronze_gtfs_static_stop_times"),
    ("bronze", "bronze_gtfs_static_trips"),
    ("bronze", "bronze_gtfs_static_routes"),
    ("bronze", "bronze_gtfs_static_calendar"),
    ("bronze", "bronze_gtfs_static_calendar_dates"),
    ("bronze", "bronze_nextbike_status"),
    ("bronze", "bronze_nextbike_station_information"),
    ("bronze", "bronze_viz_disruptions"),
    ("bronze", "bronze_csd_mentions"),
    ("silver", "silver_bike_availability"),
    ("silver", "silver_transit_delays"),
    ("silver", "silver_disruptions"),
    ("silver", "silver_transit_supply"),
    ("silver", "silver_csd_mentions"),
    ("gold", "gold_mobility_pressure"),
    ("gold", "gold_line_reliability"),
    ("gold", "gold_disruptions_by_cell"),
    ("gold", "gold_station_services"),
    ("gold", "gold_transit_capacity"),
    ("gold", "gold_csd_activity"),
    ("gold", "gold_mobility_vs_activity"),
    ("gold", "gold_delay_predictions"),
]

ESQUEMA_METRICAS = StructType([
    StructField("layer", StringType()),
    StructField("table_name", StringType()),
    StructField("num_rows", LongType()),
    StructField("num_columns", IntegerType()),
    StructField("num_delta_versions", IntegerType()),
])


def table_exists(spark: SparkSession, layer: str, table_name: str) -> bool:
    """Comprueba si la tabla esta creada en disco.

    Se salta en vez de crear error: durante el desarrollo hay tablas que todavia no se han
    generado, y no queremos que eso rompa el inventario de las demas.
    """
    return DeltaTable.isDeltaTable(spark, get_table_path(layer, table_name))


def count_delta_versions(spark: SparkSession, layer: str, table_name: str) -> int:
    """Cuenta las versiones del log de transacciones de una tabla Delta.

    Cada fila del historial es un commit. Una tabla Silver o Gold reconstruida con
    overwrite acumula una version por ejecucion; Bronze, una por ingesta.
    Prueba que time travel esta activado
    """
    tabla = DeltaTable.forPath(spark, get_table_path(layer, table_name))
    return tabla.history().count()


def measure_table(spark: SparkSession, layer: str, table_name: str) -> dict:
    """Mide una tabla: filas, columnas y versiones."""
    df = read_delta(spark, layer, table_name)
    return {
        "layer": layer,
        "table_name": table_name,
        "num_rows": df.count(),
        "num_columns": len(df.columns),
        "num_delta_versions": count_delta_versions(spark, layer, table_name),
    }


def build_gold_pipeline_metrics(spark: SparkSession, tablas: list = None) -> DataFrame:
    """Construye la tabla de metricas del pipeline, una fila por tabla existente.

    Las tablas que todavia no estan en disco se saltan para
    que el inventario se pueda ejecutar en cualquier momento del desarrollo.
    """
    if tablas is None:
        tablas = TABLAS_DEL_PIPELINE

    filas = []
    for layer, table_name in tablas:
        if not table_exists(spark, layer, table_name):
            print(f"[pipeline_metrics] {layer}.{table_name} no existe todavia, se omite")
            continue
        metricas = measure_table(spark, layer, table_name)
        print(
            f"[pipeline_metrics] {layer}.{table_name}: "
            f"{metricas['num_rows']} filas, {metricas['num_columns']} columnas, "
            f"{metricas['num_delta_versions']} versiones"
        )
        filas.append(metricas)

    print(f"[pipeline_metrics] {len(filas)} tablas medidas de {len(tablas)} esperadas")

    df = spark.createDataFrame(filas, schema=ESQUEMA_METRICAS).coalesce(1)
    return df.withColumn("metrics_ts", F.current_timestamp())


# Bloque para ejecutar en pycharm local
if __name__ == "__main__":
    from multitudcsd.config import get_spark_session
    from multitudcsd.storage import write_gold

    sesion = get_spark_session("pipeline-metrics")
    write_gold(build_gold_pipeline_metrics(sesion), "gold_pipeline_metrics")
    sesion.stop()