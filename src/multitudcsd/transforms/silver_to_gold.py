"""Agregaciones Silver -> Gold de la etapa 1 (transporte).

Igual que en bronze_to_silver: funciones puras, DataFrame -> DataFrame, sin tocar
disco. La lectura/escritura vive solo en el bloque __main__.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

UMBRAL_A_TIEMPO_SEGUNDOS = 60  # retraso <= 60s se considera "a tiempo"; decision documentada
FECHA_CSD = "2026-07-25"  # dia del recorrido del CSD Berlin 2026; decision documentada


def build_gold_mobility_pressure(silver_bikes: DataFrame, silver_delays: DataFrame) -> DataFrame:
    """Indice de presion de movilidad: disponibilidad de bici y retraso, por celda y hora.

    Cada fuente se agrega primero por separado y luego se cruzan con un full outer
    join: una celda puede tener lecturas de bicis sin incidencias de transporte
    encima, o al reves. Los retrasos sin h3_index (parada fuera de la zona filtrada
    del GTFS estatico) se descartan aqui: no se pueden situar en una celda.
    """
    bicis_por_celda = (
        silver_bikes
        .withColumn("hour_of_day", F.hour("reading_ts"))
        .groupBy("h3_index", "hour_of_day")
        .agg(
            F.avg("num_bikes_available").alias("avg_bikes_available"),
            F.avg("num_docks_available").alias("avg_docks_available"),
            F.count("*").alias("num_lecturas_bici"),
        )
    )

    retrasos_por_celda = (
        silver_delays
        .filter(F.col("h3_index").isNotNull())
        .withColumn("hour_of_day", F.hour("feed_ts"))
        .withColumn(
            "a_tiempo",
            F.when(F.col("delay_seconds") <= UMBRAL_A_TIEMPO_SEGUNDOS, 1.0).otherwise(0.0),
        )
        .groupBy("h3_index", "hour_of_day")
        .agg(
            F.avg("delay_seconds").alias("avg_delay_seconds"),
            F.avg("a_tiempo").alias("pct_on_time"),
            F.count("*").alias("num_actualizaciones_retraso"),
        )
    )

    return bicis_por_celda.join(
        retrasos_por_celda, on=["h3_index", "hour_of_day"], how="full_outer"
    )


def build_gold_line_reliability(silver_delays: DataFrame) -> DataFrame:
    """Retraso medio y porcentaje de puntualidad por linea y hora del dia.

    No necesita h3_index: agrega directamente por route_id, asi que incluye tambien
    los retrasos de paradas fuera de la zona filtrada del GTFS estatico.
    """
    return (
        silver_delays
        .withColumn("hour_of_day", F.hour("feed_ts"))
        .withColumn(
            "a_tiempo",
            F.when(F.col("delay_seconds") <= UMBRAL_A_TIEMPO_SEGUNDOS, 1.0).otherwise(0.0),
        )
        .groupBy("route_id", "hour_of_day")
        .agg(
            F.avg("delay_seconds").alias("avg_delay_seconds"),
            F.avg("a_tiempo").alias("pct_on_time"),
            F.count("*").alias("num_actualizaciones"),
        )
    )


def build_gold_disruptions_by_cell(silver_disruptions: DataFrame) -> DataFrame:
    """Cuenta cortes de trafico activos durante el dia del CSD, por celda H3.

    Un corte esta activo el dia del CSD si su ventana [valid_from, valid_to] toca ese
    dia (no hace falta que se solape con una hora concreta: basta con que el intervalo
    incluya el 25 de julio de 2026 en algun momento). Muchos cortes de obra duran
    semanas o meses (ver el ejemplo del Paso 6), asi que esto filtra fuera los que ya
    habian terminado o los que empiezan despues del evento.
    """
    inicio_del_dia = F.to_timestamp(F.lit(FECHA_CSD))
    fin_del_dia = F.to_timestamp(F.lit(f"{FECHA_CSD} 23:59:59"))

    activos_durante_el_csd = silver_disruptions.filter(
        (F.col("valid_from") <= fin_del_dia) & (F.col("valid_to") >= inicio_del_dia)
    )

    return (
        activos_durante_el_csd
        .filter(F.col("h3_index").isNotNull())
        .groupBy("h3_index")
        .agg(F.count("*").alias("num_disruptions"))
    )


if __name__ == "__main__":
    from multitudcsd.config import get_spark_session
    from multitudcsd.storage import read_delta, write_gold

    sesion = get_spark_session("silver-to-gold")

    silver_bikes = read_delta(sesion, "silver", "silver_bike_availability")
    silver_delays = read_delta(sesion, "silver", "silver_transit_delays")
    write_gold(build_gold_mobility_pressure(silver_bikes, silver_delays), "gold_mobility_pressure")
    write_gold(build_gold_line_reliability(silver_delays), "gold_line_reliability")

    silver_disruptions = read_delta(sesion, "silver", "silver_disruptions")
    write_gold(build_gold_disruptions_by_cell(silver_disruptions), "gold_disruptions_by_cell")

    sesion.stop()