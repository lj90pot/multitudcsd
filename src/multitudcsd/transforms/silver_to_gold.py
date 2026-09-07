"""Agregaciones Silver -> Gold de la etapa 1 (transporte).

Igual que en bronze_to_silver: funciones puras, DataFrame -> DataFrame, sin tocar
disco. La lectura/escritura vive solo en el bloque __main__.
"""

#Import
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from multitudcsd.config import FECHA_REFERENCIA

UMBRAL_A_TIEMPO_SEGUNDOS = 60  # retraso <= 60s se considera "a tiempo"; decision documentada

#Funciones
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
    inicio_del_dia = F.to_timestamp(F.lit(FECHA_REFERENCIA))
    fin_del_dia = F.to_timestamp(F.lit(f"{FECHA_REFERENCIA} 23:59:59"))

    activos_durante_el_csd = silver_disruptions.filter(
        (F.col("valid_from") <= fin_del_dia) & (F.col("valid_to") >= inicio_del_dia)
    )

    return (
        activos_durante_el_csd
        .filter(F.col("h3_index").isNotNull())
        .groupBy("h3_index")
        .agg(F.count("*").alias("num_disruptions"))
    )

def build_gold_station_services(silver_transit_supply: DataFrame) -> DataFrame:
    """Servicios que paran en cada estacion el dia del CSD: linea, modo y frecuencia.

    Una fila por estacion y linea. num_scheduled_stops es el numero de pasos
    programados en todo el dia; dividido entre las horas de servicio da la
    frecuencia media, pero se deja el conteo crudo para no perder informacion.
    """
    return (
        silver_transit_supply
        .groupBy("station_id", "transport_mode", "route_name")
        .agg(
            F.first("stop_name").alias("station_name"),
            F.first("h3_index").alias("h3_index"),
            F.count("*").alias("num_scheduled_stops"),
            F.min("scheduled_hour").alias("first_hour"),
            F.max("scheduled_hour").alias("last_hour"),
        )
    )

def build_gold_transit_capacity(silver_transit_supply: DataFrame) -> DataFrame:
    """Oferta de transporte planificada por celda H3, franja horaria y modo.

    num_scheduled_stops es el numero de vehiculos que tienen parada programada en
    la celda durante esa hora. Es un proxy de capacidad, no una cifra de plazas:
    GTFS no publica el aforo del vehiculo. Sirve como denominador de la presion de
    movilidad (10 retrasos en una celda con 200 pasos programados no significan lo
    mismo que en una con 12) y como feature del modelo.
    """
    return (
        silver_transit_supply
        .filter(F.col("h3_index").isNotNull())
        .groupBy("h3_index", "scheduled_hour", "transport_mode")
        .agg(
            F.count("*").alias("num_scheduled_stops"),
            F.countDistinct("route_name").alias("num_routes"),
            F.countDistinct("station_id").alias("num_stations"),
        )
    )

K_MINIMO_MENCIONES = 5  # umbral de k-anonimato: celda-hora con menos filas no se publica

def build_gold_csd_activity(silver_mentions: DataFrame) -> DataFrame:
    """Actividad social agregada por celda H3 y franja horaria.

    Filtra un minimo de menciones por hora para que no sea posible identificar usuarios
    en una celda
    """
    return (
        silver_mentions
        .filter(F.col("h3_index").isNotNull())
        .groupBy("h3_index", "hour_of_day")
        .agg(
            F.count("*").alias("num_mentions"),
            F.countDistinct("user_hash").alias("num_users"),
            F.avg("sentiment").alias("avg_sentiment"),
            F.avg(F.col("has_media").cast("double")).alias("pct_with_media"),
        )
        .filter(F.col("num_mentions") >= K_MINIMO_MENCIONES)
    )


def build_gold_mobility_vs_activity(
    gold_activity: DataFrame,
    gold_mobility_pressure: DataFrame,
    gold_transit_capacity: DataFrame,
) -> DataFrame:
    """Cruza las menciones con la presion y la capacidad de transporte, por celda y hora.

    Es la tabla que justifica el proyecto entero: pone en la misma fila un dato real
    (retrasos, bicis, pasos programados) y uno sintetico (menciones)
    Left join desde la actividad: interesa saber que pasaba en transporte donde habia
    gente
    """
    # La capacidad viene abierta por modo; aqui hace falta el total de la celda y hora.
    capacidad_por_celda = (
        gold_transit_capacity
        .groupBy("h3_index", F.col("scheduled_hour").alias("hour_of_day"))
        .agg(
            F.sum("num_scheduled_stops").alias("num_scheduled_stops"),
            F.sum("num_routes").alias("num_routes"),
        )
    )

    return (
        gold_activity
        .join(gold_mobility_pressure, on=["h3_index", "hour_of_day"], how="left")
        .join(capacidad_por_celda, on=["h3_index", "hour_of_day"], how="left")
    )

#Bloque para ejecutar en pycharm local
if __name__ == "__main__":
    from multitudcsd.config import get_spark_session
    from multitudcsd.storage import read_delta, write_gold

    sesion = get_spark_session("silver-to-gold")

    silver_bikes = read_delta(sesion, "silver", "silver_bike_availability")
    silver_delays = read_delta(sesion, "silver", "silver_transit_delays")
    gold_mobility_pressure = build_gold_mobility_pressure(silver_bikes, silver_delays)
    write_gold(gold_mobility_pressure, "gold_mobility_pressure")
    write_gold(build_gold_line_reliability(silver_delays), "gold_line_reliability")

    silver_disruptions = read_delta(sesion, "silver", "silver_disruptions")
    write_gold(build_gold_disruptions_by_cell(silver_disruptions), "gold_disruptions_by_cell")

    silver_transit_supply = read_delta(sesion, "silver", "silver_transit_supply")
    write_gold(build_gold_station_services(silver_transit_supply), "gold_station_services")
    gold_transit_capacity = build_gold_transit_capacity(silver_transit_supply)
    write_gold(gold_transit_capacity, "gold_transit_capacity")

    silver_mentions = read_delta(sesion, "silver", "silver_csd_mentions")
    gold_activity = build_gold_csd_activity(silver_mentions)
    write_gold(gold_activity, "gold_csd_activity")
    write_gold(
        build_gold_mobility_vs_activity(
            gold_activity, gold_mobility_pressure, gold_transit_capacity
        ),
        "gold_mobility_vs_activity",
    )

    sesion.stop()