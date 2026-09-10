"""Construccion de las features del modelo de retraso a partir de la capa Gold.

El calculo se hace con Spark sobre gold_line_reliability.
Pandas solo al final para usar scikit-learn
"""

import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Columnas del modelo, declaradas aqui para que train.py y predict.py usen exactamente
# las mismas y en el mismo orden. Si se toca esta lista hay que reentrenar.
COLUMNA_OBJETIVO = "avg_delay_seconds"
COLUMNAS_CATEGORICAS = ["route_id"]
COLUMNAS_NUMERICAS = [
    "hour_of_day",
    "day_of_week",
    "delay_lag_seconds",
    "pct_on_time_lag",
    "num_updates_lag",
    "gap_hours",
]


def add_slot_timestamp(gold_line_reliability: DataFrame) -> DataFrame:
    """Anade slot_ts: el instante que representa la franja (fecha del dia + hora).

    Gold guarda la fecha y la hora en columnas separadas. Para ordenar usamos fecha-hora
    como timestamp
    """
    return gold_line_reliability.withColumn(
        "slot_ts",
        F.expr("timestampadd(HOUR, hour_of_day, to_timestamp(service_date))"),
    )


def add_previous_slot(franjas: DataFrame) -> DataFrame:
    """En cada franja los valores de la franja anterior observada de esa misma linea.

    Es el nucleo del modelo: para predecir el retraso de la franja t solo se pueden usar
    datos de franjas anteriores. Las columnas de la propia franja t (pct_on_time,
    num_actualizaciones) no entran nunca como feature, porque salen de los mismos retrasos
    que forman el objetivo.

    gap_hours guarda cuantas horas separan las dos franjas. La ingesta no es continua, asi
    que la franja anterior observada no siempre es la hora inmediatamente previa: en vez de
    descartar esas filas, se le dice al modelo cuanto hueco hay.
    """
    ventana_de_la_linea = Window.partitionBy("route_id").orderBy("slot_ts")

    con_anterior = (
        franjas
        .withColumn("slot_ts_anterior", F.lag("slot_ts").over(ventana_de_la_linea))
        .withColumn("delay_lag_seconds", F.lag(COLUMNA_OBJETIVO).over(ventana_de_la_linea))
        .withColumn("pct_on_time_lag", F.lag("pct_on_time").over(ventana_de_la_linea))
        .withColumn(
            "num_updates_lag",
            F.lag("num_updates").over(ventana_de_la_linea),
        )
    )

    return con_anterior.withColumn(
        "gap_hours",
        (F.col("slot_ts").cast("long") - F.col("slot_ts_anterior").cast("long")) / 3600.0,
    )


def build_training_features(gold_line_reliability: DataFrame) -> DataFrame:
    """Conjunto de entrenamiento: una fila por linea y franja, con su objetivo.

    Se descarta la primera franja horaria de cada linea porque no tiene
    franja anterior de la que sacar las features.
    """
    con_slot = add_slot_timestamp(gold_line_reliability)
    con_anterior = add_previous_slot(con_slot)

    return (
        con_anterior
        # dayofweek: 1 = domingo, 7 = sabado. Distingue el sabado del CSD de un laborable.
        .withColumn("day_of_week", F.dayofweek("service_date"))
        .filter(F.col("delay_lag_seconds").isNotNull())
        .select(
            "route_id",
            "service_date",
            "slot_ts",
            "hour_of_day",
            "day_of_week",
            "delay_lag_seconds",
            "pct_on_time_lag",
            "num_updates_lag",
            "gap_hours",
            COLUMNA_OBJETIVO,
        )
    )


def build_scoring_features(gold_line_reliability: DataFrame) -> DataFrame:
    """Una fila por linea para predecir la franja siguiente a la ultima observada.

    Es lo que consume predict.py. No lleva objetivo, porque esa franja todavia no ha
    ocurrido. gap_hours vale 1 por construccion: se predice la hora inmediatamente
    posterior a la ultima franja con datos.
    """
    con_slot = add_slot_timestamp(gold_line_reliability)

    ventana_de_la_linea = Window.partitionBy("route_id").orderBy(F.col("slot_ts").desc())
    ultima_franja = (
        con_slot
        .withColumn("posicion", F.row_number().over(ventana_de_la_linea))
        .filter(F.col("posicion") == 1)
    )

    return (
        ultima_franja
        .withColumn("delay_lag_seconds", F.col(COLUMNA_OBJETIVO))
        .withColumn("pct_on_time_lag", F.col("pct_on_time"))
        .withColumn("num_updates_lag", F.col("num_updates"))
        # A partir de aqui slot_ts ya es la franja que se quiere predecir, no la observada.
        .withColumn("slot_ts", F.expr("timestampadd(HOUR, 1, slot_ts)"))
        .withColumn("service_date", F.to_date("slot_ts"))
        .withColumn("hour_of_day", F.hour("slot_ts"))
        .withColumn("day_of_week", F.dayofweek("service_date"))
        .withColumn("gap_hours", F.lit(1.0))
        .select(
            "route_id",
            "service_date",
            "slot_ts",
            "hour_of_day",
            "day_of_week",
            "delay_lag_seconds",
            "pct_on_time_lag",
            "num_updates_lag",
            "gap_hours",
        )
    )


def to_pandas_dataset(features: DataFrame) -> pd.DataFrame:
    """Pasa las features a pandas, ordenadas en el tiempo.

    Es el unico toPandas del proyecto.
    """
    return features.orderBy("slot_ts", "route_id").toPandas()


def split_temporal(dataset: pd.DataFrame, pct_entrenamiento: float = 0.8) -> tuple:
    """Parte el conjunto en entrenamiento y validacion por tiempo, nunca al azar.

    El split no es aleatorio porque entonces el modelo aprenderia del futuro. Se ordena
    por fecha.
    """
    ordenado = dataset.sort_values("slot_ts").reset_index(drop=True)
    franjas = sorted(ordenado["slot_ts"].unique())
    if len(franjas) < 2:
        raise RuntimeError(
            f"Solo hay {len(franjas)} franja(s) distinta(s): no se puede partir en el tiempo. "
            "Ingiere mas GTFS-RT antes de entrenar."
        )

    # Al menos una franja a cada lado del corte, pase lo que pase con el porcentaje.
    posicion_de_corte = int(len(franjas) * pct_entrenamiento)
    posicion_de_corte = max(1, min(posicion_de_corte, len(franjas) - 1))
    corte = franjas[posicion_de_corte - 1]

    entrenamiento = ordenado[ordenado["slot_ts"] <= corte]
    validacion = ordenado[ordenado["slot_ts"] > corte]
    print(
        f"[features] corte temporal en {corte}: "
        f"{len(entrenamiento)} filas de entrenamiento, {len(validacion)} de validacion"
    )
    return (entrenamiento, validacion)


if __name__ == "__main__":
    from multitudcsd.config import get_spark_session
    from multitudcsd.storage import read_delta

    sesion = get_spark_session("build-features")
    gold_line_reliability = read_delta(sesion, "gold", "gold_line_reliability")
    features = build_training_features(gold_line_reliability)
    print(f"[features] {features.count()} filas de entrenamiento disponibles")
    features.orderBy("slot_ts").show(10, truncate=False)
    sesion.stop()