"""Servicio del modelo: puntua la siguiente franja de cada linea y la publica en Gold.

Servimos en una tabla Gold.
Una Api para ofrecer estos datos a las apps de citas seria el tier 3
"""

from pathlib import Path

import joblib
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from multitudcsd.config import get_models_root
from multitudcsd.ml.features import build_scoring_features, to_pandas_dataset
from multitudcsd.ml.train import NOMBRE_FICHERO_MODELO, select_columns

TABLA_GOLD = "gold_delay_predictions"

# Esquema explicito, como en el resto del proyecto: las predicciones vuelven de pandas y
# conviene fijar los tipos en vez de dejar que Spark los deduzca de numpy.
ESQUEMA_PREDICCIONES = StructType([
    StructField("route_id", StringType()),
    StructField("predicted_for_ts", TimestampType()),
    StructField("hour_of_day", IntegerType()),
    StructField("predicted_delay_seconds", DoubleType()),
    StructField("last_observed_delay_seconds", DoubleType()),
])


def load_model():
    """Carga el pipeline entrenado desde la carpeta de modelos del lakehouse."""
    ruta = Path(get_models_root()) / NOMBRE_FICHERO_MODELO
    if not ruta.exists():
        raise RuntimeError(f"No hay modelo en {ruta}. Ejecuta antes multitudcsd-train.")
    print(f"[predict] cargando modelo de {ruta}")
    return joblib.load(ruta)


def build_predictions(
    spark: SparkSession, gold_line_reliability: DataFrame, modelo
) -> DataFrame:
    """Predice el retraso de la franja siguiente de cada linea y lo devuelve como DataFrame.
    """
    features = to_pandas_dataset(build_scoring_features(gold_line_reliability))
    if features.empty:
        raise RuntimeError("No hay franjas en gold_line_reliability para puntuar")

    features["predicted_delay_seconds"] = modelo.predict(select_columns(features))

    # Se construyen tuplas de tipos Python nativos: pasar el DataFrame de pandas
    # directamente arrastra tipos de numpy que el esquema explicito rechaza.
    filas = [
        (
            str(fila.route_id),
            fila.slot_ts.to_pydatetime(),
            int(fila.hour_of_day),
            float(fila.predicted_delay_seconds),
            float(fila.delay_lag_seconds),
        )
        for fila in features.itertuples()
    ]

    predicciones = spark.createDataFrame(filas, schema=ESQUEMA_PREDICCIONES)
    print(f"[predict] {len(filas)} predicciones generadas")
    return (
        predicciones
        .withColumn("predicted_at", F.current_timestamp())
        # El objetivo sale de datos reales de VBB, no de las menciones sinteticas.
        .withColumn("source", F.lit("real"))
    )


if __name__ == "__main__":
    from multitudcsd.config import get_spark_session
    from multitudcsd.storage import read_delta, write_gold

    sesion = get_spark_session("predict-delays")
    gold_line_reliability = read_delta(sesion, "gold", "gold_line_reliability")
    write_gold(build_predictions(sesion, gold_line_reliability, load_model()), TABLA_GOLD)
    sesion.stop()