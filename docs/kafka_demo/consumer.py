#Consumidor de la demo de Kafka que escribe lo leido en una tabla delta local

import json
from pathlib import Path

from confluent_kafka import Consumer
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

TOPIC = "kafka_demo"
CARPETA_LAKEHOUSE = Path(__file__).parent / "demo_lakehouse"
TABLA_BRONZE = "bronze_kafka_demo"
MAX_MENSAJES = 3  # mismo limite que consumer.py, solo para que la demo termine sola

# Esquema explicito, igual que en la tabla real bronze_csd_mentions: nunca inferido.
ESQUEMA_MENSAJE = StructType([
    StructField("mention_id", StringType()),
    StructField("platform", StringType()),
])


def get_spark_session() -> SparkSession:
    """Crea una SparkSession local con soporte Delta, autocontenida para esta demo."""
    from multitudcsd.config import prepare_windows_hadoop

    prepare_windows_hadoop()  # deja HADOOP_HOME y PATH listos en Windows

    constructor = (
        SparkSession.builder.appName("kafka-demo-consumer")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(constructor).getOrCreate()


def read_pending_messages(consumidor: Consumer, max_mensajes: int) -> list[dict]:
    """Lee hasta max_mensajes del topic y los devuelve como lista de dicts."""
    mensajes = []
    while len(mensajes) < max_mensajes:
        mensaje = consumidor.poll(timeout=5.0)
        if mensaje is None:
            break
        if mensaje.error():
            print(f"[consumer] error: {mensaje.error()}")
            continue
        valor = json.loads(mensaje.value().decode("utf-8"))
        print(f"[consumer] recibido: {valor}")
        mensajes.append(valor)
    return mensajes


def write_to_delta(spark: SparkSession, mensajes: list[dict]) -> int:
    """Escribe los mensajes leidos en bronze_kafka_demo, en modo append."""
    if not mensajes:
        print("[consumer] no hay mensajes que escribir")
        return 0

    dataframe = spark.createDataFrame(mensajes, schema=ESQUEMA_MENSAJE)
    ruta = str(CARPETA_LAKEHOUSE / TABLA_BRONZE)
    dataframe.write.format("delta").mode("append").save(ruta)

    filas_escritas = dataframe.count()
    print(f"[consumer] {filas_escritas} filas escritas en {ruta}")
    return filas_escritas


def main() -> None:
    """Lee lo pendiente del topic y lo escribe en Delta. Termina, no se queda ejecutando."""
    consumidor = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "kafka_demo_group",
        "auto.offset.reset": "earliest",
    })
    consumidor.subscribe([TOPIC])

    print("[consumer] leyendo mensajes pendientes...")
    mensajes = read_pending_messages(consumidor, MAX_MENSAJES)
    consumidor.close()

    sesion = get_spark_session()
    write_to_delta(sesion, mensajes)
    sesion.stop()

    print(f"[consumer] listo, {len(mensajes)} mensajes leidos y guardados")


if __name__ == "__main__":
    main()