"""ingestar datos por un tiempo, para acumular datos reales de prueba."""

import time

from multitudcsd.config import get_spark_session
from multitudcsd.ingestion.gbfs import ingest_station_status
from multitudcsd.ingestion.gtfs_rt import ingest_trip_updates
from multitudcsd.ingestion.viz import ingest_disruptions

DURACION_TOTAL_SEGUNDOS = 1 * 60 * 60  # ventana de captura: 1 horas
INTERVALO_ENTRE_RONDAS_SEGUNDOS = 5 * 60  # esperar 5 minutos entre rondas de ingesta

# Cada fuente se aisla. Si hay un  fallo, las demas siguen en la misma ronda
FUENTES = [
    ("gtfs_rt", ingest_trip_updates),
    ("gbfs", ingest_station_status),
    ("viz", ingest_disruptions),
]


def run_round(spark, ronda: int) -> None:
    """Ejecuta una ronda de ingestion de las fuentes Bronze de la etapa 1."""
    for nombre, ingest_fn in FUENTES:
        try:
            filas = ingest_fn(spark)
            print(f"[collector] ronda {ronda} - {nombre}: {filas} filas")
        except RuntimeError as error:
            print(f"[collector] ronda {ronda} - {nombre} fallo, se sigue con la siguiente fuente: {error}")


def run_collection_window(duracion_segundos: int, intervalo_segundos: int) -> None:
    """Repite rondas de ingesta Bronze hasta agotar la ventana de tiempo indicada."""
    spark = get_spark_session("collect-bronze-window")
    inicio = time.time()
    ronda = 0

    try:
        while time.time() - inicio < duracion_segundos:
            ronda += 1
            transcurrido = int(time.time() - inicio)
            print(f"[collector] --- ronda {ronda} (transcurrido {transcurrido}s / {duracion_segundos}s) ---")
            run_round(spark, ronda)

            tiempo_restante = duracion_segundos - (time.time() - inicio)
            if tiempo_restante <= 0:
                break
            time.sleep(min(intervalo_segundos, tiempo_restante))
    finally:
        spark.stop()

    print(f"[collector] ventana terminada: {ronda} rondas en {int(time.time() - inicio)}s")


if __name__ == "__main__":
    run_collection_window(DURACION_TOTAL_SEGUNDOS, INTERVALO_ENTRE_RONDAS_SEGUNDOS)