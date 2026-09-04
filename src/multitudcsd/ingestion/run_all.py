"""Lanzador de todas las ingestas de la etapa 1 hacia la capa Bronze."""

from multitudcsd.config import get_spark_session
from multitudcsd.ingestion.gbfs import ingest_station_status
from multitudcsd.ingestion.gtfs_rt import ingest_trip_updates
from multitudcsd.ingestion.gtfs_static import ingestar_gtfs_estatico
from multitudcsd.ingestion.viz import ingest_disruptions

# El GTFS estatico pesa mucho y cambia poco, asi que no se lanza en cada pasada.
INGESTAR_GTFS_ESTATICO = False


def run_all_ingestions(spark) -> dict:
    """Ejecuta las ingestas de la etapa 1 y devuelve el nº de filas de cada fuente.

    Si una fuente falla no se aborta el resto: interesa mas terminar con las demas
    tablas Bronze pobladas que quedarse a medias porque un feed externo este caido.
    """
    resultados = {}

    for nombre, funcion_de_ingesta in [
        ("gtfs_rt", ingest_trip_updates),
        ("gbfs", ingest_station_status),
        ("viz", ingest_disruptions),
    ]:
        print(f"[run_all] --- iniciando ingesta '{nombre}' ---")
        try:
            resultados[nombre] = funcion_de_ingesta(spark)
        except RuntimeError as error:
            print(f"[run_all] la ingesta '{nombre}' ha fallado: {error}")
            resultados[nombre] = -1

    if INGESTAR_GTFS_ESTATICO:
        print("[run_all] --- iniciando ingesta 'gtfs_static' ---")
        ingestar_gtfs_estatico(spark)
        resultados["gtfs_static"] = 0

    return resultados


if __name__ == "__main__":
    # Permite ejecutar toda la ingesta desde PyCharm con el boton Run.
    sesion = get_spark_session("ingest-all")
    resumen = run_all_ingestions(sesion)
    print(f"[run_all] resumen de filas escritas: {resumen}")
    sesion.stop()