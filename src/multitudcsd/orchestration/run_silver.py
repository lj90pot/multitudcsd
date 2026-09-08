""" Punto de entrada para ejecutar transforms bronze to silver"""

from multitudcsd.config import get_spark_session
from multitudcsd.storage import read_delta, write_silver
from multitudcsd.transforms.bronze_to_silver import (
    build_active_service_ids,
    build_silver_bike_availability,
    build_silver_disruptions,
    build_silver_mentions,
    build_silver_transit_delays,
    build_silver_transit_supply,
)


def build_bike_availability(spark) -> int:
    """Lee Bronze de Nextbike, construye Silver y la escribe. Devuelve el nº de filas.
    esta tabla contiene las bicis en las estaciones y las localiza en h3
    """
    bronze_status = read_delta(spark, "bronze", "bronze_nextbike_status")
    bronze_info = read_delta(spark, "bronze", "bronze_nextbike_station_information")
    silver = build_silver_bike_availability(bronze_status, bronze_info)
    filas = silver.count()
    write_silver(silver, "silver_bike_availability")
    return filas


def build_transit_delays(spark) -> int:
    """Lee Bronze de GTFS-RT y de paradas, construye Silver y la escribe.
    Esta tabla contiene los retrasos por parada, linea y ruta
    """
    bronze_tripupdates = read_delta(spark, "bronze", "bronze_gtfs_tripupdates")
    bronze_stops = read_delta(spark, "bronze", "bronze_gtfs_static_stops")
    silver = build_silver_transit_delays(bronze_tripupdates, bronze_stops)
    filas = silver.count()
    write_silver(silver, "silver_transit_delays")
    return filas


def build_disruptions(spark) -> int:
    """Lee Bronze de VIZ, construye Silver y la escribe.
    Cortes con sus fechas, dedup y punto representativo y en la celda H3
    """
    bronze_viz = read_delta(spark, "bronze", "bronze_viz_disruptions")
    silver = build_silver_disruptions(bronze_viz)
    filas = silver.count()
    write_silver(silver, "silver_disruptions")
    return filas


def build_transit_supply(spark) -> int:
    """Reconstruye la cadena completa del GTFS estatico y escribe la oferta programada.

    Obtiene la oferta del dia del archivo gtfs estático.
    Filtrado a la zona del evento mas un margen.

    Necesita seis tablas Bronze (stop_times, trips, routes, stops, calendar y
    calendar_dates), porque el GTFS estatico reparte la informacion en varios ficheros.
    """
    bronze_stop_times = read_delta(spark, "bronze", "bronze_gtfs_static_stop_times")
    bronze_trips = read_delta(spark, "bronze", "bronze_gtfs_static_trips")
    bronze_routes = read_delta(spark, "bronze", "bronze_gtfs_static_routes")
    bronze_stops = read_delta(spark, "bronze", "bronze_gtfs_static_stops")
    bronze_calendar = read_delta(spark, "bronze", "bronze_gtfs_static_calendar")
    bronze_calendar_dates = read_delta(spark, "bronze", "bronze_gtfs_static_calendar_dates")

    servicios_activos = build_active_service_ids(bronze_calendar, bronze_calendar_dates)
    print(f"[run_silver] {servicios_activos.count()} servicios activos el dia del CSD")

    silver = build_silver_transit_supply(
        bronze_stop_times, bronze_trips, bronze_routes, bronze_stops, servicios_activos
    )
    filas = silver.count()
    write_silver(silver, "silver_transit_supply")
    return filas


def build_mentions(spark) -> int:
    """Lee Bronze de menciones sinteticas (tier 2), construye Silver y la escribe.

    Menciones con su hora z su celda H3
    bronze_csd_mentions existe una vez lanzado el stream del tier 2
    """
    bronze_mentions = read_delta(spark, "bronze", "bronze_csd_mentions")
    silver = build_silver_mentions(bronze_mentions)
    filas = silver.count()
    write_silver(silver, "silver_csd_mentions")
    return filas


def run_all_silver(spark) -> dict:
    """Reconstruye toda la capa Silver y devuelve el nº de filas de cada tabla.

    si una tabla falla no se aborta el resto, porque interesa
    mas terminar con las demas tablas Silver reconstruidas que quedarse a medias. Aqui
    se captura Exception en vez de RuntimeError
    """
    resultados = {}

    for nombre, funcion_de_construccion in [
        ("bike_availability", build_bike_availability),
        ("transit_delays", build_transit_delays),
        ("disruptions", build_disruptions),
        ("transit_supply", build_transit_supply),
        ("mentions", build_mentions),
    ]:
        print(f"[run_silver] --- reconstruyendo '{nombre}' ---")
        try:
            resultados[nombre] = funcion_de_construccion(spark)
        except Exception as error:
            print(f"[run_silver] la tabla '{nombre}' ha fallado: {error}")
            resultados[nombre] = -1

    return resultados

#Main preparado asi para que funcione el pyproject.toml
def main() -> None:
    """Reconstruye toda la capa Silver. Lo invocan el Makefile en local y el Job en Databricks."""
    sesion = get_spark_session("run-silver")
    resumen = run_all_silver(sesion)
    print(f"[run_silver] resumen de filas escritas: {resumen}")
    sesion.stop()


if __name__ == "__main__":
    main()