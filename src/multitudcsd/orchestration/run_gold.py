""" Punto de entrada para ejecutar transforms silver to gold"""

#Imports

from multitudcsd.config import get_spark_session
from multitudcsd.storage import read_delta, write_gold
from multitudcsd.transforms.silver_to_gold import (
    build_gold_csd_activity,
    build_gold_disruptions_by_cell,
    build_gold_line_reliability,
    build_gold_mobility_pressure,
    build_gold_mobility_vs_activity,
    build_gold_station_services,
    build_gold_transit_capacity,
)

#Funciones

def build_mobility_pressure(spark) -> int:
    """Une bicis y retrasos por celda y hora, y escribe gold_mobility_pressure.
    Da una muestra por hora de las bicis y los retrasos
    """
    silver_bikes = read_delta(spark, "silver", "silver_bike_availability")
    silver_delays = read_delta(spark, "silver", "silver_transit_delays")
    gold = build_gold_mobility_pressure(silver_bikes, silver_delays)
    filas = gold.count()
    write_gold(gold, "gold_mobility_pressure")
    return filas


def build_line_reliability(spark) -> int:
    """Retraso medio y puntualidad por linea y hora, y escribe gold_line_reliability."""
    silver_delays = read_delta(spark, "silver", "silver_transit_delays")
    gold = build_gold_line_reliability(silver_delays)
    filas = gold.count()
    write_gold(gold, "gold_line_reliability")
    return filas


def build_disruptions_by_cell(spark) -> int:
    """Cortes activos en cada celda el dia seleccionado (CSD)
    escribe gold_disruptions_by_cell."""
    silver_disruptions = read_delta(spark, "silver", "silver_disruptions")
    gold = build_gold_disruptions_by_cell(silver_disruptions)
    filas = gold.count()
    write_gold(gold, "gold_disruptions_by_cell")
    return filas


def build_station_services(spark) -> int:
    """Servicios por estacion y linea, y escribe gold_station_services."""
    silver_transit_supply = read_delta(spark, "silver", "silver_transit_supply")
    gold = build_gold_station_services(silver_transit_supply)
    filas = gold.count()
    write_gold(gold, "gold_station_services")
    return filas


def build_transit_capacity(spark) -> int:
    """Oferta programada por celda H3, hora y modo, y escribe gold_transit_capacity."""
    silver_transit_supply = read_delta(spark, "silver", "silver_transit_supply")
    gold = build_gold_transit_capacity(silver_transit_supply)
    filas = gold.count()
    write_gold(gold, "gold_transit_capacity")
    return filas


def build_csd_activity(spark) -> int:
    """Actividad social por celda y hora con el umbral k=5 de menciones por celda
    y escribe gold_csd_activity.
    """
    silver_mentions = read_delta(spark, "silver", "silver_csd_mentions")
    gold = build_gold_csd_activity(silver_mentions)
    filas = gold.count()
    write_gold(gold, "gold_csd_activity")
    return filas


def build_mobility_vs_activity(spark) -> int:
    """Cruza actividad social con presion y capacidad de transporte,
    y escribe la tabla final.

    Lee las tres tablas Gold que la componen en vez de reconstruirlas: dentro de
    run_all_gold ya se han escrito en los pasos anteriores y
    releerlas evita calcular dos veces la misma agregacion.
    """
    gold_mobility_pressure = read_delta(spark, "gold", "gold_mobility_pressure")
    gold_transit_capacity = read_delta(spark, "gold", "gold_transit_capacity")
    gold_activity = read_delta(spark, "gold", "gold_csd_activity")

    gold = build_gold_mobility_vs_activity(
        gold_activity, gold_mobility_pressure, gold_transit_capacity
    )
    filas = gold.count()
    write_gold(gold, "gold_mobility_vs_activity")
    return filas


def run_all_gold(spark) -> dict:
    """Reconstruye toda la capa Gold y devuelve el nº de filas de cada tabla.

    si una tabla falla no se aborta el resto. El
    orden de la lista importa: mobility_vs_activity va el ultimo porque depende de que
    mobility_pressure, transit_capacity y csd_activity ya esten escritas.
    """
    resultados = {}

    for nombre, funcion_de_construccion in [
        ("mobility_pressure", build_mobility_pressure),
        ("line_reliability", build_line_reliability),
        ("disruptions_by_cell", build_disruptions_by_cell),
        ("station_services", build_station_services),
        ("transit_capacity", build_transit_capacity),
        ("csd_activity", build_csd_activity),
        ("mobility_vs_activity", build_mobility_vs_activity),
    ]:
        print(f"[run_gold] --- reconstruyendo '{nombre}' ---")
        try:
            resultados[nombre] = funcion_de_construccion(spark)
        except Exception as error:
            print(f"[run_gold] la tabla '{nombre}' ha fallado: {error}")
            resultados[nombre] = -1

    return resultados


if __name__ == "__main__":
    # Permite reconstruir toda la capa Gold desde PyCharm con el boton Run.
    sesion = get_spark_session("run-gold")
    resumen = run_all_gold(sesion)
    print(f"[run_gold] resumen de filas escritas: {resumen}")
    sesion.stop()