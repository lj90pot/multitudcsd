from multitudcsd.transforms.bronze_to_silver import (
    build_active_service_ids,
    build_silver_transit_supply,
)
from multitudcsd.transforms.silver_to_gold import build_gold_transit_capacity

def _fila_calendar(service_id, saturday):
    return (service_id, saturday, "20260601", "20261231")


CAMPOS_CALENDAR = ["service_id", "saturday", "start_date", "end_date"]
CAMPOS_CALENDAR_DATES = ["service_id", "date", "exception_type"]


def test_servicios_activos_toma_los_de_sabado_dentro_del_rango(spark):
    calendar = spark.createDataFrame(
        [
            _fila_calendar("s_sabado", "1"),
            _fila_calendar("s_laborable", "0"),
            ("s_caducado", "1", "20250101", "20251231"),
        ],
        CAMPOS_CALENDAR,
    )
    calendar_dates = spark.createDataFrame([], schema=", ".join(
        f"{campo} string" for campo in CAMPOS_CALENDAR_DATES
    ))

    resultado = [fila["service_id"] for fila in build_active_service_ids(calendar, calendar_dates).collect()]

    assert resultado == ["s_sabado"]


def test_servicios_activos_aplica_las_excepciones_del_dia(spark):
    """exception_type 1 anade un servicio de laborable; el 2 suprime uno de sabado."""
    calendar = spark.createDataFrame(
        [_fila_calendar("s_sabado", "1"), _fila_calendar("s_refuerzo", "0")],
        CAMPOS_CALENDAR,
    )
    calendar_dates = spark.createDataFrame(
        [("s_refuerzo", "20260725", "1"), ("s_sabado", "20260725", "2")],
        CAMPOS_CALENDAR_DATES,
    )

    resultado = [fila["service_id"] for fila in build_active_service_ids(calendar, calendar_dates).collect()]

    assert resultado == ["s_refuerzo"]


def test_supply_descarta_los_viajes_que_no_circulan_ese_dia(spark):
    stop_times = spark.createDataFrame(
        [("t_sabado", "p1", "1", "10:05:00", "10:06:00"),
         ("t_laborable", "p1", "1", "10:15:00", "10:16:00")],
        ["trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time"],
    )
    trips = spark.createDataFrame(
        [("t_sabado", "U2", "s_sabado"), ("t_laborable", "U2", "s_laborable")],
        ["trip_id", "route_id", "service_id"],
    )
    routes = spark.createDataFrame([("U2", "U2", "400")], ["route_id", "route_short_name", "route_type"])
    stops = spark.createDataFrame(
        [("p1", "Nollendorfplatz", "", "52.4996", "13.3540")],
        ["stop_id", "stop_name", "parent_station", "stop_lat", "stop_lon"],
    )
    activos = spark.createDataFrame([("s_sabado",)], ["service_id"])

    resultado = build_silver_transit_supply(stop_times, trips, routes, stops, activos).collect()

    assert len(resultado) == 1
    assert resultado[0]["trip_id"] == "t_sabado"
    assert resultado[0]["transport_mode"] == "metro"
    assert resultado[0]["station_id"] == "p1"  # sin parent_station, cae al propio stop_id
    assert resultado[0]["h3_index"] is not None


def test_supply_normaliza_las_horas_gtfs_mayores_de_24(spark):
    """Un servicio nocturno a las 25:10 pertenece a la franja de la 1 de la madrugada."""
    stop_times = spark.createDataFrame(
        [("t_noche", "p1", "1", "25:10:00", "25:11:00")],
        ["trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time"],
    )
    trips = spark.createDataFrame([("t_noche", "N2", "s_sabado")], ["trip_id", "route_id", "service_id"])
    routes = spark.createDataFrame([("N2", "N2", "700")], ["route_id", "route_short_name", "route_type"])
    stops = spark.createDataFrame(
        [("p1", "Spittelmarkt", "estacion_padre", "52.5119", "13.4020")],
        ["stop_id", "stop_name", "parent_station", "stop_lat", "stop_lon"],
    )
    activos = spark.createDataFrame([("s_sabado",)], ["service_id"])

    resultado = build_silver_transit_supply(stop_times, trips, routes, stops, activos).collect()

    assert resultado[0]["scheduled_hour"] == 1
    assert resultado[0]["station_id"] == "estacion_padre"


def test_gold_capacity_agrega_por_celda_franja_y_modo(spark):
    supply = spark.createDataFrame(
        [("celda_a", 10, "bus", "100", "est_1"),
         ("celda_a", 10, "bus", "200", "est_1"),
         ("celda_a", 11, "bus", "100", "est_1")],
        ["h3_index", "scheduled_hour", "transport_mode", "route_name", "station_id"],
    )

    resultado = {(f["scheduled_hour"], f["num_scheduled_stops"], f["num_routes"])
                 for f in build_gold_transit_capacity(supply).collect()}

    assert resultado == {(10, 2, 2), (11, 1, 1)}