"""Tests Transforms Bronze -> Silver -> Gold."""

#Imports
import json
from datetime import datetime

from multitudcsd.config import FECHA_REFERENCIA_GTFS
from multitudcsd.transforms.bronze_to_silver import (
    build_active_service_ids,
    build_silver_bike_availability,
    build_silver_disruptions,
    build_silver_transit_delays,
    build_silver_transit_supply,
    extract_representative_point,
)
from multitudcsd.transforms.silver_to_gold import (
    build_gold_disruptions_by_cell,
    build_gold_line_reliability,
    build_gold_mobility_pressure,
    build_gold_station_services,
    build_gold_transit_capacity,
)

#Funciones
def _fila_calendar(service_id, saturday):
    return (service_id, saturday, "20260601", "20261231")


CAMPOS_CALENDAR = ["service_id", "saturday", "start_date", "end_date"]
CAMPOS_CALENDAR_DATES = ["service_id", "date", "exception_type"]


def test_servicios_activos_toma_los_de_sabado_dentro_del_rango(spark):
    #Crear un fake calendar de gtfs statico
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

    resultado = sorted(
        fila["service_id"] for fila in build_active_service_ids(calendar, calendar_dates).collect()
    )

    assert resultado == ["s_sabado"]


def test_servicios_activos_aplica_las_excepciones_del_dia(spark):
    """exception_type 1 anade un servicio de laborable; el 2 suprime uno de sabado."""
    # Crear un fake calendar de gtfs statico
    calendar = spark.createDataFrame(
        [_fila_calendar("s_sabado", "1"), _fila_calendar("s_refuerzo", "0")],
        CAMPOS_CALENDAR,
    )
    calendar_dates = spark.createDataFrame(
        [
            ("s_refuerzo", FECHA_REFERENCIA_GTFS, "1"),
            ("s_sabado", FECHA_REFERENCIA_GTFS, "2"),
        ],
        CAMPOS_CALENDAR_DATES,
    )

    resultado = sorted(
        fila["service_id"] for fila in build_active_service_ids(calendar, calendar_dates).collect()
    )

    assert resultado == ["s_refuerzo"]


def test_supply_descarta_los_viajes_que_no_circulan_ese_dia(spark):
    #Crear datasets para los test
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
    assert resultado[0]["transport_mode"] == "ubahn"
    assert resultado[0]["station_id"] == "p1"  # sin parent_station
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

def test_supply_clasifica_los_modos_del_feed_de_vbb(spark):
    """Los cinco route_type que VBB publica en la zona del CSD y mas uno desconocido."""
    stop_times = spark.createDataFrame(
        [("t_bus", "p1", "1", "10:00:00", "10:01:00"),
         ("t_sbahn", "p1", "1", "10:00:00", "10:01:00"),
         ("t_tram", "p1", "1", "10:00:00", "10:01:00"),
         ("t_regio", "p1", "1", "10:00:00", "10:01:00"),
         ("t_ubahn", "p1", "1", "10:00:00", "10:01:00"),
         ("t_raro", "p1", "1", "10:00:00", "10:01:00")],
        ["trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time"],
    )
    trips = spark.createDataFrame(
        [("t_bus", "r_bus", "s_sabado"),
         ("t_sbahn", "r_sbahn", "s_sabado"),
         ("t_tram", "r_tram", "s_sabado"),
         ("t_regio", "r_regio", "s_sabado"),
         ("t_ubahn", "r_ubahn", "s_sabado"),
         ("t_raro", "r_raro", "s_sabado")],
        ["trip_id", "route_id", "service_id"],
    )
    routes = spark.createDataFrame(
        [("r_bus", "100", "700"),
         ("r_sbahn", "S1", "109"),
         ("r_tram", "M10", "900"),
         ("r_regio", "RB63", "106"),
         ("r_ubahn", "U2", "400"),
         ("r_raro", "X99", "1501")],
        ["route_id", "route_short_name", "route_type"],
    )
    stops = spark.createDataFrame(
        [("p1", "Spittelmarkt", "", "52.5119", "13.4020")],
        ["stop_id", "stop_name", "parent_station", "stop_lat", "stop_lon"],
    )
    activos = spark.createDataFrame([("s_sabado",)], ["service_id"])

    resultado = build_silver_transit_supply(stop_times, trips, routes, stops, activos).collect()
    modos = {fila["route_name"]: fila["transport_mode"] for fila in resultado}

    assert modos == {
        "100": "bus",
        "S1": "sbahn",
        "M10": "tram",
        "RB63": "regio",
        "U2": "ubahn",
        "X99": "otro",   # codigo no visto en el feed: no rompe, cae en otro
    }

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

# Silver: bicis

def _fila_status(station_id, bikes, docks, last_reported):
    #funcion auxiliar
    """Bronze guarda el JSON de la estacion en payload_json."""
    return (json.dumps({
        "station_id": station_id,
        "num_bikes_available": bikes,
        "num_docks_available": docks,
        "is_renting": 1,
        "is_returning": 1,
        "last_reported": last_reported,
    }),)


def _fila_info(station_id, lat, lon, capacity):
    #funcion auxiliar
    return (json.dumps({
        "station_id": station_id, "lat": lat, "lon": lon, "capacity": capacity,
    }),)


def test_bikes_cruza_disponibilidad_con_ubicacion_y_geolocaliza(spark):
    status = spark.createDataFrame(
        [_fila_status("e1", 3, 5, 1757000000)], ["payload_json"]
    )
    info = spark.createDataFrame(
        [_fila_info("e1", "52.5163", "13.3777", 10)], ["payload_json"]
    )

    resultado = build_silver_bike_availability(status, info).collect()

    assert len(resultado) == 1
    assert resultado[0]["num_bikes_available"] == 3
    assert resultado[0]["capacity"] == 10
    assert resultado[0]["h3_index"] is not None


def test_bikes_descarta_la_estacion_sin_informacion_estatica(spark):
    """Inner join: sin lat/lon no se sabe donde esta la estacion. No se guarda"""
    status = spark.createDataFrame(
        [_fila_status("e1", 3, 5, 1757000000), _fila_status("e_fantasma", 1, 1, 1757000000)],
        ["payload_json"],
    )
    info = spark.createDataFrame([_fila_info("e1", "52.5163", "13.3777", 10)], ["payload_json"])

    resultado = build_silver_bike_availability(status, info).collect()

    assert [fila["station_id"] for fila in resultado] == ["e1"]


def test_bikes_deduplica_la_misma_lectura_repetida(spark):
    """La misma estacion con el mismo last_reported entra dos veces si se ingesta dos veces."""
    lectura = _fila_status("e1", 3, 5, 1757000000)
    status = spark.createDataFrame([lectura, lectura], ["payload_json"])
    info = spark.createDataFrame([_fila_info("e1", "52.5163", "13.3777", 10)], ["payload_json"])

    assert build_silver_bike_availability(status, info).count() == 1


# Silver: retrasos

def _fila_tripupdate(trip_id, route_id, stop_id, delay_llegada, delay_salida=None):
    actualizacion = {"stop_id": stop_id, "stop_sequence": 1}
    if delay_llegada is not None:
        actualizacion["arrival"] = {"delay": delay_llegada}
    if delay_salida is not None:
        actualizacion["departure"] = {"delay": delay_salida}
    return (
        json.dumps({
            "trip": {"trip_id": trip_id, "route_id": route_id, "start_date": "20260905"},
            "stop_time_update": [actualizacion],
        }),
        "1757000000",
    )


CAMPOS_TRIPUPDATE = ["payload_json", "feed_timestamp"]
CAMPOS_STOPS = ["stop_id", "stop_lat", "stop_lon"]


def test_delays_aplana_las_paradas_y_geolocaliza(spark):
    tripupdates = spark.createDataFrame(
        [_fila_tripupdate("t1", "U2", "p1", 120)], CAMPOS_TRIPUPDATE
    )
    stops = spark.createDataFrame([("p1", "52.5163", "13.3777")], CAMPOS_STOPS)

    resultado = build_silver_transit_delays(tripupdates, stops).collect()

    assert len(resultado) == 1
    assert resultado[0]["delay_seconds"] == 120
    assert resultado[0]["route_id"] == "U2"
    assert resultado[0]["h3_index"] is not None


def test_delays_usa_la_salida_si_no_hay_llegada(spark):
    """coalesce(arrival.delay, departure.delay): la primera parada no trae llegada."""
    tripupdates = spark.createDataFrame(
        [_fila_tripupdate("t1", "U2", "p1", None, delay_salida=90)], CAMPOS_TRIPUPDATE
    )
    stops = spark.createDataFrame([("p1", "52.5163", "13.3777")], CAMPOS_STOPS)

    assert build_silver_transit_delays(tripupdates, stops).collect()[0]["delay_seconds"] == 90


def test_delays_conserva_el_adelanto_como_valor_valido(spark):
    """Un retraso negativo es un tren adelantado, no un dato malo"""
    tripupdates = spark.createDataFrame(
        [_fila_tripupdate("t1", "U2", "p1", -45)], CAMPOS_TRIPUPDATE
    )
    stops = spark.createDataFrame([("p1", "52.5163", "13.3777")], CAMPOS_STOPS)

    assert build_silver_transit_delays(tripupdates, stops).collect()[0]["delay_seconds"] == -45


def test_delays_conserva_la_parada_fuera_de_la_zona_con_h3_nulo(spark):
    """Left join: si la parada no esta en el GTFS filtrado no tenemos posicion pero se queda el dato"""
    tripupdates = spark.createDataFrame(
        [_fila_tripupdate("t1", "U2", "p_lejana", 120)], CAMPOS_TRIPUPDATE
    )
    stops = spark.createDataFrame([("p1", "52.5163", "13.3777")], CAMPOS_STOPS)

    resultado = build_silver_transit_delays(tripupdates, stops).collect()

    assert len(resultado) == 1
    assert resultado[0]["h3_index"] is None


# Silver: cortes de trafico

def test_punto_representativo_de_una_geometria_simple():
    payload = json.dumps({"geometry": {"type": "Point", "coordinates": [13.3777, 52.5163]}})
    assert extract_representative_point(payload) == (13.3777, 52.5163)


def test_punto_representativo_dentro_de_una_geometrycollection():
    """VIZ mezcla el marcador (Point) con el tramo (LineString) en la misma geometria."""
    payload = json.dumps({"geometry": {"type": "GeometryCollection", "geometries": [
        {"type": "LineString", "coordinates": [[13.37, 52.51], [13.38, 52.52]]},
        {"type": "Point", "coordinates": [13.3777, 52.5163]},
    ]}})
    assert extract_representative_point(payload) == (13.3777, 52.5163)


def test_sin_punto_representativo_devuelve_nulos():
    payload = json.dumps({"geometry": {"type": "LineString", "coordinates": [[13.37, 52.51]]}})
    assert extract_representative_point(payload) == (None, None)


def test_geometria_ausente_devuelve_nulos():
    assert extract_representative_point(json.dumps({"properties": {}})) == (None, None)


def _fila_viz(disruption_id, desde, hasta):
    payload = {
        "geometry": {"type": "Point", "coordinates": [13.3777, 52.5163]},
        "properties": {
            "id": disruption_id, "tstore": "2026-09-01 10:00:00", "objectState": "new",
            "subtype": "Sperrung", "severity": "high", "street": "Ebertstr",
            "section": "a-b", "content": "gesperrt",
            "validity": {"from": desde, "to": hasta},
        },
    }
    return (disruption_id, json.dumps(payload))


def test_disruptions_tipa_las_fechas_y_geolocaliza(spark):
    bronze = spark.createDataFrame(
        [_fila_viz("d1", "01.09.2026 08:00", "30.09.2026 20:00")],
        ["disruption_id", "payload_json"],
    )

    resultado = build_silver_disruptions(bronze).collect()

    assert len(resultado) == 1
    assert resultado[0]["valid_from"].strftime("%Y-%m-%d %H:%M") == "2026-09-01 08:00"
    assert resultado[0]["subtype"] == "Sperrung"
    assert resultado[0]["h3_index"] is not None


def test_disruptions_deduplica_el_mismo_corte_ingestado_dos_veces(spark):
    fila = _fila_viz("d1", "01.09.2026 08:00", "30.09.2026 20:00")
    bronze = spark.createDataFrame([fila, fila], ["disruption_id", "payload_json"])

    assert build_silver_disruptions(bronze).count() == 1


# Gold

CAMPOS_DELAYS_GOLD = ["route_id", "h3_index", "delay_seconds", "feed_ts"]


def test_gold_line_reliability_calcula_la_puntualidad(spark):
    """Con umbral de 60 s: 30 s cuenta como puntual, 300 s no."""
    delays = spark.createDataFrame(
        [("U2", "celda_a", 30, datetime(2026, 9, 5, 14, 5)),
         ("U2", "celda_a", 300, datetime(2026, 9, 5, 14, 30))],
        "route_id string, h3_index string, delay_seconds int, feed_ts timestamp",
    )

    resultado = build_gold_line_reliability(delays).collect()[0]

    assert resultado["hour_of_day"] == 14
    assert resultado["avg_delay_seconds"] == 165.0
    assert resultado["pct_on_time"] == 0.5
    assert resultado["num_actualizaciones"] == 2


def test_gold_mobility_pressure_conserva_las_celdas_de_una_sola_fuente(spark):
    """Full outer join. Nos quedamos con todos los datos"""
    bikes = spark.createDataFrame(
        [("celda_bici", 4, 6, datetime(2026, 9, 5, 14, 0))],
        "h3_index string, num_bikes_available int, num_docks_available int, reading_ts timestamp",
    )
    delays = spark.createDataFrame(
        [("U2", "celda_tren", 30, datetime(2026, 9, 5, 14, 0))],
        "route_id string, h3_index string, delay_seconds int, feed_ts timestamp",
    )

    resultado = {fila["h3_index"]: fila for fila in
                 build_gold_mobility_pressure(bikes, delays).collect()}

    assert set(resultado) == {"celda_bici", "celda_tren"}
    assert resultado["celda_bici"]["avg_delay_seconds"] is None
    assert resultado["celda_tren"]["avg_bikes_available"] is None


def test_gold_mobility_pressure_descarta_los_retrasos_sin_celda(spark):
    """Un retraso sin h3_index no se puede situar: no entra en una tabla por celda."""
    bikes = spark.createDataFrame(
        [("celda_bici", 4, 6, datetime(2026, 9, 5, 14, 0))],
        "h3_index string, num_bikes_available int, num_docks_available int, reading_ts timestamp",
    )
    delays = spark.createDataFrame(
        [("U2", None, 30, datetime(2026, 9, 5, 14, 0))],
        "route_id string, h3_index string, delay_seconds int, feed_ts timestamp",
    )

    resultado = build_gold_mobility_pressure(bikes, delays).collect()

    assert [fila["h3_index"] for fila in resultado] == ["celda_bici"]


CAMPOS_DISRUPTIONS_GOLD = "h3_index string, valid_from timestamp, valid_to timestamp"


def test_gold_disruptions_solo_cuenta_los_cortes_vigentes_ese_dia(spark):
    disruptions = spark.createDataFrame(
        [("celda_a", datetime(2026, 8, 1), datetime(2026, 10, 1)),   # vigente
         ("celda_a", datetime(2026, 9, 5, 20), datetime(2026, 9, 6)),  # empieza ese dia: vale
         ("celda_b", datetime(2026, 1, 1), datetime(2026, 2, 1)),    # ya termino
         ("celda_b", datetime(2026, 12, 1), datetime(2026, 12, 31))],  # aun no empieza
        CAMPOS_DISRUPTIONS_GOLD,
    )

    resultado = {fila["h3_index"]: fila["num_disruptions"]
                 for fila in build_gold_disruptions_by_cell(disruptions).collect()}

    assert resultado == {"celda_a": 2}


def test_gold_disruptions_descarta_los_cortes_sin_celda(spark):
    disruptions = spark.createDataFrame(
        [(None, datetime(2026, 8, 1), datetime(2026, 10, 1))], CAMPOS_DISRUPTIONS_GOLD
    )

    assert build_gold_disruptions_by_cell(disruptions).count() == 0


def test_gold_station_services_agrega_por_estacion_y_linea(spark):
    supply = spark.createDataFrame(
        [("est_1", "metro", "U2", "Nollendorfplatz", "celda_a", 10),
         ("est_1", "metro", "U2", "Nollendorfplatz", "celda_a", 14),
         ("est_1", "metro", "U1", "Nollendorfplatz", "celda_a", 12)],
        ["station_id", "transport_mode", "route_name", "stop_name", "h3_index", "scheduled_hour"],
    )

    resultado = {(f["route_name"], f["num_scheduled_stops"], f["first_hour"], f["last_hour"])
                 for f in build_gold_station_services(supply).collect()}

    assert resultado == {("U2", 2, 10, 14), ("U1", 1, 12, 12)}