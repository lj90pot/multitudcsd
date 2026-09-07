"""Transformaciones Bronze -> Silver del tier 1 (transporte).

Cada build_silver_* es una funcion: recibe DataFrame de Bronze ya leidos y
devuelve el DataFrame de Silver correspondiente. Eso permite testear con datos de ejemplo
sin depender de que exista Bronze en el filesystem.
"""

#Imports
import json
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from multitudcsd.transforms.geo import add_h3_index
from multitudcsd.config import DIA_SEMANA_REFERENCIA, FECHA_REFERENCIA_GTFS

# BICIS
## Esquemas para parsear el payload_json de Bronze (GBFS)

ESQUEMA_STATUS_JSON = StructType([
    StructField("station_id", StringType()),
    StructField("num_bikes_available", IntegerType()),
    StructField("num_docks_available", IntegerType()),
    StructField("is_renting", IntegerType()),
    StructField("is_returning", IntegerType()),
    StructField("last_reported", LongType()),  # segundos desde epoch
])

ESQUEMA_INFO_JSON = StructType([
    StructField("station_id", StringType()),
    # lat/lon como string: algunos operadores GBFS los mandan como texto, no numero.
    # Se tranforman a double al seleccionar.
    StructField("lat", StringType()),
    StructField("lon", StringType()),
    StructField("capacity", IntegerType()),
])

#Funciones
def build_silver_bike_availability(
    bronze_status: DataFrame, bronze_info: DataFrame
) -> DataFrame:
    """Cruza disponibilidad (station_status) con ubicacion (station_information).

    Devuelve una fila por lectura de estacion, con h3_index calculado a partir de la
    ubicacion estatica de la estacion. Inner join: una estacion sin informacion
    estatica se elimina, ya que no tiene sentido inventarse donde está.
    """
    status_tipado = (
        bronze_status
        .withColumn("datos", F.from_json("payload_json", ESQUEMA_STATUS_JSON))
        .select(
            F.col("datos.station_id").alias("station_id"),
            F.col("datos.num_bikes_available").alias("num_bikes_available"),
            F.col("datos.num_docks_available").alias("num_docks_available"),
            F.col("datos.is_renting").alias("is_renting"),
            F.col("datos.is_returning").alias("is_returning"),
            F.col("datos.last_reported").cast("timestamp").alias("reading_ts"),
        )
        .dropDuplicates(["station_id", "reading_ts"])
    )

    info_tipado = (
        bronze_info
        .withColumn("datos", F.from_json("payload_json", ESQUEMA_INFO_JSON))
        .select(
            F.col("datos.station_id").alias("station_id"),
            F.col("datos.lat").cast("double").alias("lat"),
            F.col("datos.lon").cast("double").alias("lon"),
            F.col("datos.capacity").alias("capacity"),
        )
        .dropDuplicates(["station_id"])
    )

    silver = status_tipado.join(info_tipado, on="station_id", how="inner")
    return add_h3_index(silver, lat_col="lat", lon_col="lon")

# OPNV
## Esquemas para parsear el protobuf de Bronze (GTFS)
ESQUEMA_TRIP_UPDATE_JSON = StructType([
    StructField("trip", StructType([
        StructField("trip_id", StringType()),
        StructField("route_id", StringType()),
        StructField("start_date", StringType()),
    ])),
    StructField("stop_time_update", ArrayType(StructType([
        StructField("stop_id", StringType()),
        StructField("stop_sequence", IntegerType()),
        StructField("arrival", StructType([
            StructField("delay", IntegerType()),
        ])),
        StructField("departure", StructType([
            StructField("delay", IntegerType()),
        ])),
    ]))),
])


def build_silver_transit_delays(
    bronze_tripupdates: DataFrame, bronze_stops: DataFrame
) -> DataFrame:
    """Aplana los trip_update de GTFS-RT a una fila por parada, con retraso y h3_index.

    Cruza por stop_id con las paradas del GTFS estatico (bronze_gtfs_static_stops,
    ya filtradas a la zona del CSD por gtfs_static.py) para obtener la coordenada de
    la parada. Join left: si una parada no aparece en el estatico filtrado, el retraso
    se conserva igualmente pero con h3_index nulo (no se pierde el dato, se pierde solo
    la posicion). Un retraso negativo (el tren va adelantado) es un valor valido, no se
    filtra.
    """
    parseado = bronze_tripupdates.withColumn(
        "datos", F.from_json("payload_json", ESQUEMA_TRIP_UPDATE_JSON)
    )

    explotado = parseado.select(
        F.col("datos.trip.trip_id").alias("trip_id"),
        F.col("datos.trip.route_id").alias("route_id"),
        F.explode("datos.stop_time_update").alias("actualizacion"),
        F.col("feed_timestamp").cast("long").alias("feed_timestamp"),
    )

    retrasos = (
        explotado
        .select(
            "trip_id",
            "route_id",
            F.col("actualizacion.stop_id").alias("stop_id"),
            F.coalesce(
                F.col("actualizacion.arrival.delay"),
                F.col("actualizacion.departure.delay"),
            ).alias("delay_seconds"),
            F.from_unixtime("feed_timestamp").cast("timestamp").alias("feed_ts"),
        )
        .filter(F.col("delay_seconds").isNotNull())
        .dropDuplicates(["trip_id", "stop_id", "feed_ts"])
    )

    paradas = (
        bronze_stops
        .select(
            F.col("stop_id"),
            F.col("stop_lat").cast("double").alias("lat"),
            F.col("stop_lon").cast("double").alias("lon"),
        )
        .dropDuplicates(["stop_id"])
    )

    silver = retrasos.join(paradas, on="stop_id", how="left")
    return add_h3_index(silver, lat_col="lat", lon_col="lon")

# CORTES
ESQUEMA_PUNTO = StructType([
    StructField("lon", DoubleType()),
    StructField("lat", DoubleType()),
])


def extract_representative_point(payload_json: str) -> tuple:
    """Extrae (lon, lat) de la geometria del payload VIZ. (None, None) si no hay punto.

    El feed mezcla geometrias simples con GeometryCollection (marcador + linea del
    tramo). Nos basta un punto representativo para el h3_index:
      - si geometry.type es 'Point', se usa directamente.
      - si es 'GeometryCollection', se busca el primer 'Point' dentro de 'geometries'.
      - en cualquier otro caso (un LineString o Polygon sueltos, sin Point dentro), no
        hay punto representativo y se devuelve (None, None).
    """
    datos = json.loads(payload_json)
    geometria = datos.get("geometry") or {}
    tipo = geometria.get("type")

    if tipo == "Point":
        lon, lat = geometria["coordinates"]
        return (lon, lat)

    if tipo == "GeometryCollection":
        for sub_geometria in geometria.get("geometries", []):
            if sub_geometria.get("type") == "Point":
                lon, lat = sub_geometria["coordinates"]
                return (lon, lat)

    return (None, None)


_extraer_punto_udf = F.udf(extract_representative_point, ESQUEMA_PUNTO)

# properties.validity.{from,to}: fecha de inicio/fin del corte, formato "2026.03.12 12:00"
ESQUEMA_VIZ_FEATURE_JSON = StructType([
    StructField("properties", StructType([
        StructField("id", StringType()),
        StructField("tstore", StringType()),       # instante en que VIZ publica esta version
        StructField("objectState", StringType()),  # "new" | "modified" | ...
        StructField("subtype", StringType()),      # "Sperrung" (corte) | "Baustelle" (obra)
        StructField("icon", StringType()),
        StructField("severity", StringType()),
        StructField("street", StringType()),
        StructField("section", StringType()),
        StructField("content", StringType()),      # descripcion libre: "gesperrt, Demonstration"
        StructField("validity", StructType([
            StructField("from", StringType()),
            StructField("to", StringType()),
        ])),
    ])),
])

FORMATO_FECHA_VIZ = "dd.MM.yyyy HH:mm" #Formato aleman de fecha


def build_silver_disruptions(bronze_viz: DataFrame) -> DataFrame:
    """Contruye los puntos de incidencias, con punto representativo, ventana de vigencia y h3_index.

    disruption_id ya viene desde Bronze (columna propia).
    dropDuplicates por disruption_id: el mismo corte se
    vuelve a descargar con una nueva ingesta mientras siga activo asi que lo eliminamos,
    solo necesitamos una fila por incidencia
    """
    con_punto = bronze_viz.withColumn("punto", _extraer_punto_udf(F.col("payload_json")))
    parseado = con_punto.withColumn(
        "datos", F.from_json("payload_json", ESQUEMA_VIZ_FEATURE_JSON)
    )
    propiedades = F.col("datos.properties")
    validez = propiedades.getField("validity")

    silver = parseado.select(
        "disruption_id",
        propiedades.getField("subtype").alias("subtype"),
        propiedades.getField("severity").alias("severity"),
        propiedades.getField("street").alias("street"),
        propiedades.getField("section").alias("section"),
        propiedades.getField("content").alias("content"),
        propiedades.getField("objectState").alias("object_state"),
        propiedades.getField("tstore").cast("timestamp").alias("published_ts"),
        F.to_timestamp(validez.getField("from"), FORMATO_FECHA_VIZ).alias("valid_from"),
        F.to_timestamp(validez.getField("to"), FORMATO_FECHA_VIZ).alias("valid_to"),
        F.col("punto.lon").alias("lon"),
        F.col("punto.lat").alias("lat"),
    ).dropDuplicates(["disruption_id"])

    return add_h3_index(silver, lat_col="lat", lon_col="lon")

def build_active_service_ids(
    bronze_calendar: DataFrame, bronze_calendar_dates: DataFrame
) -> DataFrame:
    """Devuelve los service_id que circulan el dia del CSD, una columna, sin duplicados.

    GTFS separa el patron semanal (calendar.txt: "circula los sabados entre estas dos
    fechas") de las excepciones puntuales (calendar_dates.txt: exception_type 1 anade
    un dia suelto, 2 lo suprime). Sin este filtro, contar la oferta usando todos los
    trip_id mezclaria servicios de laborable, de sabado y de periodos de obras inflando
     el numero de pasos programados.
    """
    servicios_regulares = (
        bronze_calendar
        .filter(F.col(DIA_SEMANA_REFERENCIA) == "1")
        .filter(F.col("start_date") <= F.lit(FECHA_REFERENCIA_GTFS))
        .filter(F.col("end_date") >= F.lit(FECHA_REFERENCIA_GTFS))
        .select("service_id")
    )

    excepciones_del_dia = bronze_calendar_dates.filter(F.col("date") == F.lit(FECHA_REFERENCIA_GTFS))
    servicios_anadidos = excepciones_del_dia.filter(F.col("exception_type") == "1").select("service_id")
    servicios_suprimidos = excepciones_del_dia.filter(F.col("exception_type") == "2").select("service_id")

    # left_anti = quedate con rows de la izquierda que no estan en la derecha
    return (
        servicios_regulares
        .union(servicios_anadidos)
        .distinct()
        .join(servicios_suprimidos, on="service_id", how="left_anti")
    )

#Codigos comprobados sobre Bronze. Codigos google/hvt.
ROUTE_TYPES_BUS = ("700",)
ROUTE_TYPES_SBAHN = ("109",)
ROUTE_TYPES_TRAM = ("900",)
ROUTE_TYPES_REGIO = ("100", "106")
ROUTE_TYPES_UBAHN = ("400",)


def build_silver_transit_supply(
    bronze_stop_times: DataFrame,
    bronze_trips: DataFrame,
    bronze_routes: DataFrame,
    bronze_stops: DataFrame,
    servicios_activos: DataFrame,
) -> DataFrame:
    """Oferta de transporte programada: una fila por viaje, parada y hora del dia del CSD.

    Reconstruye la cadena stop_times -> trips -> routes -> stops que el GTFS deja
    repartida en cuatro ficheros, y se queda solo con los viajes cuyo service_id
    circula el dia del evento. El grano es el paso programado (un vehiculo parando
    en una parada a una hora), que es el nivel mas fino desde el que se puede agregar
    despues por estacion o por celda.
    """
    horarios = (
        bronze_stop_times
        .select(
            "trip_id",
            "stop_id",
            F.col("stop_sequence").cast("int").alias("stop_sequence"),
            # Alguna parada intermedia puede venir sin arrival_time: se usa la salida.
            F.coalesce(F.col("arrival_time"), F.col("departure_time")).alias("hora_programada"),
        )
        .dropDuplicates(["trip_id", "stop_id", "stop_sequence"])
    )

    # GTFS admite horas >= 24 para el servicio nocturno que es del dia anterior
    # ("25:10:00" son las 01:10 de la madrugada siguiente). Por eso no se puede transformar
    # a timestamp: se extrae la hora y se lleva al rango 0-23 con un modulo.
    horarios = horarios.withColumn(
        "scheduled_hour",
        F.split(F.col("hora_programada"), ":").getItem(0).cast("int") % 24,
    ).filter(F.col("scheduled_hour").isNotNull())

    viajes = bronze_trips.select("trip_id", "route_id", "service_id")
    lineas = bronze_routes.select(
        "route_id",
        F.col("route_short_name").alias("route_name"),
        "route_type",
    )

    paradas = (
        bronze_stops
        .select(
            "stop_id",
            "stop_name",
            # parent_station puede llegar como cadena vacia en vez de nulo desde el csv.
            F.when(
                F.col("parent_station").isNull() | (F.col("parent_station") == ""),
                F.col("stop_id"),
            ).otherwise(F.col("parent_station")).alias("station_id"),
            F.col("stop_lat").cast("double").alias("lat"),
            F.col("stop_lon").cast("double").alias("lon"),
        )
        .dropDuplicates(["stop_id"])
    )

    unido = (
        horarios
        .join(viajes, on="trip_id", how="inner")
        .join(servicios_activos, on="service_id", how="inner")
        .join(lineas, on="route_id", how="left")
        .join(paradas, on="stop_id", how="inner")
    )

    con_modo = unido.withColumn(
        "transport_mode",
        F.when(F.col("route_type").isin(*ROUTE_TYPES_BUS), "bus")
        .when(F.col("route_type").isin(*ROUTE_TYPES_SBAHN), "sbahn")
        .when(F.col("route_type").isin(*ROUTE_TYPES_TRAM), "tram")
        .when(F.col("route_type").isin(*ROUTE_TYPES_REGIO), "regio")
        .when(F.col("route_type").isin(*ROUTE_TYPES_UBAHN), "ubahn")
        .otherwise("otro"),
    ).withColumn("source", F.lit("real"))

    return add_h3_index(con_modo, lat_col="lat", lon_col="lon")

def build_silver_mentions(bronze_mentions: DataFrame) -> DataFrame:
    """Silver para las menciones, y les anade celda H3 y franja horaria.

    Bronze ya guarda las columnas con tipos porque el stream lee con esquema explicito, asi
    que aqui solo queda convertir event_ts a timestamp, quitar repetidos y geolocalizar.
    """
    tipado = (
        bronze_mentions
        .select(
            "mention_id",
            F.to_timestamp("event_ts").alias("event_ts"),
            "lat",
            "lon",
            "platform",
            "language",
            "sentiment",
            "has_media",
            "user_hash",
            "source",
        )
        .dropDuplicates(["mention_id"])
        .withColumn("hour_of_day", F.hour("event_ts"))
    )
    return add_h3_index(tipado, lat_col="lat", lon_col="lon")

#Bloque para ejecutar en pycharm local
if __name__ == "__main__":
    from multitudcsd.config import get_spark_session
    from multitudcsd.storage import read_delta, write_silver

    sesion = get_spark_session("bronze-to-silver")

    bronze_status = read_delta(sesion, "bronze", "bronze_nextbike_status")
    bronze_info = read_delta(sesion, "bronze", "bronze_nextbike_station_information")
    write_silver(build_silver_bike_availability(bronze_status, bronze_info), "silver_bike_availability")

    bronze_tripupdates = read_delta(sesion, "bronze", "bronze_gtfs_tripupdates")
    bronze_stops = read_delta(sesion, "bronze", "bronze_gtfs_static_stops")
    write_silver(build_silver_transit_delays(bronze_tripupdates, bronze_stops), "silver_transit_delays")

    bronze_viz = read_delta(sesion, "bronze", "bronze_viz_disruptions")
    write_silver(build_silver_disruptions(bronze_viz), "silver_disruptions")

    bronze_stop_times = read_delta(sesion, "bronze", "bronze_gtfs_static_stop_times")
    bronze_trips = read_delta(sesion, "bronze", "bronze_gtfs_static_trips")
    bronze_routes = read_delta(sesion, "bronze", "bronze_gtfs_static_routes")
    bronze_stops = read_delta(sesion, "bronze", "bronze_gtfs_static_stops")
    bronze_calendar = read_delta(sesion, "bronze", "bronze_gtfs_static_calendar")
    bronze_calendar_dates = read_delta(sesion, "bronze", "bronze_gtfs_static_calendar_dates")

    servicios_activos = build_active_service_ids(bronze_calendar, bronze_calendar_dates)
    print(f"[silver] {servicios_activos.count()} servicios activos el dia del CSD")

    supply = build_silver_transit_supply(
        bronze_stop_times, bronze_trips, bronze_routes, bronze_stops, servicios_activos
    )
    write_silver(supply, "silver_transit_supply")

    bronze_mentions = read_delta(sesion, "bronze", "bronze_mentions")
    write_silver(bronze_mentions, "silver_mentions")

    sesion.stop()