"""Utilidades de indexacion geoespacial con H3.

Resolucion fija en todo el proyecto: 9 (celdas de ~174 m de arista).
Esta resolucion proporciona suficiente anonimizacion a la vez que
suficiente detalle para el objeto del proyecto
"""

import h3
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

RESOLUCION_H3 = 9


def compute_h3_cell(lat: float, lon: float, resolution: int = RESOLUCION_H3) -> str | None:
    """Devuelve el identificador de celda H3 para un punto (lat, lon).

    Si las coordenadas son nulas o no validas, devuelve None en lugar de lanzar una
    excepcion: es mejor una fila con h3_index nulo a que falle todo el job de Spark
    por un dato incompleto.
    """
    if lat is None or lon is None:
        return None
    try:
        return h3.latlng_to_cell(lat, lon, resolution)
    except (ValueError, TypeError):
        return None


# h3-py no tiene integracion nativa con Spark, por lo tanto se envuelve la funcion pura de
# arriba en un udf. No hace falta un pandas_udf de momento: el volumen de este proyecto es
# pequenho.
_h3_udf = F.udf(compute_h3_cell, StringType())


def add_h3_index(df: DataFrame, lat_col: str = "lat", lon_col: str = "lon") -> DataFrame:
    """Anade la columna h3_index a partir de dos columnas de coordenadas.

    Se usa en todo registro de Silver que tenga coordenadas.
    Si lat/lon son nulos h3_index es nulo
    en vez de triguear un error
    """
    return df.withColumn("h3_index", _h3_udf(F.col(lat_col), F.col(lon_col)))