"""Configuracion general del motor: entorno, rutas y sesion de Spark."""

import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Windows necesita winutils.exe y hadoop.dll para el sistema de ficheros local de Hadoop.
HADOOP_HOME_WINDOWS = r"C:\Hadoop"

# Parametros del evento
#El csd 2026 ocurrio el 25 de julio. Ya ha pasado mientras se desarrollaba el proyecto.
#se usa otro sabado. Pero se puede usar para otros eventos cambiando estas variables
FECHA_REFERENCIA = "2026-09-05"
FECHA_REFERENCIA_GTFS = "20260905"   # mismo dia en el formato yyyyMMdd para GTFS
DIA_SEMANA_REFERENCIA = "saturday"

# Puntos del recorrido
#Se puede usar para cambiar el evento por otro
#Se usa en gtfs_static.py para filtrar paradas
#Se usa en mentions.py para generar las menciones en rrss
PUNTOS_DEL_RECORRIDO = [
    (52.5111, 13.4022),  # Spittelmarkt
    (52.4994, 13.3542),  # Nollendorfplatz
    (52.5163, 13.3777),  # Puerta de Brandeburgo
]
#Amplias el area para capturar las estaciones cercanas.
MARGEN_KILOMETROS = 3.0

def get_environment() -> str:
    """Devuelve el entorno de ejecucion: 'local' o 'databricks'."""
    return os.getenv("ENV", "local").lower()


def get_lakehouse_root() -> str:
    """Devuelve la raiz donde viven las tablas Delta, segun el entorno.

    En Azure Databricks la ruta se inyecta como variable de entorno del cluster,
    para no acoplar el codigo a ningun catalogo ni cuenta de almacenamiento.
    """
    if get_environment() == "databricks":
        raiz = os.getenv("LAKEHOUSE_ROOT", "")
        if not raiz:
            raise RuntimeError(
                "En Databricks hay que definir LAKEHOUSE_ROOT en la configuracion del cluster"
            )
        return raiz

    raiz_repo = Path(__file__).resolve().parents[2]
    ruta = Path(os.getenv("LAKEHOUSE_ROOT", "./data/lakehouse"))
    if not ruta.is_absolute():
        ruta = raiz_repo / ruta
    # as_posix() convierte D:\... en D:/..., que es lo que entiende Spark en Windows.
    return ruta.resolve().as_posix()

def prepare_windows_hadoop() -> None:
    """Corrige el error al intentar leer las delta tables.
    Deja HADOOP_HOME y el PATH listos para que Spark cargue la libreria nativa en Windows."""
    if os.name != "nt":
        return

    hadoop_home = Path(os.getenv("HADOOP_HOME", HADOOP_HOME_WINDOWS))
    carpeta_bin = hadoop_home / "bin"
    for binario in ("winutils.exe", "hadoop.dll"):
        if not (carpeta_bin / binario).exists():
            raise RuntimeError(
                f"Falta {binario} en {carpeta_bin}. Descargalo de cdarlint/winutils para Hadoop 3.3.x."
            )

    os.environ["HADOOP_HOME"] = str(hadoop_home)
    # La JVM lee el PATH al arrancar para construir su java.library.path
    os.environ["PATH"] = f"{carpeta_bin};{os.environ.get('PATH', '')}"

def get_spark_session(app_name: str = "multitudcsd") -> SparkSession:
    """Crea (o reutiliza) la sesion de Spark adecuada al entorno.

    En Databricks la sesion ya existe y solo hay que recogerla. En local hay que
    construirla activando las extensiones de Delta Lake.
    """
    if get_environment() == "databricks":
        return SparkSession.builder.getOrCreate()

    prepare_windows_hadoop()

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # En local
        .config("spark.sql.shuffle.partitions", "4")
    )
    # configure_spark_with_delta_pip descarga los JARs de Delta que corresponden
    # a la version instalada de delta-spark.
    return configure_spark_with_delta_pip(builder).getOrCreate()

#print(get_environment())
#print(get_lakehouse_root())
#print(get_spark_session())

#Funciones para el tier 2
def get_landing_root() -> str:
    """Carpeta donde el generador sintetico deja los ficheros que lee el stream"""
    return f"{get_lakehouse_root()}/landing"


def get_checkpoints_root() -> str:
    """Carpeta de checkpoints de Structured Streaming

    Cuelga del lakehouse porque en Databricks es un Volume, que admite las
    escrituras de ficheros que el checkpoint necesita
    """
    return f"{get_lakehouse_root()}/_checkpoints"


def get_models_root() -> str:
    """Carpeta donde se guardan los modelos entrenados con joblib"""
    return f"{get_lakehouse_root()}/models"