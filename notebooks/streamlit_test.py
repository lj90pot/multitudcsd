""" test de explorador de las tablas gold con streamlit."""

import h3
import pandas as pd
import streamlit as st

from multitudcsd.config import get_spark_session
from multitudcsd.storage import read_delta


TABLAS_GOLD = [
    "gold_mobility_pressure",
    "gold_line_reliability",
    "gold_disruptions_by_cell",
    "gold_station_services",
    "gold_transit_capacity",
    "gold_csd_activity",
    "gold_mobility_vs_activity",
    "gold_delay_predictions",
]

#limite para no bloquear el pc
LIMITE_FILAS = 20000

COLUMNAS_HORA = ["hour_of_day", "scheduled_hour"]


@st.cache_resource
def get_cached_session():
    """Crea la SparkSession una sola vez para toda la sesion del navegador.

    Streamlit reejecuta el script entero en cada interaccion y
    se levanta una JVM nueva cada vez.
    """
    return get_spark_session("streamlit-gold-explorer")

@st.cache_data(show_spinner="Leyendo la tabla en Delta...")
def load_gold_table(nombre_tabla: str) -> pd.DataFrame:
    """Lee una tabla Gold entera y la devuelve como DataFrame de pandas."""
    sesion = get_cached_session()
    df_spark = read_delta(sesion, "gold", nombre_tabla).limit(LIMITE_FILAS)
    return df_spark.toPandas()

def find_hour_column(df: pd.DataFrame) -> str:
    """busca la columna de la hora en la tabla, o cadena vacia si no tiene."""
    for nombre in COLUMNAS_HORA:
        if nombre in df.columns:
            return nombre
    return ""


def add_cell_center(df: pd.DataFrame) -> pd.DataFrame:
    """Anade el centro (lat, lon) de cada celda H3 para poder pintarla en el mapa.

    las tablas Gold guardan la celda,
    y st.map necesita coordenadas
    """
    con_centro = df.copy()
    centros = con_centro["h3_index"].map(h3.cell_to_latlng)
    con_centro["lat"] = [centro[0] for centro in centros]
    con_centro["lon"] = [centro[1] for centro in centros]
    return con_centro


def render_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Pinta los filtros de la barra lateral y devuelve el DataFrame ya filtrado."""
    columna_hora = find_hour_column(df)
    if not columna_hora:
        return df

    horas = sorted(int(hora) for hora in df[columna_hora].dropna().unique())
    if not horas:
        return df

    desde, hasta = st.sidebar.select_slider(
        "Franja horaria",
        options=horas,
        value=(horas[0], horas[-1]),
    )
    return df[df[columna_hora].between(desde, hasta)]

def render_metrics(df: pd.DataFrame, nombre_tabla: str) -> None:
    """Muestra el count de filas y el de celdas distintas, si la tabla tiene celda."""
    columna_izquierda, columna_derecha = st.columns(2)
    columna_izquierda.metric("Filas", f"{len(df):,}".replace(",", "."))
    if "h3_index" in df.columns:
        columna_derecha.metric("Celdas H3", df["h3_index"].nunique())
    if len(df) == LIMITE_FILAS:
        st.warning(f"{nombre_tabla} se ha truncado a {LIMITE_FILAS} filas.")


def render_chart(df: pd.DataFrame) -> None:
    """Pinta una serie por hora de la metrica numerica que elija el usuario."""
    columna_hora = find_hour_column(df)
    columnas_numericas = [
        columna
        for columna in df.select_dtypes("number").columns
        if columna != columna_hora
    ]
    if not columna_hora or not columnas_numericas:
        return

    metrica = st.selectbox("Metrica por hora", columnas_numericas)
    serie = df.groupby(columna_hora)[metrica].mean().sort_index()
    st.bar_chart(serie)


def render_map(df: pd.DataFrame) -> None:
    """Pinta las celdas H3 de la tabla como puntos en el mapa de Berlin."""
    if "h3_index" not in df.columns:
        return

    con_coordenadas = add_cell_center(df.dropna(subset=["h3_index"]))
    st.map(con_coordenadas[["lat", "lon"]], size=60)


def main() -> None:
    """WEBAPP"""
    st.set_page_config(page_title="multitudcsd - tablas Gold", layout="wide")
    st.title("Tablas Gold de multitudcsd")
    st.caption(
        "Lee el lakehouse local en Delta. La aplicacion solo consulta y dibuja: "
        "La logica esta en el paquete multitudcsd"
    )

    nombre_tabla = st.sidebar.selectbox("Tabla", TABLAS_GOLD)

    try:
        df = load_gold_table(nombre_tabla)
    except Exception as error:
        st.error(
            f"No se ha podido leer {nombre_tabla}. Ejecuta antes `make gold` "
            f"para construirla.\n\nDetalle: {error}"
        )
        return

    if df.empty:
        st.info(f"{nombre_tabla} existe pero no tiene filas todavia.")
        return

    df_filtrado = render_filters(df)
    render_metrics(df_filtrado, nombre_tabla)
    render_chart(df_filtrado)
    render_map(df_filtrado)
    st.dataframe(df_filtrado, use_container_width=True)


if __name__ == "__main__":
    main()