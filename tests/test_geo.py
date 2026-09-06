"""Tests de la indexacion H3. Resolucion unica del proyecto: 9."""

#Imports
from multitudcsd.transforms.geo import RESOLUCION_H3, add_h3_index, compute_h3_cell

#Funciones
def test_un_punto_de_berlin_devuelve_una_celda():
    celda = compute_h3_cell(52.5163, 13.3777)
    assert isinstance(celda, str)
    assert len(celda) == 15  # los identificadores H3 estan en hexadecimal miden 15 caracteres


def test_dos_puntos_muy_cercanos_caen_en_la_misma_celda():
    """A resolucion 9 la celda mide ~174 m: 20 m de diferencia no cambian de celda."""
    assert compute_h3_cell(52.5163, 13.3777) == compute_h3_cell(52.51645, 13.37785)


def test_dos_puntos_lejanos_caen_en_celdas_distintas():
    assert compute_h3_cell(52.5163, 13.3777) != compute_h3_cell(52.4994, 13.3542)


def test_coordenadas_nulas_devuelven_none():
    """Preferimos una fila con h3_index nulo a que reviente el job entero."""
    assert compute_h3_cell(None, 13.3777) is None
    assert compute_h3_cell(52.5163, None) is None
    assert compute_h3_cell(None, None) is None


def test_coordenadas_no_numericas_devuelven_none():
    """h3 normaliza las latitudes fuera de rango sin error, pero un texto si rompe.
    """
    assert compute_h3_cell("cincuenta y dos", "trece") is None


def test_la_resolucion_por_defecto_es_la_del_proyecto():
    """salta si la resolucion del proyecto cambia. """
    assert RESOLUCION_H3 == 9
    assert compute_h3_cell(52.5163, 13.3777) == compute_h3_cell(52.5163, 13.3777, 9)


def test_add_h3_index_anade_la_columna_y_respeta_los_nulos(spark):
    df = spark.createDataFrame(
        [(52.5163, 13.3777), (None, None)],
        "lat double, lon double",
    )

    resultado = add_h3_index(df).collect()

    assert "h3_index" in add_h3_index(df).columns
    assert resultado[0]["h3_index"] is not None
    assert resultado[1]["h3_index"] is None