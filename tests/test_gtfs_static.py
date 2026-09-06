"""Tests de la ingesta del GTFS estatico.
No se hacen en  red: el zip se construye en memoria."""

#Imports
import io
import zipfile

#Funciones
from multitudcsd.ingestion.gtfs_static import (
    compute_recorrido_area,
    esta_cerca_del_csd,
    obtener_calendario_viaje_excepciones,
    obtener_calendario_viajes,
    obtener_horarios_de_esas_paradas,
    obtener_lineas_relevantes,
    obtener_paradas_cerca_del_csd,
    obtener_viajes_relevantes,
)


def _zip_de_prueba(ficheros: dict) -> bytes:
    """Construye en memoria un zip con los ficheros csv que le pasemos."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zip_abierto:
        for nombre, contenido in ficheros.items():
            zip_abierto.writestr(nombre, contenido)
    return buffer.getvalue()


CALENDAR_CSV = (
    "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
    "s_laborable,1,1,1,1,1,0,0,20260601,20261212\n"
    "s_sabado,0,0,0,0,0,1,0,20260601,20261212\n"
    "s_no_usado,1,1,1,1,1,1,1,20260601,20261212\n"
)


def test_calendar_filtra_solo_los_servicios_de_los_viajes_guardados():
    contenido_zip = _zip_de_prueba({"calendar.txt": CALENDAR_CSV})

    resultado = obtener_calendario_viajes(contenido_zip, {"s_laborable", "s_sabado"})

    assert len(resultado) == 2
    assert {fila["service_id"] for fila in resultado} == {"s_laborable", "s_sabado"}


def test_calendar_devuelve_vacio_si_el_fichero_no_esta_en_el_zip():
    """Un feed GTFS puede traer solo calendar_dates.txt: la ingesta sigue."""
    contenido_zip = _zip_de_prueba({"stops.txt": "stop_id\np1\n"})

    assert obtener_calendario_viajes(contenido_zip, {"s_laborable"}) == []


def test_calendar_dates_conserva_el_tipo_de_excepcion():
    csv_excepciones = (
        "service_id,date,exception_type\n"
        "s_sabado,20260725,1\n"
        "s_otro,20260725,2\n"
    )
    contenido_zip = _zip_de_prueba({"calendar_dates.txt": csv_excepciones})

    resultado = obtener_calendario_viaje_excepciones(contenido_zip, {"s_sabado"})

    assert len(resultado) == 1
    assert resultado[0]["exception_type"] == "1"  # el csv se lee sin castear, eso es cosa de Silver