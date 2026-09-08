"""Test de la generacion sintetica de menciones"""

# Imports

import json
from datetime import datetime
from pathlib import Path

from multitudcsd.synthetic.mentions import (
    PERFIL_HORARIO,
    generate_landing_files,
    generate_mentions,
    hash_user_id,
)

#Campos que el generador tiene que producir. Es el
#contrato que consume el esquema de streaming/mentions_stream.py.
CAMPOS_ESPERADOS = {
    "mention_id",
    "event_ts",
    "lat",
    "lon",
    "platform",
    "language",
    "sentiment",
    "has_media",
    "user_hash",
}

#Funciones

def test_los_identificadores_no_se_repiten_entre_lotes():
    """mention_id es la clave de deduplicacion de Silver: tiene que ser unico en el conjunto."""
    lote_1 = generate_mentions(10, seed=1, primer_num=1)
    lote_2 = generate_mentions(10, seed=2, primer_num=11)

    identificadores = [m["mention_id"] for m in lote_1 + lote_2]
    assert len(set(identificadores)) == 20


def test_las_horas_siguen_el_perfil_del_evento():
    """Las horas con peso 0 en PERFIL_HORARIO no pueden aparecer nunca.
    """
    horas_imposibles = {hora for hora, peso in enumerate(PERFIL_HORARIO) if peso == 0}
    horas_generadas = {
        datetime.fromisoformat(mencion["event_ts"]).hour
        for mencion in generate_mentions(500)
    }

    assert horas_generadas.isdisjoint(horas_imposibles)


def test_el_hash_de_usuario_no_expone_el_identificador():
    """La seudonimizacion sin identificador original y sin reidentificacion."""
    hash_del_usuario = hash_user_id(7)

    assert len(hash_del_usuario) == 16
    assert hash_del_usuario != "7"
    # Hexadecimal: comprueba que es un digest y no el id disfrazado.
    assert all(caracter in "0123456789abcdef" for caracter in hash_del_usuario)
    # Determinista: el mismo usuario sintetico da siempre el mismo hash, que es lo que
    # permite contar usuarios distintos en gold_csd_activity.
    assert hash_user_id(7) == hash_del_usuario
    assert hash_user_id(8) != hash_del_usuario


def test_los_ficheros_de_landing_son_json_lines_validos(tmp_path, monkeypatch):
    """comprueba el formato que espera el stream."""
    monkeypatch.setenv("ENV", "local")
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))

    total = generate_landing_files(num_lotes=2, menciones_por_lote=5)

    ficheros = sorted((tmp_path / "landing" / "mentions").glob("*.json"))
    assert total == 10
    assert len(ficheros) == 2

    for fichero in ficheros:
        lineas = fichero.read_text(encoding="utf-8").splitlines()
        assert len(lineas) == 5
        # Un objeto JSON por linea, que es lo que sabe leer spark.readStream.json().
        for linea in lineas:
            assert set(json.loads(linea)) == CAMPOS_ESPERADOS


def test_landing_es_reproducible_byte_a_byte(tmp_path, monkeypatch):
    """Dos ejecuciones con la misma semilla producen ficheros identicos."""
    monkeypatch.setenv("ENV", "local")

    contenidos = []
    for nombre_de_ejecucion in ("primera", "segunda"):
        raiz = tmp_path / nombre_de_ejecucion
        monkeypatch.setenv("LAKEHOUSE_ROOT", str(raiz))
        generate_landing_files(num_lotes=1, menciones_por_lote=20)
        fichero = Path(raiz) / "landing" / "mentions" / "mentions_0001.json"
        contenidos.append(fichero.read_bytes())

    assert contenidos[0] == contenidos[1]