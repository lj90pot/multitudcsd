"""Generador de menciones geolocalizadas sinteticas hacia la carpeta landing"""

import hashlib
import json
import math
import random
from datetime import datetime, timedelta
from pathlib import Path

from multitudcsd.config import (
    FECHA_REFERENCIA,
    PUNTOS_DEL_RECORRIDO,
    get_landing_root,
)

SEMILLA = 20260725  # semilla fija: el generador tiene que ser reproducible
SAL_SEUDONIMIZACION = "multitudcsd-tfm"  # sal publica: los datos ya son sinteticos
NUM_USUARIOS_SINTETICOS = 400

# Dispersion de las menciones alrededor de los puntos del recorrido.
DESVIACION_METROS = 300.0
METROS_POR_GRADO_LATITUD = 111320.0
LATITUD_DE_REFERENCIA = 52.51

# Peso relativo de menciones por hora del dia (indice = hora, 0 a 23). El perfil imita
# una manifestacion de tarde: arranque a mediodia, pico entre las 14 y las 17, cola de
# noche por las fiestas posteriores. No es un dato medido es un supuesto
PERFIL_HORARIO = [
    1, 1, 1, 0, 0, 0, 0, 1, 2, 4, 8, 14,
    22, 30, 38, 40, 36, 28, 20, 16, 14, 10, 6, 3,
]

PLATAFORMAS = ["mastodon", "bluesky", "x"]
IDIOMAS = ["de", "en", "pl"]


def hash_user_id(numero_de_usuario: int) -> str:
    """Seudonimiza un identificador de usuario con SHA-256 y sal.

    Aunque el dato es sintetico, el diseño es el que se aplicaria con datos reales: no
    se guarda el identificador original y no existe tabla de reidentificacion.
    """
    texto = f"{SAL_SEUDONIMIZACION}:{numero_de_usuario}"
    return hashlib.sha256(texto.encode("utf-8")).hexdigest()[:16]


def pick_hour(generador: random.Random) -> int:
    """Elige una hora del dia siguiendo el perfil horario del evento.
    arriba definido"""
    horas = list(range(24))
    return generador.choices(horas, weights=PERFIL_HORARIO, k=1)[0]


def jitter_coordinates(generador: random.Random, latitud: float, longitud: float) -> tuple:
    """Desplaza un punto unos cientos de metros al azar, en grados.

    Un grado de latitud son aprox. 111 km en cualquier sitio, pero un grado de longitud son
    111 km * cos(latitud): en Berlin se queda en aprox. 68 km. Sin esa correccion la nube de
    puntos saldria estirada en horizontal.
    """
    metros_por_grado_longitud = METROS_POR_GRADO_LATITUD * math.cos(
        math.radians(LATITUD_DE_REFERENCIA)
    )
    desplazamiento_latitud = generador.gauss(0, DESVIACION_METROS) / METROS_POR_GRADO_LATITUD
    desplazamiento_longitud = generador.gauss(0, DESVIACION_METROS) / metros_por_grado_longitud
    return (latitud + desplazamiento_latitud, longitud + desplazamiento_longitud)


def generate_mentions(num_menciones: int, seed: int = SEMILLA, primer_num: int = 1) -> list[dict]:
    """Genera una lista de menciones sinteticas reproducible para la fecha de referencia.
    primer_num se usa para que los lotes no salgan todos con el mismo id.
    Sino salen menciones duplicadas
    """
    generador = random.Random(seed)
    inicio_del_dia = datetime.fromisoformat(FECHA_REFERENCIA)

    menciones = []
    for desplazamiento in range(num_menciones):
        numero = primer_num + desplazamiento
        punto_del_recorrido = generador.choice(PUNTOS_DEL_RECORRIDO)
        latitud, longitud = jitter_coordinates(generador, *punto_del_recorrido)
        instante = inicio_del_dia + timedelta(
            hours=pick_hour(generador),
            minutes=generador.randrange(60),
            seconds=generador.randrange(60),
        )
        menciones.append({
            "mention_id": f"men_{numero:06d}",
            "event_ts": instante.isoformat(),
            "lat": round(latitud, 6),
            "lon": round(longitud, 6),
            "platform": generador.choice(PLATAFORMAS),
            "language": generador.choice(IDIOMAS),
            # Sentimiento sesgado a positivo y recortado al rango valido.
            "sentiment": round(max(-1.0, min(1.0, generador.gauss(0.35, 0.4))), 3),
            "has_media": generador.random() < 0.4,
            "user_hash": hash_user_id(generador.randrange(NUM_USUARIOS_SINTETICOS)),
        })
    return menciones


def write_mentions_batch(menciones: list[dict], numero_de_lote: int) -> str:
    """Escribe un lote de menciones como JSON Lines en la carpeta landing.

    Un objeto JSON por linea es el formato para spark.readStream.json(). Se usa
    Path.write_text en Databricks la carpeta es
    un Volume de Unity Catalog, que admite este tipo de escritura.
    """
    carpeta = Path(get_landing_root()) / "mentions"
    carpeta.mkdir(parents=True, exist_ok=True)

    ruta = carpeta / f"mentions_{numero_de_lote:04d}.json"
    lineas = [json.dumps(mencion, ensure_ascii=False) for mencion in menciones]
    ruta.write_text("\n".join(lineas), encoding="utf-8")
    return ruta.as_posix()


def generate_landing_files(num_lotes: int = 5, menciones_por_lote: int = 400) -> int:
    """Genera varios ficheros en landing para que el stream pueda leer por tandas.

    Varios ficheros en vez de uno grande: asi el stream demuestra lectura incremental
    """
    total = 0
    for numero_de_lote in range(1, num_lotes + 1):
        # Semilla derivada del lote: cada fichero es distinto pero el conjunto es reproducible.
        menciones = generate_mentions(
            menciones_por_lote,
            seed=SEMILLA + numero_de_lote,
            primer_num= total + 1,

        )
        ruta = write_mentions_batch(menciones, numero_de_lote)
        print(f"[synthetic] lote {numero_de_lote}: {len(menciones)} menciones en {ruta}")
        total += len(menciones)

    print(f"[synthetic] {total} menciones generadas con semilla {SEMILLA}")
    return total


if __name__ == "__main__":
    generate_landing_files()