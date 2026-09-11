"""Punto de entrada del tier 2: genera menciones sinteticas y las vuelca en Bronze."""

from multitudcsd.config import get_spark_session
from multitudcsd.streaming.mentions_stream import ingest_mentions_stream
from multitudcsd.synthetic.mentions import generate_landing_files

# Volumen fijado en el plan: 5 lotes de 400 son 2.000 menciones. Suficiente para que queden
# celdas-hora por encima del umbral k = 5 de anonimizacion,
# y pequeno para que el pipeline corra en minutos
# y para subirlo al Volume de Azure y limitar el consumo
NUM_LOTES = 5
MENCIONES_POR_LOTE = 400


def main() -> None:
    """Genera los ficheros de landing y lanza el stream que los procesa a Bronze.
    """
    total = generate_landing_files(
        num_lotes=NUM_LOTES, menciones_por_lote=MENCIONES_POR_LOTE
    )
    print(f"[run_stream] {total} menciones disponibles en landing")

    sesion = get_spark_session("run-stream")
    ingest_mentions_stream(sesion)
    print("[run_stream] menciones sinteticas ingeridas en Bronze")
    sesion.stop()


if __name__ == "__main__":
    main()