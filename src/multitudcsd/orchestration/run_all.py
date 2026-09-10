"""Encadena el pipeline completo en local.
En Databricks esto lo hace el Job, no este modulo."""

from multitudcsd.orchestration import run_bronze, run_gold, run_ml, run_silver, run_stream


def main() -> None:
    """Ejecuta las cinco etapas en el mismo orden que el DAG del Job."""
    run_bronze.main()
    run_stream.main()
    run_silver.main()
    run_gold.main()
    run_ml.main()
    print("[run_all] pipeline completo terminado")


if __name__ == "__main__":
    main()