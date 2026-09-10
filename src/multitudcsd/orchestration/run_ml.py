"""Punto de entrada del bloque de ML: entrena el modelo y publica sus predicciones."""

from multitudcsd.config import get_spark_session
from multitudcsd.ml.features import build_training_features, to_pandas_dataset
from multitudcsd.ml.predict import TABLA_GOLD, build_predictions, load_model
from multitudcsd.ml.train import save_model, train_model
from multitudcsd.storage import read_delta, write_gold


def main() -> None:
    """Entrena con gold_line_reliability y escribe gold_delay_predictions.

    El modelo se recarga con load_model() despues de guardarlo, en vez de reutilizar el
    objeto en memoria: asi se comprueba en cada ejecucion que el artefacto de joblib se
    lee bien. En databricks hay un job por cada paso
    """
    sesion = get_spark_session("run-ml")
    gold_line_reliability = read_delta(sesion, "gold", "gold_line_reliability")

    dataset = to_pandas_dataset(build_training_features(gold_line_reliability))
    modelo, metricas = train_model(dataset)
    save_model(modelo, metricas)

    predicciones = build_predictions(sesion, gold_line_reliability, load_model())
    write_gold(predicciones, TABLA_GOLD)

    print("[run_ml] modelo entrenado y predicciones publicadas")
    sesion.stop()


if __name__ == "__main__":
    main()