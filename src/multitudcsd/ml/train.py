"""Entrenamiento del modelo de retraso por linea y franja horaria."""

import json
import math
from datetime import datetime, timezone
from pathlib import Path

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from multitudcsd.config import get_models_root
from multitudcsd.ml.features import (
    COLUMNA_OBJETIVO,
    COLUMNAS_CATEGORICAS,
    COLUMNAS_NUMERICAS,
    split_temporal,
)

SEMILLA = 20260725  # misma semilla que el generador sintetico: el proyecto es reproducible
NUM_ARBOLES = 100
NOMBRE_FICHERO_MODELO = "delay_model.joblib"
NOMBRE_FICHERO_METRICAS = "delay_model_metrics.json"
MINIMO_FILAS_ENTRENAMIENTO = 50


def select_columns(dataset: pd.DataFrame) -> pd.DataFrame:
    """Deja solo las columnas que entran al modelo, en el orden declarado en features.py."""
    return dataset[COLUMNAS_CATEGORICAS + COLUMNAS_NUMERICAS]


def build_model() -> Pipeline:
    """Pipeline de scikit-learn: codificacion de la linea + bosque aleatorio.

    Se guarda el Pipeline entero, no solo el regresor, para que predict.py aplique
    exactamente la misma codificacion que se uso al entrenar. Es la forma mas corta de
    evitar que entrenamiento y servicio se desalineen.

    handle_unknown='ignore' evita que una linea que no aparecia al entrenar genere error
    sus columnas quedan a cero en vez de lanzar una excepcion.
    """
    codificador = ColumnTransformer(
        transformers=[
            (
                "linea",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                COLUMNAS_CATEGORICAS,
            )
        ],
        remainder="passthrough",  # las columnas numericas pasan tal cual
    )
    regresor = RandomForestRegressor(
        n_estimators=NUM_ARBOLES, random_state=SEMILLA, n_jobs=-1
    )
    return Pipeline([("codificador", codificador), ("regresor", regresor)])


def evaluate_predictions(valores_reales, valores_predichos) -> dict:
    """Calcula MAE y RMSE, en segundos.

    El RMSE se saca con math.sqrt
    """
    mae = mean_absolute_error(valores_reales, valores_predichos)
    rmse = math.sqrt(mean_squared_error(valores_reales, valores_predichos))
    return {"mae_segundos": round(float(mae), 2), "rmse_segundos": round(float(rmse), 2)}


def evaluate_baseline(validacion: pd.DataFrame) -> dict:
    """Metricas del modelo trivial: suponer que el retraso sera el de la franja anterior.

    Es la referencia contra la que se compara. Si el modelo entrenado no baja de estas
    cifras no esta aportando nada
    """
    return evaluate_predictions(validacion[COLUMNA_OBJETIVO], validacion["delay_lag_seconds"])


def train_model(dataset: pd.DataFrame) -> tuple:
    """Entrena con las franjas antiguas y valida con las recientes.

    Devuelve el pipeline entrenado y el diccionario de metricas.
    """
    entrenamiento, validacion = split_temporal(dataset)
    if len(entrenamiento) < MINIMO_FILAS_ENTRENAMIENTO:
        raise RuntimeError(
            f"Solo hay {len(entrenamiento)} filas de entrenamiento; hacen falta al menos "
            f"{MINIMO_FILAS_ENTRENAMIENTO}. Ingiere mas GTFS-RT antes de entrenar."
        )
    if len(validacion) == 0:
        raise RuntimeError("La particion de validacion ha quedado vacia: revisa el corte temporal")

    modelo = build_model()
    modelo.fit(select_columns(entrenamiento), entrenamiento[COLUMNA_OBJETIVO])

    metricas = {
        "validacion": evaluate_predictions(
            validacion[COLUMNA_OBJETIVO], modelo.predict(select_columns(validacion))
        ),
        "baseline_franja_anterior": evaluate_baseline(validacion),
        "entrenamiento": evaluate_predictions(
            entrenamiento[COLUMNA_OBJETIVO], modelo.predict(select_columns(entrenamiento))
        ),
        "num_filas_entrenamiento": int(len(entrenamiento)),
        "num_filas_validacion": int(len(validacion)),
        "num_lineas": int(dataset["route_id"].nunique()),
        "primera_franja": str(dataset["slot_ts"].min()),
        "ultima_franja": str(dataset["slot_ts"].max()),
        "entrenado_el": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }

    print(
        f"[train] validacion  MAE {metricas['validacion']['mae_segundos']} s"
        f" · RMSE {metricas['validacion']['rmse_segundos']} s"
    )
    print(
        f"[train] baseline    MAE {metricas['baseline_franja_anterior']['mae_segundos']} s"
        f" · RMSE {metricas['baseline_franja_anterior']['rmse_segundos']} s"
    )
    return (modelo, metricas)


def save_model(modelo: Pipeline, metricas: dict) -> str:
    """Guarda el modelo con joblib y sus metricas en JSON, dentro del lakehouse.

    get_models_root() esta en lakehouse a proposito: en Databricks es un Volume de
    Unity Catalog, que admite escritura de ficheros con Python normal. La misma linea
    funciona en local y en la nube.
    """
    carpeta = Path(get_models_root())
    carpeta.mkdir(parents=True, exist_ok=True)

    ruta_modelo = carpeta / NOMBRE_FICHERO_MODELO
    joblib.dump(modelo, ruta_modelo)
    (carpeta / NOMBRE_FICHERO_METRICAS).write_text(
        json.dumps(metricas, indent=2), encoding="utf-8"
    )
    print(f"[train] modelo guardado en {ruta_modelo}")
    return ruta_modelo.as_posix()


if __name__ == "__main__":
    from multitudcsd.config import get_spark_session
    from multitudcsd.ml.features import build_training_features, to_pandas_dataset
    from multitudcsd.storage import read_delta

    sesion = get_spark_session("train-delay-model")
    gold_line_reliability = read_delta(sesion, "gold", "gold_line_reliability")
    dataset = to_pandas_dataset(build_training_features(gold_line_reliability))
    modelo_entrenado, metricas_del_modelo = train_model(dataset)
    save_model(modelo_entrenado, metricas_del_modelo)
    sesion.stop()