"""Tests del bloque de ML"""
import datetime as dt

import pandas as pd
import pytest

from multitudcsd.ml.features import (
    build_scoring_features,
    build_training_features,
    split_temporal,
    to_pandas_dataset,
)
from multitudcsd.ml.train import build_model, evaluate_predictions, select_columns

COLUMNAS_GOLD = [
    "route_id",
    "service_date",
    "hour_of_day",
    "avg_delay_seconds",
    "pct_on_time",
    "num_actualizaciones",
]


def _gold_de_ejemplo(spark):
    """Tres franjas consecutivas de una linea y dos de otra, como saldrian de Gold."""
    dia = dt.date(2026, 9, 5)
    filas = [
        ("100", dia, 10, 60.0, 0.5, 20),
        ("100", dia, 11, 90.0, 0.4, 22),
        ("100", dia, 12, 30.0, 0.8, 18),
        ("200", dia, 10, 10.0, 0.9, 5),
        ("200", dia, 11, 15.0, 0.9, 6),
    ]
    return spark.createDataFrame(filas, COLUMNAS_GOLD)


def test_se_descarta_la_primera_franja_de_cada_linea(spark):
    """Sin franja anterior no hay features, asi que la primera observacion no cuenta."""
    features = build_training_features(_gold_de_ejemplo(spark))
    assert features.count() == 3  # 2 filas de la linea 100 y 1 de la 200
