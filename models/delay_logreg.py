from __future__ import annotations

from pathlib import Path
from typing import Optional, Dict, Any

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    roc_auc_score,
    classification_report,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from etl.config import get_engine


def get_project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def get_model_path() -> Path:
    models_dir = get_project_root() / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    return models_dir / "logreg_delay.pkl"


def load_training_data(limit: Optional[int] = None) -> pd.DataFrame:
    engine = get_engine()

    sql = """
    SELECT
        ff.flight_id,
        ff.flight_date_id,
        ff.dep_airport_id,
        ff.arr_airport_id,
        ff.airline_id,
        ff.dep_time_label,
        ff.is_delayed_15,
        dw.tavg,
        dw.prcp,
        dw.wspd,
        dd.month,
        dd.day_of_week,
        dd.is_weekend
    FROM fact_flights ff
    LEFT JOIN dim_weather dw
      ON dw.weather_id = ff.weather_id
    LEFT JOIN dim_date dd
      ON dd.date_id = ff.flight_date_id
    """

    if limit is not None:
        sql += f" LIMIT {int(limit)}"

    df = pd.read_sql(sql, con=engine)
    return df


def add_congestion_feature(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fügt das Feature num_departures_same_slot_airport hinzu:

      Anzahl der Abflüge im gleichen Airport und gleichen Zeitfenster.

    Da wir keine echte Stunde haben, verwenden wir:
      - flight_date_id
      - dep_airport_id
      - dep_time_label

    1 Gruppe = "gleicher Tag, gleicher Abflughafen, gleicher DepTime_label"
    """
    required = ["flight_date_id", "dep_airport_id", "dep_time_label"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Fehlende Spalten für Kongestions-Feature: {missing}")

    df = df.copy()

    group_sizes = (
        df.groupby(["flight_date_id", "dep_airport_id", "dep_time_label"])
        .size()
        .rename("num_departures_same_slot_airport")
    )

    df = df.join(
        group_sizes,
        on=["flight_date_id", "dep_airport_id", "dep_time_label"],
    )

    return df


def prepare_features(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    if "is_delayed_15" not in df.columns:
        raise KeyError("Spalte 'is_delayed_15' fehlt in Trainingsdaten.")

    required_for_model = [
        "dep_airport_id",
        "arr_airport_id",
        "airline_id",
        "dep_time_label",
        "tavg",
        "prcp",
        "wspd",
        "month",
        "day_of_week",
        "is_weekend",
        "is_delayed_15",
    ]
    missing_cols = [c for c in required_for_model if c not in df.columns]
    if missing_cols:
        raise KeyError(f"Fehlende Spalten für das Modell: {missing_cols}")

    df = df.dropna(subset=required_for_model)

    df = add_congestion_feature(df)

    feature_cols_cat = [
        "dep_airport_id",
        "arr_airport_id",
        "airline_id",
        "dep_time_label",
        "month",
        "day_of_week",
        "is_weekend",
    ]

    feature_cols_num = [
        "tavg",
        "prcp",
        "wspd",
        "num_departures_same_slot_airport",
    ]

    X = df[feature_cols_cat + feature_cols_num].copy()
    y = df["is_delayed_15"].astype(int)

    return X, y


def build_pipeline(
    categorical_features: list[str],
    numeric_features: list[str],
) -> Pipeline:
    """
    Erstellt die sklearn-Pipeline mit:

      - ColumnTransformer:
          * OneHotEncoder für kategoriale Features
          * StandardScaler für numerische Features
      - LogisticRegression als Klassifikator
    """

    categorical_transformer = OneHotEncoder(
        handle_unknown="ignore",
        sparse_output=True,
    )

    numeric_transformer = StandardScaler()

    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", categorical_transformer, categorical_features),
            ("num", numeric_transformer, numeric_features),
        ]
    )

    logreg = LogisticRegression(
        max_iter=1000,
        class_weight=None,  # "balanced",
        solver="lbfgs",
        n_jobs=-1,
    )

    pipeline = Pipeline(
        steps=[
            ("preprocess", preprocessor),
            ("clf", logreg),
        ]
    )

    return pipeline


def train_model(
    test_size: float = 0.2,
    limit: Optional[int] = None,
    random_state: int = 42,
) -> Dict[str, Any]:
    """
    Lädt Daten, trainiert das Delay-Modell und speichert die Pipeline.

    Parameter:
        test_size: Anteil der Validierungsdaten (default 0.2)
        limit: optionales Row-Limit fürs Laden (z. B. 100_000)
        random_state: Seed für Reproduzierbarkeit

    Rückgabe:
        Dict mit Metriken (accuracy, precision, recall, roc_auc).
    """

    df = load_training_data(limit=limit)

    X, y = prepare_features(df)

    categorical_features = [
        "dep_airport_id",
        "arr_airport_id",
        "airline_id",
        "dep_time_label",
        "month",
        "day_of_week",
        "is_weekend",
    ]
    numeric_features = ["tavg", "prcp", "wspd", "num_departures_same_slot_airport"]

    pipeline = build_pipeline(
        categorical_features=categorical_features,
        numeric_features=numeric_features,
    )

    X_train, X_valid, y_train, y_valid = train_test_split(
        X,
        y,
        test_size=test_size,
        random_state=random_state,
        stratify=y,
    )

    print("Trainiere Modell (LogisticRegression)...")
    pipeline.fit(X_train, y_train)

    print("Berechne Metriken auf dem Validation-Set...")
    y_pred = pipeline.predict(X_valid)
    y_proba = pipeline.predict_proba(X_valid)[:, 1]

    for thr in [0.3, 0.5, 0.7]:
        y_pred_thr = (y_proba >= thr).astype(int)
        acc = accuracy_score(y_valid, y_pred_thr)
        prec = precision_score(y_valid, y_pred_thr, zero_division=0)
        rec = recall_score(y_valid, y_pred_thr, zero_division=0)
        print(f"\n=== Metrics @ threshold={thr:.2f} ===")
        print(f"accuracy : {acc:.4f}")
        print(f"precision: {prec:.4f}")
        print(f"recall   : {rec:.4f}")

    metrics: Dict[str, Any] = {
        "accuracy": float(accuracy_score(y_valid, y_pred)),
        "precision": float(precision_score(y_valid, y_pred, zero_division=0)),
        "recall": float(recall_score(y_valid, y_pred, zero_division=0)),
        "roc_auc": float(roc_auc_score(y_valid, y_proba)),
    }

    print("\n=== Validation Metrics ===")
    for k, v in metrics.items():
        print(f"{k:10s}: {v:.4f}")

    print("\nClassification report:")
    print(classification_report(y_valid, y_pred, digits=3, zero_division=0))

    model_path = get_model_path()
    joblib.dump(
        {
            "pipeline": pipeline,
            "categorical_features": categorical_features,
            "numeric_features": numeric_features,
            "metrics": metrics,
        },
        model_path,
    )
    print(f"\n Modell-Pipeline gespeichert unter: {model_path}")

    return metrics


def _load_trained_pipeline(model_path: Optional[Path] = None) -> Pipeline:
    if model_path is None:
        model_path = get_model_path()

    if not model_path.exists():
        raise FileNotFoundError(
            f"Trainiertes Modell nicht gefunden unter {model_path}. "
            "Bitte zuerst train_model() ausführen."
        )

    obj = joblib.load(model_path)
    pipeline: Pipeline = obj["pipeline"]
    return pipeline


def predict_delay_proba(
    airport_id: str,
    airline_id: str,
    dep_time_label: str,
    tavg: float,
    prcp: float,
    wspd: float,
    num_departures_same_slot_airport: int = 1,
    model_path: Optional[Path] = None,
) -> float:
    """
    Gibt P(Delay >= 15 Minuten) für eine gegebene Konfiguration zurück.

    Hinweis:
      Das Modell wurde mit zusätzlichen Features trainiert:
        - arr_airport_id
        - month
        - day_of_week
        - is_weekend

      Da diese in der Inferenz-Signatur nicht vorkommen, werden hier
      einfache Default-Werte gesetzt:
        - arr_airport_id = dep_airport_id
        - month = 1
        - day_of_week = 1
        - is_weekend = False

      Für eine realistische Nutzung kannst du die Funktion später
      erweitern (z. B. um ein Datum und arr_airport_id).
    """

    pipeline = _load_trained_pipeline(model_path)

    dep_airport = airport_id.strip().upper()
    airline = airline_id.strip()
    dep_label = dep_time_label.strip()

    data = pd.DataFrame(
        [
            {
                "dep_airport_id": dep_airport,
                "arr_airport_id": dep_airport,
                "airline_id": airline,
                "dep_time_label": dep_label,
                "month": 1,
                "day_of_week": 1,
                "is_weekend": False,
                "tavg": float(tavg),
                "prcp": float(prcp),
                "wspd": float(wspd),
                "num_departures_same_slot_airport": int(
                    num_departures_same_slot_airport
                ),
            }
        ]
    )

    proba = pipeline.predict_proba(data)[0, 1]
    return float(proba)


if __name__ == "__main__":
    metrics = train_model(limit=None)
