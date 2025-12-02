from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from models.delay_logreg import train_model, predict_delay_proba
from models import anomaly_detection


# ---------------------------------------------------------------------------
# 1) Logistic Regression: Quality-Check
# ---------------------------------------------------------------------------


def test_delay_logreg_training_quality():
    """
    Testet, ob das Delay-Modell überhaupt trainierbar ist und
    eine sinnvolle Qualität erreicht (ROC-AUC über Mindestschwelle).

    Hinweis:
      - Dieser Test lädt Daten aus dem DWH und kann einige Zeit dauern.
      - Falls kein DWH verfügbar ist, wird der Test übersprungen.
    """
    MIN_ROC_AUC = 0.60
    MIN_ACCURACY = 0.60

    try:
        metrics = train_model(limit=200_000, test_size=0.2, random_state=42)
    except Exception as e:
        pytest.skip(
            f"Delay-LogReg konnte nicht trainiert werden (DWH/Verbindung?): {e}"
        )
        return

    assert "roc_auc" in metrics, "train_model() liefert kein 'roc_auc' zurück."
    assert "accuracy" in metrics, "train_model() liefert keine 'accuracy' zurück."

    assert metrics["roc_auc"] >= MIN_ROC_AUC, (
        f"ROC-AUC des Delay-Modells ist zu niedrig: {metrics['roc_auc']:.3f} "
        f"(Schwellwert: {MIN_ROC_AUC:.2f})"
    )

    assert metrics["accuracy"] >= MIN_ACCURACY, (
        f"Accuracy des Delay-Modells ist zu niedrig: {metrics['accuracy']:.3f} "
        f"(Schwellwert: {MIN_ACCURACY:.2f})"
    )


def test_predict_delay_proba_returns_valid_probability(tmp_path: Path):
    """
    Testet, dass predict_delay_proba() eine Wahrscheinlichkeit zwischen 0 und 1 liefert.
    Falls noch kein Modell trainiert wurde, wird zunächst mit kleinem Limit trainiert.
    """
    try:
        proba = predict_delay_proba(
            airport_id="JFK",
            airline_id="Delta Air Lines Inc",
            dep_time_label="Morning",
            tavg=5.0,
            prcp=0.1,
            wspd=10.0,
            num_departures_same_slot_airport=20,
        )
    except FileNotFoundError:
        try:
            train_model(limit=50_000, test_size=0.2, random_state=42)
        except Exception as e:
            pytest.skip(
                f"Delay-LogReg konnte nicht trainiert werden (DWH/Verbindung?): {e}"
            )
            return

        proba = predict_delay_proba(
            airport_id="JFK",
            airline_id="Delta Air Lines Inc",
            dep_time_label="Morning",
            tavg=5.0,
            prcp=0.1,
            wspd=10.0,
            num_departures_same_slot_airport=20,
        )

    assert isinstance(
        proba, float
    ), "predict_delay_proba() sollte einen float zurückgeben."
    assert 0.0 <= proba <= 1.0, f"Probability außerhalb [0, 1]: {proba}"


# ---------------------------------------------------------------------------
# 2) Anomaly Detection: Test with artificial Data
# ---------------------------------------------------------------------------


def test_anomaly_detection_zscore_detects_obvious_spike():
    """
    Testet die Z-Score-Berechnung der Anomaly Detection mit künstlichen Daten:

      - 60 Tage mit konstantem Delay von 10 Minuten
      - 1 Tag mit großem Spike (z. B. 60 Minuten)

    Erwartung:
      - Der Spike-Tag hat |z_score| > 3
      - Ein "normaler" Tag im stabilen Bereich bleibt in der Nähe von 0
    """
    dates = pd.date_range("2023-01-01", periods=60, freq="D")

    base_delay = 10.0
    delays = np.full(len(dates), base_delay, dtype=float)

    spike_idx = 40
    delays[spike_idx] = 60.0

    df = pd.DataFrame({"date": dates, "avg_dep_delay": delays})

    df_z = anomaly_detection._compute_z_scores(df, window=30, min_periods=10)

    spike_row = df_z.loc[df_z["date"] == dates[spike_idx]].iloc[0]
    spike_z = spike_row["z_score"]

    normal_row = df_z.loc[df_z["date"] == dates[20]].iloc[0]
    normal_z = normal_row["z_score"]

    assert (
        abs(spike_z) > 3.0
    ), f"Offensichtlicher Spike sollte |z| > 3 haben, hat aber {spike_z:.3f}"

    assert (
        abs(normal_z) < 1.5
    ), f"Normaler Tag sollte einen kleinen |z|-Wert haben, hat aber {normal_z:.3f}"
