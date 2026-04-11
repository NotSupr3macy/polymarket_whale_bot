"""
Train and validate EV models for MLB and NBA.

Produces 6 trained models total:
  - MLB: moneyline, spread, over_under
  - NBA: moneyline, spread, over_under

Input: per-season state CSVs from ev_engine/data/{mlb,nba}/
Output: .joblib model files in ev_engine/models/

For spread and over_under models we AUGMENT the training data with
multiple candidate lines per row, so the model learns a single flexible
function that accepts any line as an input feature.

Usage:
    python -m ev_engine.model_training                  # train all 6
    python -m ev_engine.model_training --sport mlb      # just MLB
    python -m ev_engine.model_training --xgboost        # use XGBoost instead
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | training | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
MODELS_DIR = ROOT / "models"


# ─────────────────────────────────────────────────────────────────────
#  Data loading
# ─────────────────────────────────────────────────────────────────────

def load_mlb_states(seasons: Optional[list[int]] = None) -> pd.DataFrame:
    """Load MLB state CSVs into a single DataFrame."""
    paths = sorted((DATA_DIR / "mlb").glob("*_states.csv"))
    if seasons:
        wanted = {f"{s}_states.csv" for s in seasons}
        paths = [p for p in paths if p.name in wanted]
    if not paths:
        raise FileNotFoundError(f"No MLB state CSVs found in {DATA_DIR/'mlb'}")

    dfs = []
    for p in paths:
        logger.info("  Loading %s ...", p.name)
        dfs.append(pd.read_csv(p))
    df = pd.concat(dfs, ignore_index=True)
    logger.info("MLB: %d rows from %d seasons", len(df), len(paths))
    return df


def load_nba_states(seasons: Optional[list[int]] = None) -> pd.DataFrame:
    """Load NBA state CSVs into a single DataFrame."""
    paths = sorted((DATA_DIR / "nba").glob("*_states.csv"))
    if seasons:
        wanted = {f"{s}_states.csv" for s in seasons}
        paths = [p for p in paths if p.name in wanted]
    if not paths:
        raise FileNotFoundError(f"No NBA state CSVs found in {DATA_DIR/'nba'}")

    dfs = []
    for p in paths:
        logger.info("  Loading %s ...", p.name)
        dfs.append(pd.read_csv(p))
    df = pd.concat(dfs, ignore_index=True)
    logger.info("NBA: %d rows from %d seasons", len(df), len(paths))
    return df


# ─────────────────────────────────────────────────────────────────────
#  Feature construction
# ─────────────────────────────────────────────────────────────────────

MLB_BASE_FEATURES = [
    "inning", "top_bottom", "outs", "runners_on",
    "home_score", "away_score", "score_diff", "total_runs_so_far",
]

NBA_BASE_FEATURES = [
    "period", "time_remaining_sec", "game_time_elapsed_sec",
    "home_score", "away_score", "score_diff",
    "total_points_so_far", "pace_estimate",
]


def mlb_moneyline_features(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    X = df[MLB_BASE_FEATURES].to_numpy(dtype=np.float32)
    y = df["home_win"].to_numpy(dtype=np.int8)
    return X, y


def nba_moneyline_features(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    X = df[NBA_BASE_FEATURES].to_numpy(dtype=np.float32)
    y = df["home_win"].to_numpy(dtype=np.int8)
    return X, y


def mlb_spread_features(
    df: pd.DataFrame, line_choices: Optional[list[float]] = None
) -> tuple[np.ndarray, np.ndarray]:
    """
    Build a flexible spread dataset by augmenting each row with multiple
    candidate spread lines. The target is whether the FINAL run diff
    (home minus away) exceeded the line.
    """
    if line_choices is None:
        line_choices = [-3.5, -2.5, -1.5, -0.5, 0.5, 1.5, 2.5, 3.5]

    parts_X: list[np.ndarray] = []
    parts_y: list[np.ndarray] = []
    base = df[MLB_BASE_FEATURES].to_numpy(dtype=np.float32)
    final_diff = df["final_run_diff"].to_numpy(dtype=np.float32)

    for line in line_choices:
        line_col = np.full((len(df), 1), line, dtype=np.float32)
        X = np.concatenate([base, line_col], axis=1)
        y = (final_diff > line).astype(np.int8)
        parts_X.append(X)
        parts_y.append(y)

    return np.concatenate(parts_X, axis=0), np.concatenate(parts_y, axis=0)


def nba_spread_features(
    df: pd.DataFrame, line_choices: Optional[list[float]] = None
) -> tuple[np.ndarray, np.ndarray]:
    if line_choices is None:
        line_choices = [-12.5, -8.5, -5.5, -3.5, -1.5, 1.5, 3.5, 5.5, 8.5, 12.5]

    parts_X: list[np.ndarray] = []
    parts_y: list[np.ndarray] = []
    base = df[NBA_BASE_FEATURES].to_numpy(dtype=np.float32)
    final_diff = df["final_point_diff"].to_numpy(dtype=np.float32)

    for line in line_choices:
        line_col = np.full((len(df), 1), line, dtype=np.float32)
        X = np.concatenate([base, line_col], axis=1)
        y = (final_diff > line).astype(np.int8)
        parts_X.append(X)
        parts_y.append(y)

    return np.concatenate(parts_X, axis=0), np.concatenate(parts_y, axis=0)


def mlb_ou_features(
    df: pd.DataFrame, line_choices: Optional[list[float]] = None
) -> tuple[np.ndarray, np.ndarray]:
    if line_choices is None:
        line_choices = [5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5]

    parts_X: list[np.ndarray] = []
    parts_y: list[np.ndarray] = []
    base = df[MLB_BASE_FEATURES].to_numpy(dtype=np.float32)
    final_total = df["final_total_runs"].to_numpy(dtype=np.float32)

    for line in line_choices:
        line_col = np.full((len(df), 1), line, dtype=np.float32)
        X = np.concatenate([base, line_col], axis=1)
        y = (final_total > line).astype(np.int8)
        parts_X.append(X)
        parts_y.append(y)

    return np.concatenate(parts_X, axis=0), np.concatenate(parts_y, axis=0)


def nba_ou_features(
    df: pd.DataFrame, line_choices: Optional[list[float]] = None
) -> tuple[np.ndarray, np.ndarray]:
    if line_choices is None:
        line_choices = [195.5, 205.5, 215.5, 220.5, 225.5, 230.5, 240.5]

    parts_X: list[np.ndarray] = []
    parts_y: list[np.ndarray] = []
    base = df[NBA_BASE_FEATURES].to_numpy(dtype=np.float32)
    final_total = df["final_total_points"].to_numpy(dtype=np.float32)

    for line in line_choices:
        line_col = np.full((len(df), 1), line, dtype=np.float32)
        X = np.concatenate([base, line_col], axis=1)
        y = (final_total > line).astype(np.int8)
        parts_X.append(X)
        parts_y.append(y)

    return np.concatenate(parts_X, axis=0), np.concatenate(parts_y, axis=0)


# ─────────────────────────────────────────────────────────────────────
#  Training
# ─────────────────────────────────────────────────────────────────────

def make_logreg_pipeline() -> Pipeline:
    """Standard scaler + logistic regression pipeline."""
    return Pipeline([
        ("scaler", StandardScaler()),
        ("clf", LogisticRegression(
            solver="lbfgs",
            max_iter=2000,
            n_jobs=-1,
            C=1.0,
        )),
    ])


def make_xgboost_pipeline():
    """XGBoost fallback pipeline (only imported when requested)."""
    from xgboost import XGBClassifier  # noqa: WPS433
    return Pipeline([
        ("scaler", StandardScaler()),
        ("clf", XGBClassifier(
            n_estimators=200,
            max_depth=6,
            learning_rate=0.1,
            use_label_encoder=False,
            eval_metric="logloss",
            n_jobs=-1,
            verbosity=0,
        )),
    ])


def train_and_evaluate(
    name: str,
    X: np.ndarray,
    y: np.ndarray,
    feature_names: list[str],
    use_xgboost: bool = False,
    min_accuracy: float = 0.65,
) -> tuple[Pipeline, dict]:
    """Train a model, print metrics, return (model, metrics_dict)."""
    logger.info("─" * 50)
    logger.info("Training: %s (%d samples, %d features)", name, len(X), X.shape[1])

    # Stratified split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.20, random_state=42, stratify=y,
    )

    pipe = make_xgboost_pipeline() if use_xgboost else make_logreg_pipeline()
    pipe.fit(X_train, y_train)

    # Predictions
    y_pred = pipe.predict(X_test)
    y_proba = pipe.predict_proba(X_test)[:, 1]

    acc = float(accuracy_score(y_test, y_pred))
    ll = float(log_loss(y_test, np.clip(y_proba, 1e-6, 1 - 1e-6)))
    brier = float(brier_score_loss(y_test, y_proba))
    base_rate = float(y_test.mean())

    logger.info("  Accuracy:    %.4f (baseline: %.4f)", acc, max(base_rate, 1 - base_rate))
    logger.info("  Log loss:    %.4f", ll)
    logger.info("  Brier score: %.4f (lower is better; 0.25 = random)", brier)

    # Calibration buckets
    buckets = [(0.0, 0.1), (0.1, 0.3), (0.3, 0.5), (0.5, 0.7), (0.7, 0.9), (0.9, 1.0)]
    logger.info("  Calibration:")
    for lo, hi in buckets:
        mask = (y_proba >= lo) & (y_proba < hi)
        n = int(mask.sum())
        if n > 0:
            pred_mean = float(y_proba[mask].mean())
            actual = float(y_test[mask].mean())
            logger.info("    %.1f-%.1f: pred=%.3f actual=%.3f (n=%d)", lo, hi, pred_mean, actual, n)

    metrics = {
        "name": name,
        "n_samples": len(X),
        "n_features": X.shape[1],
        "feature_names": feature_names,
        "accuracy": acc,
        "log_loss": ll,
        "brier_score": brier,
        "base_rate": base_rate,
        "passes_min_accuracy": acc >= min_accuracy,
    }

    if not metrics["passes_min_accuracy"] and not use_xgboost:
        logger.warning(
            "  ⚠️  Accuracy %.3f below %.2f threshold — consider --xgboost fallback",
            acc, min_accuracy,
        )

    return pipe, metrics


def save_model(pipe: Pipeline, metrics: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump({"model": pipe, "metrics": metrics}, path)
    logger.info("  Saved: %s", path)


# ─────────────────────────────────────────────────────────────────────
#  Driver
# ─────────────────────────────────────────────────────────────────────

def train_mlb(use_xgboost: bool, seasons: Optional[list[int]]) -> None:
    df = load_mlb_states(seasons)
    df = df.dropna(subset=["home_win"])

    # Moneyline
    X, y = mlb_moneyline_features(df)
    pipe, metrics = train_and_evaluate(
        "mlb_moneyline", X, y, MLB_BASE_FEATURES, use_xgboost,
    )
    save_model(pipe, metrics, MODELS_DIR / "mlb_moneyline.joblib")

    # Spread (flexible line)
    X, y = mlb_spread_features(df)
    pipe, metrics = train_and_evaluate(
        "mlb_spread", X, y, MLB_BASE_FEATURES + ["spread_line"], use_xgboost,
    )
    save_model(pipe, metrics, MODELS_DIR / "mlb_spread.joblib")

    # Over/Under (flexible line)
    X, y = mlb_ou_features(df)
    pipe, metrics = train_and_evaluate(
        "mlb_over_under", X, y, MLB_BASE_FEATURES + ["ou_line"], use_xgboost,
    )
    save_model(pipe, metrics, MODELS_DIR / "mlb_over_under.joblib")


def train_nba(use_xgboost: bool, seasons: Optional[list[int]]) -> None:
    df = load_nba_states(seasons)
    df = df.dropna(subset=["home_win"])

    # Moneyline
    X, y = nba_moneyline_features(df)
    pipe, metrics = train_and_evaluate(
        "nba_moneyline", X, y, NBA_BASE_FEATURES, use_xgboost,
    )
    save_model(pipe, metrics, MODELS_DIR / "nba_moneyline.joblib")

    # Spread
    X, y = nba_spread_features(df)
    pipe, metrics = train_and_evaluate(
        "nba_spread", X, y, NBA_BASE_FEATURES + ["spread_line"], use_xgboost,
    )
    save_model(pipe, metrics, MODELS_DIR / "nba_spread.joblib")

    # Over/Under
    X, y = nba_ou_features(df)
    pipe, metrics = train_and_evaluate(
        "nba_over_under", X, y, NBA_BASE_FEATURES + ["ou_line"], use_xgboost,
    )
    save_model(pipe, metrics, MODELS_DIR / "nba_over_under.joblib")


def main() -> int:
    parser = argparse.ArgumentParser(description="Train EV models for MLB and NBA.")
    parser.add_argument("--sport", choices=["mlb", "nba", "both"], default="both")
    parser.add_argument("--seasons", type=int, nargs="*", default=None,
                        help="Specific seasons to train on (default: all available)")
    parser.add_argument("--xgboost", action="store_true",
                        help="Use XGBoost instead of logistic regression")
    args = parser.parse_args()

    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    if args.sport in ("mlb", "both"):
        train_mlb(args.xgboost, args.seasons)
    if args.sport in ("nba", "both"):
        train_nba(args.xgboost, args.seasons)

    logger.info("=" * 50)
    logger.info("All models trained and saved to %s", MODELS_DIR)
    return 0


if __name__ == "__main__":
    sys.exit(main())
