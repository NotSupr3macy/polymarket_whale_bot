"""
EV decision engine.

Given an enriched TexaskidPosition (cashout quote + live game state),
compute our estimated probability of the bet resolving in the user's
favor, compare it to the current Polymarket cashout price, and decide
whether to alert.

Alert rule (cashout-only):
    CASH OUT if  p_hold  <  cashout_price  -  EDGE_MARGIN
                 (i.e. holding is worse than taking the money now)

Where:
    p_hold       = model-estimated probability the bet wins at resolution
    cashout_price = current Polymarket midpoint (what you'd sell for)
    EDGE_MARGIN  = required safety margin (default 0.05 = 5 cents)

This is strictly a "should I sell?" engine. It never suggests buying.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import joblib
import numpy as np

from .live_feed_mlb import MLBGameState
from .live_feed_nba import NBAGameState
from .position_manager import BetParse, TexaskidPosition
from .team_mappings import get_mlb_team_name, get_nba_team_name  # noqa: F401


logger = logging.getLogger(__name__)

MODELS_DIR = Path(__file__).resolve().parent / "models"

# Default safety margin — require the model to be at least this far below
# the cashout price before we suggest selling.
DEFAULT_EDGE_MARGIN = 0.05

# Minimum size/price conditions to bother alerting
MIN_POSITION_USD = 100.0
MIN_TIME_REMAINING_NBA_SEC = 60          # don't alert in the last minute

# ── Early-game guards ──
# Before this many outs elapsed, MLB O/U (and spread) are essentially noise.
# Bottom of 5th start = (5-1)*6 + 1*3 + 0 = 27 outs. Require at least that.
MLB_OU_MIN_OUTS_ELAPSED = 27
MLB_SPREAD_MIN_OUTS_ELAPSED = 24         # top/bot 5th
MLB_MONEYLINE_MIN_OUTS_ELAPSED = 18      # bottom of 4th

# NBA: don't alert during Q1 — model output is too noisy
NBA_MIN_ELAPSED_SEC = 720                # full Q1 done

# ── Lock-in-loss guard ──
# "Cashout recommended" implies selling. If cashout price is below entry,
# we'd be locking in a loss. Only do that when the model is VERY confident
# AND the game is past halfway.
LOCK_IN_LOSS_MARGIN = 0.03               # cashout must exceed entry + 0.03 normally
STRONG_EDGE_MARGIN = 0.15                # else require edge of at least 15¢


@dataclass
class Decision:
    """Result of evaluating one position."""
    action: str                  # "cashout" | "hold" | "skip"
    reason: str
    p_hold: Optional[float]      # model probability the bet wins
    p_hold_home: Optional[float] # raw model output (home-side win prob)
    cashout_price: Optional[float]
    edge: Optional[float]        # cashout_price - p_hold (positive = cash out)
    expected_loss_usd: Optional[float]


class DecisionEngine:
    """Loads 6 trained models and evaluates cashout decisions."""

    def __init__(
        self,
        models_dir: Path = MODELS_DIR,
        edge_margin: float = DEFAULT_EDGE_MARGIN,
    ) -> None:
        self.models_dir = Path(models_dir)
        self.edge_margin = edge_margin
        self._models: dict[str, object] = {}
        self._feature_names: dict[str, list[str]] = {}
        self._load_all()

    def _load_all(self) -> None:
        for name in [
            "mlb_moneyline", "mlb_spread", "mlb_over_under",
            "nba_moneyline", "nba_spread", "nba_over_under",
        ]:
            path = self.models_dir / f"{name}.joblib"
            if not path.exists():
                logger.warning("Model missing: %s", path)
                continue
            blob = joblib.load(path)
            self._models[name] = blob["model"]
            self._feature_names[name] = blob["metrics"]["feature_names"]
            logger.info("Loaded model %s (acc=%.3f)", name, blob["metrics"]["accuracy"])

    # ─────────────────────────────────────────────────────────
    #  Feature builders (must mirror model_training.py)
    # ─────────────────────────────────────────────────────────

    @staticmethod
    def _mlb_base_row(state: MLBGameState) -> list[float]:
        """Must stay in lockstep with MLB_BASE_FEATURES in model_training.py."""
        inning = state.inning
        top_bottom = state.top_bottom
        outs = state.outs
        total = state.home_score + state.away_score
        outs_elapsed = max(0, (inning - 1) * 6 + top_bottom * 3 + outs)
        outs_remaining = max(0, 54 - outs_elapsed)
        safe_outs = max(outs_elapsed, 3)
        pace_runs = (total / safe_outs) * 54.0
        return [
            float(inning),
            float(top_bottom),
            float(outs),
            float(state.runners_on),
            float(state.home_score),
            float(state.away_score),
            float(state.home_score - state.away_score),
            float(total),
            float(outs_elapsed),
            float(outs_remaining),
            float(pace_runs),
        ]

    @staticmethod
    def _mlb_outs_elapsed(state: MLBGameState) -> int:
        return max(0, (state.inning - 1) * 6 + state.top_bottom * 3 + state.outs)

    @staticmethod
    def _nba_base_row(state: NBAGameState) -> list[float]:
        return [
            float(state.period),
            float(state.time_remaining_sec),
            float(state.game_time_elapsed_sec),
            float(state.home_score),
            float(state.away_score),
            float(state.home_score - state.away_score),
            float(state.home_score + state.away_score),
            float(state.pace_estimate),
        ]

    # ─────────────────────────────────────────────────────────
    #  Model inference
    # ─────────────────────────────────────────────────────────

    def _predict_proba(self, model_name: str, X: np.ndarray) -> Optional[float]:
        model = self._models.get(model_name)
        if model is None:
            return None
        try:
            proba = model.predict_proba(X)[0, 1]
            return float(proba)
        except Exception as e:
            logger.warning("Model %s predict failed: %s", model_name, e)
            return None

    def estimate_hold_probability(
        self,
        parse: BetParse,
        mlb_state: Optional[MLBGameState],
        nba_state: Optional[NBAGameState],
        matched_home_abbr: Optional[str] = None,
    ) -> Optional[float]:
        """
        Return P(bet wins | current state), or None if we can't evaluate.

        For moneyline/spread, we need to know which live-game team corresponds
        to the *picked* team (so we can flip home/away as needed).
        """
        if parse.sport == "MLB" and mlb_state is not None:
            return self._mlb_probability(parse, mlb_state)
        if parse.sport == "NBA" and nba_state is not None:
            return self._nba_probability(parse, nba_state)
        return None

    def _mlb_probability(self, parse: BetParse, s: MLBGameState) -> Optional[float]:
        base = np.array([self._mlb_base_row(s)], dtype=np.float32)

        if parse.bet_type == "moneyline":
            p_home = self._predict_proba("mlb_moneyline", base)
            if p_home is None:
                return None
            return self._orient_side_probability(p_home, parse, s.home_abbr, s.away_abbr)

        if parse.bet_type == "spread" and parse.line is not None:
            # Model target: home_final - away_final > line
            # We'll ask "does the picked side cover?"
            return self._mlb_spread_probability(parse, s)

        if parse.bet_type == "over_under" and parse.line is not None:
            X = np.concatenate([base, [[float(parse.line)]]], axis=1)
            p_over = self._predict_proba("mlb_over_under", X)
            if p_over is None:
                return None
            return p_over if parse.direction.lower() in ("over", "yes") else (1.0 - p_over)

        return None

    def _nba_probability(self, parse: BetParse, s: NBAGameState) -> Optional[float]:
        base = np.array([self._nba_base_row(s)], dtype=np.float32)

        if parse.bet_type == "moneyline":
            p_home = self._predict_proba("nba_moneyline", base)
            if p_home is None:
                return None
            return self._orient_side_probability(p_home, parse, s.home_abbr, s.away_abbr)

        if parse.bet_type == "spread" and parse.line is not None:
            return self._nba_spread_probability(parse, s)

        if parse.bet_type == "over_under" and parse.line is not None:
            X = np.concatenate([base, [[float(parse.line)]]], axis=1)
            p_over = self._predict_proba("nba_over_under", X)
            if p_over is None:
                return None
            return p_over if parse.direction.lower() in ("over", "yes") else (1.0 - p_over)

        return None

    # ─────────────────────────────────────────────────────────
    #  Side orientation
    # ─────────────────────────────────────────────────────────

    def _orient_side_probability(
        self,
        p_home_wins: float,
        parse: BetParse,
        home_abbr: str,
        away_abbr: str,
    ) -> Optional[float]:
        """
        Translate raw model "home wins" probability into "user's side wins".
        """
        picked = parse.picked_team_abbr
        if not picked:
            return None
        if picked == home_abbr:
            return p_home_wins
        if picked == away_abbr:
            return 1.0 - p_home_wins
        # Title-mentioned teams might also match
        if parse.team1_abbr == picked:
            return p_home_wins if parse.team1_abbr == home_abbr else (1.0 - p_home_wins)
        return None

    def _mlb_spread_probability(self, parse: BetParse, s: MLBGameState) -> Optional[float]:
        """
        The title line is stated relative to one side, e.g. 'Spread: Phillies (-1.5)'.
        If the user's picked_team is the favorite, they need home_diff > 1.5 (or >1.5
        relative to their side); if the user picked the dog, they need the opposite.
        We normalize the line so the feature is always "home - away > line_home",
        then flip the probability based on who the user picked.
        """
        line = parse.line
        if line is None or not parse.picked_team_abbr:
            return None

        # Determine whether parse.team1_abbr (the team the line is stated relative to)
        # is home or away in the live game
        if parse.team1_abbr == s.home_abbr:
            line_home = float(line)              # home favored by |line|
        elif parse.team1_abbr == s.away_abbr:
            line_home = -float(line)             # away favored by |line|
        else:
            return None

        base = np.array([self._mlb_base_row(s)], dtype=np.float32)
        X = np.concatenate([base, [[line_home]]], axis=1)
        p_home_covers = self._predict_proba("mlb_spread", X)
        if p_home_covers is None:
            return None

        # Model target: home_final - away_final > line_home
        if parse.picked_team_abbr == s.home_abbr:
            return p_home_covers
        if parse.picked_team_abbr == s.away_abbr:
            return 1.0 - p_home_covers
        return None

    def _nba_spread_probability(self, parse: BetParse, s: NBAGameState) -> Optional[float]:
        line = parse.line
        if line is None or not parse.picked_team_abbr:
            return None

        if parse.team1_abbr == s.home_abbr:
            line_home = float(line)
        elif parse.team1_abbr == s.away_abbr:
            line_home = -float(line)
        else:
            return None

        base = np.array([self._nba_base_row(s)], dtype=np.float32)
        X = np.concatenate([base, [[line_home]]], axis=1)
        p_home_covers = self._predict_proba("nba_spread", X)
        if p_home_covers is None:
            return None

        if parse.picked_team_abbr == s.home_abbr:
            return p_home_covers
        if parse.picked_team_abbr == s.away_abbr:
            return 1.0 - p_home_covers
        return None

    # ─────────────────────────────────────────────────────────
    #  Top-level decision
    # ─────────────────────────────────────────────────────────

    def evaluate(self, pos: TexaskidPosition) -> Decision:
        """Decide whether the user should cash out this position."""
        skip = lambda reason, cashout=None: Decision(  # noqa: E731
            "skip", reason, None, None, cashout, None, None,
        )

        if pos.current_size_usd < MIN_POSITION_USD:
            return skip("size_below_min")
        if pos.parse is None or pos.parse.sport not in ("MLB", "NBA"):
            return skip("non_supported_sport")
        if pos.cashout is None:
            return skip("no_cashout_quote")

        cashout = pos.cashout.mid_price
        if cashout <= 0.01 or cashout >= 0.99:
            return skip("cashout_at_extreme", cashout)

        # Need a live game
        if pos.sport == "MLB" and pos.mlb_state is None:
            return skip("no_mlb_live_state", cashout)
        if pos.sport == "NBA" and pos.nba_state is None:
            return skip("no_nba_live_state", cashout)
        status = pos.live_status or ""
        if status != "LIVE":
            return skip(f"game_not_live:{status}", cashout)

        # ── Early-game guards (too noisy to act on) ──
        if pos.sport == "MLB" and pos.mlb_state is not None:
            outs_elapsed = self._mlb_outs_elapsed(pos.mlb_state)
            bet = pos.parse.bet_type
            min_outs = {
                "over_under": MLB_OU_MIN_OUTS_ELAPSED,
                "spread": MLB_SPREAD_MIN_OUTS_ELAPSED,
                "moneyline": MLB_MONEYLINE_MIN_OUTS_ELAPSED,
            }.get(bet, MLB_MONEYLINE_MIN_OUTS_ELAPSED)
            if outs_elapsed < min_outs:
                return skip(
                    f"mlb_too_early:{bet}_outs={outs_elapsed}<{min_outs}", cashout,
                )

        if pos.sport == "NBA" and pos.nba_state is not None:
            if pos.nba_state.game_time_elapsed_sec < NBA_MIN_ELAPSED_SEC:
                return skip(
                    f"nba_too_early:elapsed={pos.nba_state.game_time_elapsed_sec}s", cashout,
                )
            if pos.nba_state.time_remaining_sec < MIN_TIME_REMAINING_NBA_SEC \
                    and pos.nba_state.period >= 4:
                return skip("nba_final_minute", cashout)

        p_hold = self.estimate_hold_probability(
            pos.parse, pos.mlb_state, pos.nba_state,
        )
        if p_hold is None:
            return skip("model_unavailable_or_unresolved_side", cashout)

        edge = cashout - p_hold
        expected_loss = pos.current_size_usd * edge if edge > 0 else 0.0

        # Basic threshold not met → hold
        if edge <= self.edge_margin:
            return Decision(
                "hold",
                f"p_hold={p_hold:.3f} vs cashout={cashout:.3f} (edge={edge:+.3f})",
                p_hold, None, cashout, edge, expected_loss,
            )

        # ── Lock-in-loss guard ──
        # If the cashout price isn't materially above entry, the user would
        # be locking in a loss. Require strong edge + past halfway.
        entry = pos.first_seen_price
        is_lock_in_loss = cashout < entry + LOCK_IN_LOSS_MARGIN
        if is_lock_in_loss:
            if edge < STRONG_EDGE_MARGIN:
                return skip(
                    f"lock_in_loss_edge_small:cashout={cashout:.3f}<entry+{LOCK_IN_LOSS_MARGIN}"
                    f"_edge={edge:.3f}<{STRONG_EDGE_MARGIN}",
                    cashout,
                )
            if not self._past_halfway(pos):
                return skip(
                    f"lock_in_loss_too_early:cashout={cashout:.3f}<entry+{LOCK_IN_LOSS_MARGIN}",
                    cashout,
                )

        return Decision(
            action="cashout",
            reason=f"p_hold={p_hold:.3f} < cashout={cashout:.3f} (edge={edge:+.3f})",
            p_hold=p_hold,
            p_hold_home=None,
            cashout_price=cashout,
            edge=edge,
            expected_loss_usd=expected_loss,
        )

    def _past_halfway(self, pos: TexaskidPosition) -> bool:
        """Game is past the 50% mark — acceptable context for locking in a loss."""
        if pos.mlb_state is not None:
            return self._mlb_outs_elapsed(pos.mlb_state) >= 27
        if pos.nba_state is not None:
            return pos.nba_state.game_time_elapsed_sec >= 1440   # half of 2880s
        return False
