# Polymarket Live EV Cashout Engine

Live expected-value cashout engine for Polymarket sports positions. Monitors
open NBA and MLB positions in real time, estimates true win probability using
trained models, and alerts when the market is offering more than a position
is worth (cashout > hold).

## Architecture

```
ev_engine/
├── team_mappings.py          # MLB + NBA team IDs, slug parsing, fuzzy matching
├── data_acquisition/         # Historical data pullers
│   ├── mlb_puller.py         # MLB Stats API -> state snapshot CSVs
│   └── nba_puller.py         # ESPN PBP -> state snapshot CSVs
├── data/                     # Pulled CSVs (gitignored)
│   ├── mlb/
│   │   ├── 2022_states.csv
│   │   ├── 2023_states.csv
│   │   ├── 2024_states.csv
│   │   ├── 2025_states.csv
│   │   └── checkpoint.json
│   └── nba/
│       ├── 2022_states.csv   ... etc
│       └── checkpoint.json
├── models/                   # Trained .joblib models (phase 2)
└── scripts/                  # Launcher shell scripts
```

## Phase 1: Historical Data Pull

### MLB
```bash
python -m ev_engine.data_acquisition.mlb_puller --seasons 2022 2023 2024 2025
```

Pulls ~9,720 games from statsapi.mlb.com (4 seasons × ~2,430 games each).
Expected runtime: 4-8 hours at default concurrency.

### NBA
```bash
python -m ev_engine.data_acquisition.nba_puller --seasons 2022 2023 2024 2025
```

Pulls ~5,260 games from ESPN's PBP API (4 seasons × ~1,315 games each).
Expected runtime: 3-6 hours at default concurrency.

### Checkpointing

Both pullers write `data/<sport>/checkpoint.json` tracking completed game IDs.
Restart the command to resume from where it stopped. The CSV files are
appended to, never truncated.

## Integration

This module lives inside the existing whale-bot repo so:
- It shares the same `.env` (Telegram credentials, etc.)
- Deployment is a single `git pull`
- It can read `trades.db` directly for texaskid position tracking

## Phase 2+ (future)

- Model training (phase 2)
- Live game state feeds (phase 3)
- Polymarket orderbook + position manager (phase 4)
- EV engine + cashout-only alerts (phase 5)
- Main loop + deployment (phase 6)
