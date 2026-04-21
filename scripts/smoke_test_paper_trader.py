#!/usr/bin/env python3
"""Smoke test the paper trader end-to-end against a seeded temp DB.

Covers:
  1. init_db() creates tables + seeds paper_state with $100 bankroll
  2. Candidate query finds unmuted open whale positions
  3. Open path deducts bankroll + inserts paper_position
  4. Resolution path updates bankroll + marks paper_position outcome
  5. Consensus detection: 2nd-whale open gets conviction_mult=1.5
  6. Tilt guard: post-LOSS open for same whale gets conviction_mult=0.5
  7. nbasniper bypass: muted_reason='sport' still produces a paper open
  8. deployment cap: can't exceed 50% of bankroll
  9. restart: re-init against existing DB preserves state
"""
import os, sys, sqlite3, tempfile, asyncio
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Make paper_trader importable + set DRY_RUN BEFORE importing (Telegram calls no-op)
tmpfile = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
tmpfile.close()
os.environ['DB_PATH'] = tmpfile.name
os.environ['PAPER_TRADER_SILENT'] = '1'
os.environ['PAPER_BOT_TOKEN'] = 'fake'
os.environ['PAPER_BOT_CHAT_ID'] = '0'

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'monitor'))
import paper_trader as pt

# ── Mock Gamma so synthetic cids in the smoke test resolve deterministically.
# The test seeds fake tracker rows (cid='0xabc1', etc.) that don't exist in
# real Gamma. Real Gamma would return None for every one, which under the
# Apr 19 safety change would mark every close as RESOLVED break-even and
# break T5/T6. The mock mirrors the real Gamma by reading our synthetic
# tracker row's outcome field and translating it into Gamma's response shape.
_MOCK_GAMMA_OUTCOMES: dict[tuple[str, str], tuple[str, float]] = {}

def mock_set_gamma_outcome(cid: str, direction: str, outcome: str, price: float):
    _MOCK_GAMMA_OUTCOMES[(cid, direction)] = (outcome, price)

async def _mock_gamma(cid: str, direction: str):
    # If test explicitly set an outcome, return it. Otherwise fall through
    # to None (simulating Gamma unreachable) so we can test that path too.
    return _MOCK_GAMMA_OUTCOMES.get((cid, direction))

pt.resolve_ambiguous_via_gamma = _mock_gamma

# ── Seed parent whale-tracker tables (simulate tracker output) ───────
def seed_source_tables(db_path):
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS tracked_whale_positions (
            wallet TEXT NOT NULL, alias TEXT NOT NULL,
            condition_id TEXT NOT NULL, direction TEXT NOT NULL,
            market_title TEXT NOT NULL,
            first_seen_price REAL, first_seen_size_usd REAL,
            current_size_usd REAL, current_price REAL,
            status TEXT NOT NULL DEFAULT 'open', outcome TEXT,
            pnl REAL, first_seen_at TEXT NOT NULL,
            last_updated TEXT, alert_sent INTEGER DEFAULT 0,
            resolved_at TEXT, muted_reason TEXT,
            PRIMARY KEY (wallet, condition_id)
        );
        CREATE TABLE IF NOT EXISTS texaskid_positions (
            condition_id TEXT PRIMARY KEY, direction TEXT NOT NULL,
            market_title TEXT NOT NULL,
            first_seen_price REAL, first_seen_size_usd REAL,
            current_size_usd REAL, current_price REAL,
            status TEXT NOT NULL DEFAULT 'open', outcome TEXT,
            pnl REAL, first_seen_at TEXT NOT NULL,
            last_updated TEXT, alert_sent INTEGER DEFAULT 0,
            resolved_at TEXT, muted_reason TEXT
        );
    """)
    conn.commit(); conn.close()

def insert_whale_position(db_path, alias, cid, direction, title,
                          entry, size, first_seen_at, muted_reason=None,
                          table='tracked_whale_positions'):
    conn = sqlite3.connect(db_path)
    if table == 'tracked_whale_positions':
        conn.execute(
            """INSERT OR REPLACE INTO tracked_whale_positions
               (wallet, alias, condition_id, direction, market_title,
                first_seen_price, first_seen_size_usd, current_size_usd,
                current_price, status, first_seen_at, last_updated,
                alert_sent, muted_reason)
               VALUES (?,?,?,?,?,?,?,?,?,'open',?,?,1,?)""",
            (f'0x{alias}', alias, cid, direction, title, entry, size, size,
             entry, first_seen_at, first_seen_at, muted_reason),
        )
    else:
        conn.execute(
            """INSERT OR REPLACE INTO texaskid_positions
               (condition_id, direction, market_title,
                first_seen_price, first_seen_size_usd, current_size_usd,
                current_price, status, first_seen_at, last_updated,
                alert_sent, muted_reason)
               VALUES (?,?,?,?,?,?,?,'open',?,?,1,?)""",
            (cid, direction, title, entry, size, size, entry,
             first_seen_at, first_seen_at, muted_reason),
        )
    conn.commit(); conn.close()

def mark_resolved(db_path, alias, cid, outcome, table='tracked_whale_positions'):
    conn = sqlite3.connect(db_path)
    now = datetime.now(timezone.utc).isoformat()
    if table == 'tracked_whale_positions':
        conn.execute(
            "UPDATE tracked_whale_positions SET status='closed', outcome=?, resolved_at=? WHERE alias=? AND condition_id=?",
            (outcome, now, alias, cid),
        )
    else:
        conn.execute(
            "UPDATE texaskid_positions SET status='closed', outcome=?, resolved_at=? WHERE condition_id=?",
            (outcome, now, cid),
        )
    conn.commit(); conn.close()

# ── Tests ─────────────────────────────────────────────────────────────
async def run_tests():
    db = tmpfile.name
    seed_source_tables(db)
    pt.init_db(db)

    # Test 1: init creates paper_state with $100 bankroll
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    state = pt.load_state(conn)
    assert state['bankroll_usd'] == 100.0, f'Expected $100, got ${state["bankroll_usd"]}'
    print('[OK] T1: paper_state seeded with $100 bankroll')

    # Need to set started_at to 1 min ago so subsequent positions pass filter
    past = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    conn.execute('UPDATE paper_state SET started_at=? WHERE id=1', (past,))
    conn.commit()

    future_seen = datetime.now(timezone.utc).isoformat()

    # Seed: kch123 unmuted DOG position ($5 base, in BASE_ALLOC).
    # Entry 0.45 because kch123 now has the dogs-or-spreads filter +
    # a max_entry cap of 0.50 (Apr 19 strategic overhaul).
    insert_whale_position(db, 'kch123', '0xabc1', 'Lakers',
                          'Lakers vs. Celtics', 0.45, 15000, future_seen)
    # Seed: texaskid unmuted dog position (SHADOW whale — should log-only)
    insert_whale_position(db, 'texaskid', '0xabc2', 'Rockies',
                          'Rockies vs. Dodgers', 0.42, 20000, future_seen,
                          table='texaskid_positions')
    # Seed: nbasniper position with muted_reason='sport' (shadow bypass, opens).
    # Entry 0.40 to pass both the dogs-or-spreads filter and max_entry=0.45.
    insert_whale_position(db, 'nbasniper', '0xabc3', 'Mavs',
                          'Mavs vs. Warriors', 0.40, 25000, future_seen,
                          muted_reason='sport')
    # Seed: bigsix — muted with require_multi_trade BUT paper trader bypasses
    # this mute (like nbasniper sport-bypass). Entry=$0.60 is a FAV on h2h-ml,
    # so bigsix's whale_filter should still reject it. Net: appears in
    # query_candidates, filtered out in main loop.
    insert_whale_position(db, 'bigsix', '0xabc4', 'Rangers',
                          'Rangers vs. Kings', 0.60, 10000, future_seen,
                          muted_reason='require_multi_trade')
    # Seed: TheOnlyHuman — also now a SHADOW whale (Apr 19 mute).
    # Should appear in query_candidates but get skipped in the main loop.
    insert_whale_position(db, 'TheOnlyHuman', '0xabc5', 'Knicks',
                          'Knicks vs. Heat', 0.50, 18000, future_seen)

    # Test 2: candidate query returns all 5 unmuted/bypassed positions.
    # bigsix's require_multi_trade mute is now bypassed (like nbasniper),
    # so he appears here — but his whale_filter will reject him in T3.
    cands = pt.query_candidates(conn, past)
    aliases = sorted(c['alias'] for c in cands)
    assert aliases == ['TheOnlyHuman', 'bigsix', 'kch123', 'nbasniper',
                       'texaskid'], f'Expected 5, got {aliases}'
    print(f'[OK] T2: candidate query returned {aliases} '
          f'(bigsix require_multi_trade bypassed, texaskid/TheOnlyHuman shadow)')

    # Test 3: run one tick → kch123 + nbasniper open; texaskid + TheOnlyHuman
    # shadow-skip; bigsix whale-filter rejects (fav@$0.60 on h2h-ml).
    async def _one_tick():
        # Inline version of the poll loop, just one pass.
        # Mirrors production main loop ordering:
        #   shadow-skip → base_alloc check → whale filter → max_entry → open
        state_ = pt.load_state(conn)
        # Resolutions first (none yet)
        # Then signals
        for sig in pt.query_candidates(conn, state_['started_at']):
            if sig['alias'] in pt.SHADOW_WHALES:
                pt.log_shadow_candidate(sig)
                continue
            base_frac = pt.BASE_ALLOC.get(sig['alias'])
            if base_frac is None: continue
            whale_filter = pt.WHALE_FILTERS.get(sig['alias'])
            if whale_filter and not whale_filter(sig):
                continue  # per-whale edge filter rejected
            max_entry = pt.WHALE_MAX_ENTRY.get(sig['alias'])
            if max_entry is not None and sig['entry_price'] > max_entry:
                continue  # entry above break-even cap
            base_size = pt.STARTING_BANKROLL * base_frac
            mult, desc = pt.compute_conviction_mult(
                conn, sig['alias'], sig['cid'], sig['direction'],
            )
            size = min(base_size * mult, pt.HARD_SIZE_CAP_USD)
            ok, _ = pt.can_open(conn, size, state_, alias=sig['alias'])
            if ok:
                await pt.open_paper_position(conn, sig, size, mult, state_)

    await _one_tick()

    n_open = conn.execute(
        "SELECT COUNT(*) FROM paper_positions WHERE outcome='OPEN'"
    ).fetchone()[0]
    assert n_open == 2, \
        f'Expected 2 open (texaskid/TOH shadow, bigsix filtered), got {n_open}'
    print(f'[OK] T3: 2 paper positions opened '
          f'(texaskid + TheOnlyHuman shadow-skipped, bigsix filter-rejected)')

    # Check bankroll = 100 - (kch123 $5 + nbasniper $4) = 91
    state_after = pt.load_state(conn)
    expected_bankroll = 100.0 - (5.0 + 4.0)
    assert abs(state_after['bankroll_usd'] - expected_bankroll) < 0.01, \
        f'Expected bankroll ${expected_bankroll}, got ${state_after["bankroll_usd"]}'
    print(f'[OK] T4: bankroll correctly deducted: ${state_after["bankroll_usd"]:.2f}')

    # T4.5: shadow-log fired for BOTH texaskid AND TheOnlyHuman
    assert ('texaskid', '0xabc2') in pt._shadow_logged_keys, \
        f'Expected texaskid shadow-log; got {pt._shadow_logged_keys}'
    assert ('TheOnlyHuman', '0xabc5') in pt._shadow_logged_keys, \
        f'Expected TheOnlyHuman shadow-log; got {pt._shadow_logged_keys}'
    print(f'[OK] T4.5: shadow log captured BOTH texaskid + TheOnlyHuman')

    # Test 5: Resolve kch123 position as WIN → bankroll refills
    mark_resolved(db, 'kch123', '0xabc1', 'WIN')
    mock_set_gamma_outcome('0xabc1', 'Lakers', 'WIN', 1.0)
    # Run resolution phase
    async def _resolve_tick():
        state_ = pt.load_state(conn)
        open_rows = conn.execute(
            "SELECT id, whale_alias, condition_id, direction, market_title,"
            " entry_price, paper_size_usd, source_table FROM paper_positions"
            " WHERE outcome='OPEN'"
        ).fetchall()
        for row in open_rows:
            pp = dict(row)
            src = pt.query_source_status(
                conn, pp['source_table'], pp['whale_alias'], pp['condition_id'],
            )
            if src and src['status'] == 'closed' and src['outcome'] in ('WIN','LOSS'):
                await pt.close_paper_position(conn, pp, src, state_)
    await _resolve_tick()

    # kch123 WIN at entry 0.45: pnl = 5 * (1/0.45 - 1) = 5 * 1.2222 = +$6.11
    # Bankroll gain: 5 (stake return) + 6.11 (pnl) = 11.11
    state_after = pt.load_state(conn)
    expected = (100.0 - 5 - 4) + 5.0 / 0.45  # stake back + payout at $1
    assert abs(state_after['bankroll_usd'] - expected) < 0.01, \
        f'Expected ${expected:.2f}, got ${state_after["bankroll_usd"]:.2f}'
    print(f'[OK] T5: WIN resolution refilled bankroll to ${state_after["bankroll_usd"]:.2f} (expected ${expected:.2f})')

    # Test 6: Resolve nbasniper as LOSS → no bankroll return
    mark_resolved(db, 'nbasniper', '0xabc3', 'LOSS')
    mock_set_gamma_outcome('0xabc3', 'Mavs', 'LOSS', 0.0)
    bankroll_before = state_after['bankroll_usd']
    await _resolve_tick()
    state_after = pt.load_state(conn)
    # LOSS: bankroll += 0 (no stake return)
    assert abs(state_after['bankroll_usd'] - bankroll_before) < 0.01, \
        f'LOSS shouldn\'t change bankroll; before=${bankroll_before:.2f}, after=${state_after["bankroll_usd"]:.2f}'
    print(f'[OK] T6: LOSS resolution consumed stake (bankroll unchanged: ${state_after["bankroll_usd"]:.2f})')

    # Test 7: Consensus — seed two whales on SAME cid+direction → 2nd gets 1.5x
    # Reset started_at to ensure new positions are in-window
    conn.execute('UPDATE paper_state SET started_at=? WHERE id=1', (past,))
    conn.commit()
    # Clean slate for consensus test — delete old paper_positions
    conn.execute("DELETE FROM paper_positions")
    conn.execute("UPDATE paper_state SET bankroll_usd=100.0 WHERE id=1")
    conn.commit()

    future2 = datetime.now(timezone.utc).isoformat()
    insert_whale_position(db, 'bigsix', '0xcon1', 'Jets',
                          'Jets vs. Sharks', 0.45, 50000, future2)
    insert_whale_position(db, 'kch123', '0xcon1', 'Jets',
                          'Jets vs. Sharks', 0.45, 80000, future2)

    await _one_tick()
    # bigsix opens at 1.0x, kch123 opens at 1.5x (bigsix now counts as peer)
    rows = conn.execute(
        "SELECT whale_alias, conviction_mult FROM paper_positions"
        " WHERE condition_id='0xcon1' ORDER BY opened_at"
    ).fetchall()
    assert len(rows) == 2, f'Expected 2 rows, got {len(rows)}'
    assert rows[0][1] == 1.0, f'First whale mult should be 1.0, got {rows[0][1]}'
    assert rows[1][1] == 1.5, f'Second whale mult should be 1.5 (consensus), got {rows[1][1]}'
    print(f'[OK] T7: consensus detected — 2nd whale opened at 1.5x mult')

    # Test 8: Tilt guard — seed a LOSS for kch123 in the last hour, then try to open
    # Insert a historical resolved LOSS for kch123 (1h ago)
    one_hr_ago = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    conn.execute(
        """INSERT INTO paper_positions
           (whale_alias, condition_id, direction, market_title, entry_price,
            paper_size_usd, bankroll_at_open, conviction_mult,
            opened_at, resolved_at, outcome, resolution_price, paper_pnl,
            source_table)
           VALUES ('kch123','0xprior','X','tilt-test',0.5,5,100,1.0,?,?,'LOSS',0.0,-5,
                   'tracked_whale_positions')""",
        (one_hr_ago, one_hr_ago),
    )
    conn.commit()
    mult, desc = pt.compute_conviction_mult(conn, 'kch123', '0xtilt1', 'X')
    assert mult == 0.5, f'Expected 0.5 mult (tilt guard), got {mult}: {desc}'
    print(f'[OK] T8: tilt guard applied after recent LOSS (mult=0.5)')

    # Test 8.5: GIAYN is exempt from tilt guard (TILT_GUARD_EXCLUDE set).
    # Seed a recent LOSS for him + assert mult stays 1.0, not 0.5.
    conn.execute(
        """INSERT INTO paper_positions
           (whale_alias, condition_id, direction, market_title, entry_price,
            paper_size_usd, bankroll_at_open, conviction_mult,
            opened_at, resolved_at, outcome, resolution_price, paper_pnl,
            source_table)
           VALUES ('GamblingIsAllYouNeed','0xgiayn_prior','X','tilt-test',0.5,4,100,1.0,?,?,'LOSS',0.0,-4,
                   'tracked_whale_positions')""",
        (one_hr_ago, one_hr_ago),
    )
    conn.commit()
    mult, desc = pt.compute_conviction_mult(conn, 'GamblingIsAllYouNeed', '0xgiayn_new', 'X')
    assert mult == 1.0, f'GIAYN should be exempt from tilt (expected 1.0, got {mult}): {desc}'
    print(f'[OK] T8.5: GIAYN exempt from tilt guard (mult={mult} as expected)')

    # Test 9: deployment cap ($60 max at 60% of $100 equity)
    # Already open: bigsix $3 + kch123 $7.50 (5 * 1.5) = $10.50
    # With $89.50 bankroll + $10.50 deployed = $100 equity, cap = $60
    # Try to open $55 more — should fail (would exceed $60 cap with $10.50 already out)
    big_sig = {
        'alias': 'TheOnlyHuman', 'cid': '0xbigtest', 'direction': 'X',
        'title': 'big market', 'entry_price': 0.5, 'whale_size_usd': 50000,
        'source_table': 'tracked_whale_positions', 'muted_reason': None,
        'first_seen_at': future2,
    }
    state_ = pt.load_state(conn)
    ok, reason = pt.can_open(conn, 55.0, state_)
    assert not ok, f'Expected rejection at 55$, got ok={ok}'
    print(f'[OK] T9: deploy cap enforced: "{reason}"')

    # Test 10: idempotent init_db — rerun should not crash or change state
    pt.init_db(db)
    state_after = pt.load_state(conn)
    # Bankroll should be unchanged
    assert abs(state_after['bankroll_usd'] - state_['bankroll_usd']) < 0.01
    print(f'[OK] T10: idempotent init_db — state preserved on re-init')

    # Test 11: per-whale concurrent cap (GIAYN limited to 8 open)
    # Clean slate, seed 8 GIAYN opens, assert 9th is rejected.
    conn.execute("DELETE FROM paper_positions")
    conn.execute("UPDATE paper_state SET bankroll_usd=100.0 WHERE id=1")
    conn.commit()
    now_iso = datetime.now(timezone.utc).isoformat()
    for i in range(8):
        conn.execute(
            """INSERT INTO paper_positions
               (whale_alias, condition_id, direction, market_title, entry_price,
                paper_size_usd, bankroll_at_open, conviction_mult,
                opened_at, outcome, source_table)
               VALUES ('GamblingIsAllYouNeed', ?, 'X', 'cap-test',
                       0.5, 4, 100, 1.0, ?, 'OPEN', 'tracked_whale_positions')""",
            (f"0xcap{i}", now_iso),
        )
    conn.commit()
    state_ = pt.load_state(conn)
    ok_no_alias, _ = pt.can_open(conn, 4.0, state_)  # global cap not yet hit
    assert ok_no_alias, "deploy cap shouldn't be an issue with $32 on $100"
    ok_with_alias, reason = pt.can_open(
        conn, 4.0, state_, alias='GamblingIsAllYouNeed',
    )
    assert not ok_with_alias, f"Expected per-whale cap rejection, got ok={ok_with_alias}"
    assert 'per-whale cap' in reason, f"Expected 'per-whale cap' in reason: {reason}"
    print(f'[OK] T11: per-whale cap enforced: "{reason}"')

    # Test 12: cross-whale duplicate filter.
    # Seed an existing paper_position on (cid, direction) that opened
    # 45 min ago (OUTSIDE the 30-min consensus window). A new tracked_whale
    # candidate for a DIFFERENT whale on the same (cid, direction) should be
    # filtered out by query_candidates.
    conn.execute("DELETE FROM paper_positions")
    conn.commit()
    forty_five_min_ago = (
        datetime.now(timezone.utc) - timedelta(minutes=45)
    ).isoformat()
    conn.execute(
        """INSERT INTO paper_positions
           (whale_alias, condition_id, direction, market_title, entry_price,
            paper_size_usd, bankroll_at_open, conviction_mult,
            opened_at, outcome, source_table)
           VALUES ('bigsix','0xdupe','Blues','dupe-test',0.5,3,100,1.0,?,
                   'OPEN','tracked_whale_positions')""",
        (forty_five_min_ago,),
    )
    conn.commit()
    # Seed kch123 tracker row on the same (cid, direction)
    insert_whale_position(
        db, 'kch123', '0xdupe', 'Blues', 'dupe-test', 0.5, 1000, future2,
    )
    cands = pt.query_candidates(conn, past)
    aliases_on_dupe = [c['alias'] for c in cands if c['cid'] == '0xdupe']
    assert 'kch123' not in aliases_on_dupe, \
        f"kch123 should be dupe-filtered, got {aliases_on_dupe}"
    print(f'[OK] T12: cross-whale duplicate filter (>30min old) excludes kch123')

    # Test 12.5: same scenario but existing open is WITHIN 30min —
    # new whale IS allowed (consensus window).
    conn.execute("DELETE FROM paper_positions")
    conn.commit()
    ten_min_ago = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    conn.execute(
        """INSERT INTO paper_positions
           (whale_alias, condition_id, direction, market_title, entry_price,
            paper_size_usd, bankroll_at_open, conviction_mult,
            opened_at, outcome, source_table)
           VALUES ('bigsix','0xdupe2','Blues','dupe-test',0.5,3,100,1.0,?,
                   'OPEN','tracked_whale_positions')""",
        (ten_min_ago,),
    )
    conn.commit()
    insert_whale_position(
        db, 'kch123', '0xdupe2', 'Blues', 'dupe-test', 0.5, 1000, future2,
    )
    cands = pt.query_candidates(conn, past)
    aliases_on_consensus = [c['alias'] for c in cands if c['cid'] == '0xdupe2']
    assert 'kch123' in aliases_on_consensus, \
        f"kch123 inside consensus window should pass, got {aliases_on_consensus}"
    print(f'[OK] T12.5: consensus window (<30min) still allows 2nd whale')

    # Test 13: Gamma-None safety — when Gamma lookup returns None, we
    # MUST mark the position as RESOLVED (break-even), NOT trust the
    # tracker's outcome. This prevents the phantom-WIN/phantom-LOSS bug
    # from Apr 19 (sportmaster Padres marked LOSS when Padres actually won).
    conn.execute("DELETE FROM paper_positions")
    conn.execute("UPDATE paper_state SET bankroll_usd=100.0 WHERE id=1")
    conn.commit()
    # Seed an open paper position for kch123 that whale_tracker says WIN but
    # Gamma can't confirm (no mock set for this cid).
    now_iso_t13 = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """INSERT INTO paper_positions
           (whale_alias, condition_id, direction, market_title, entry_price,
            paper_size_usd, bankroll_at_open, conviction_mult,
            opened_at, outcome, source_table)
           VALUES ('kch123','0xgamma_blip','X','gamma-blip-test',
                   0.5, 5, 100, 1.0, ?, 'OPEN', 'tracked_whale_positions')""",
        (now_iso_t13,),
    )
    # Deduct bankroll to match an open position
    conn.execute("UPDATE paper_state SET bankroll_usd=95.0 WHERE id=1")
    conn.commit()
    # Tracker-side says WIN (which would be a phantom since Gamma can't verify)
    insert_whale_position(
        db, 'kch123', '0xgamma_blip', 'X', 'gamma-blip-test',
        0.5, 10000, now_iso_t13,
    )
    mark_resolved(db, 'kch123', '0xgamma_blip', 'WIN')
    # DO NOT set Gamma mock → _mock_gamma returns None → simulates
    # Gamma unreachable / all retries exhausted in production.
    state_ = pt.load_state(conn)
    pp_row = conn.execute(
        "SELECT id, whale_alias, condition_id, direction, market_title,"
        " entry_price, paper_size_usd, source_table FROM paper_positions"
        " WHERE condition_id='0xgamma_blip' AND outcome='OPEN'"
    ).fetchone()
    src = pt.query_source_status(
        conn, 'tracked_whale_positions', 'kch123', '0xgamma_blip',
    )
    await pt.close_paper_position(conn, dict(pp_row), src, state_)
    result = conn.execute(
        "SELECT outcome, paper_pnl FROM paper_positions WHERE id=?",
        (pp_row['id'],),
    ).fetchone()
    assert result[0] == 'RESOLVED', \
        f"Gamma-None should yield RESOLVED, got {result[0]} (phantom-WIN bug regressed!)"
    assert abs(result[1]) < 0.01, \
        f"RESOLVED pnl should be 0, got ${result[1]} (phantom-WIN bug regressed!)"
    state_after = pt.load_state(conn)
    assert abs(state_after['bankroll_usd'] - 100.0) < 0.01, \
        f"RESOLVED should refund stake to 100, got ${state_after['bankroll_usd']}"
    print(f'[OK] T13: Gamma-None correctly yields RESOLVED break-even '
          f'(tracker WIN ignored, prevents phantom-WIN bug)')

    # Test 14: bigsix whale-filter — accept dogs + spreads, reject
    # favorites-on-non-spread and totals.
    bigsix_cases = [
        # (title, entry_price, expected_accept, description)
        ('Knicks vs. Heat', 0.40, True, 'dog ML — accept'),
        ('Knicks vs. Heat', 0.60, False, 'fav ML — reject'),
        ('Spread: Knicks (-5.5)', 0.65, True, 'spread (any price) — accept'),
        ('Spread: Knicks (-5.5)', 0.30, True, 'dog spread — accept'),
        ('Knicks vs. Heat: O/U 215.5', 0.48, False, 'totals — always reject'),
        ('Knicks vs. Heat: O/U 215.5', 0.30, False, 'totals even at dog price — reject'),
    ]
    for title, entry, expected, desc in bigsix_cases:
        sig = {'alias': 'bigsix', 'title': title, 'entry_price': entry}
        actual = pt.WHALE_FILTERS['bigsix'](sig)
        assert actual == expected, (
            f'bigsix filter case [{desc}] expected {expected}, got {actual} '
            f'(title={title}, entry={entry}, subtype={pt.classify_subtype(title)})'
        )
    print(f'[OK] T14: bigsix whale filter correctly routes '
          f'(dogs+spreads accept, favs/totals reject)')

    # Test 15: bigsix mute-bypass — query_candidates should return his rows
    # regardless of muted_reason. Tests both 'require_multi_trade' and 'sport'
    # since his tracker emits both under different scenarios.
    conn.execute("DELETE FROM paper_positions")
    conn.commit()
    past2 = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    future3 = datetime.now(timezone.utc).isoformat()
    for mute_reason, cid in [
        ('require_multi_trade', '0xbigsix_rmt'),
        ('sport', '0xbigsix_sport'),
        ('hour', '0xbigsix_hour'),
    ]:
        insert_whale_position(
            db, 'bigsix', cid, 'Underdogs', f'Dog ML ({mute_reason})',
            0.40, 15000, future3, muted_reason=mute_reason,
        )
    cands = pt.query_candidates(conn, past2)
    bigsix_cids = {c['cid'] for c in cands if c['alias'] == 'bigsix'}
    for cid in ['0xbigsix_rmt', '0xbigsix_sport', '0xbigsix_hour']:
        assert cid in bigsix_cids, \
            f"bigsix mute-bypass missed {cid}: {bigsix_cids}"
    print(f'[OK] T15: bigsix mute-bypass covers all reasons '
          f'(rmt/sport/hour all visible to paper trader)')

    # Test 16: WHALE_MAX_ENTRY enforcement (Apr 20 retune per fingerprints)
    # sportmaster cap=0.67, GIAYN=0.65, kch123=0.90, nbasniper=0.55
    cases = [
        ('sportmaster777',     0.70, False, 'sportmaster 0.70 > cap 0.67'),
        ('sportmaster777',     0.65, True,  'sportmaster 0.65 < cap 0.67'),
        ('GamblingIsAllYouNeed', 0.70, False, 'GIAYN 0.70 > cap 0.65'),
        ('GamblingIsAllYouNeed', 0.60, True,  'GIAYN 0.60 < cap 0.65 (h2h fav)'),
        ('kch123',             0.95, False, 'kch123 0.95 > cap 0.90 (chalk)'),
        ('kch123',             0.85, True,  'kch123 0.85 heavy-fav <= cap 0.90'),
        ('nbasniper',          0.60, False, 'nbasniper 0.60 > cap 0.55'),
        ('nbasniper',          0.50, True,  'nbasniper 0.50 < cap 0.55'),
    ]
    for alias, entry, expected_pass, desc in cases:
        cap = pt.WHALE_MAX_ENTRY.get(alias)
        actual_pass = cap is None or entry <= cap
        assert actual_pass == expected_pass, \
            f'max_entry case [{desc}] expected pass={expected_pass}, got {actual_pass}'
    print(f'[OK] T16: WHALE_MAX_ENTRY caps correctly enforced for all whales')

    # Test 17: each whale now has a fingerprint-tuned filter (Apr 20 rewire)
    # ── bigsix: dogs-or-spreads (unchanged) ────────────────────────────
    f = pt.WHALE_FILTERS['bigsix']
    assert     f({'title': 'Team A vs. B', 'entry_price': 0.40})  # dog h2h
    assert not f({'title': 'Team A vs. B', 'entry_price': 0.60})  # fav h2h
    assert     f({'title': 'Spread: A (-5.5)', 'entry_price': 0.70})  # spread
    assert not f({'title': 'A vs. B: O/U 215.5', 'entry_price': 0.40})  # totals
    # ── GIAYN: h2h-ml only ─────────────────────────────────────────────
    f = pt.WHALE_FILTERS['GamblingIsAllYouNeed']
    assert     f({'title': 'Team A vs. B', 'entry_price': 0.60})  # h2h fav OK
    assert     f({'title': 'Team A vs. B', 'entry_price': 0.40})  # h2h dog OK
    assert not f({'title': 'Spread: A (-5.5)', 'entry_price': 0.40})  # spread blocked
    assert not f({'title': 'A vs. B: O/U 215.5', 'entry_price': 0.40})  # totals blocked
    # ── kch123: barbell (dogs <0.50 or heavy favs >=0.75) ─────────────
    f = pt.WHALE_FILTERS['kch123']
    assert     f({'title': 'A vs. B', 'entry_price': 0.30})   # dog
    assert     f({'title': 'A vs. B', 'entry_price': 0.49})   # dog edge
    assert not f({'title': 'A vs. B', 'entry_price': 0.55})   # mid-fav DEAD ZONE
    assert not f({'title': 'A vs. B', 'entry_price': 0.70})   # mid-fav top
    assert     f({'title': 'A vs. B', 'entry_price': 0.75})   # heavy fav entry
    assert     f({'title': 'A vs. B', 'entry_price': 0.85})   # heavy fav
    # ── nbasniper: all subtypes except daily-ml ────────────────────────
    f = pt.WHALE_FILTERS['nbasniper']
    assert     f({'title': 'A vs. B', 'entry_price': 0.40})  # h2h OK
    assert     f({'title': 'Spread: A (-5.5)', 'entry_price': 0.60})  # spread OK
    assert     f({'title': 'A vs. B: O/U 215.5', 'entry_price': 0.45})  # totals OK
    assert not f({'title': 'Will A win on 2026-04-20?', 'entry_price': 0.40})  # daily-ml blocked
    # ── sportmaster: not in WHALE_FILTERS ──────────────────────────────
    assert 'sportmaster777' not in pt.WHALE_FILTERS
    print(f'[OK] T17: per-whale fingerprint-tuned filters '
          f'(bigsix=dogs+spreads, GIAYN=h2h-ml only, '
          f'kch123=barbell, nbasniper=non-daily, sportmaster=unrestricted)')

    # Test 18: MIN_ENTRY_PRICE floor blocks desperation longshots
    # at any price below $0.10 regardless of whale.
    assert pt.MIN_ENTRY_PRICE == 0.10, \
        f'MIN_ENTRY_PRICE expected 0.10, got {pt.MIN_ENTRY_PRICE}'
    # Time-to-event should be disabled (Apr 20)
    assert pt.MIN_MINUTES_TO_GAME_START == 0, \
        f'MIN_MINUTES_TO_GAME_START expected 0 (disabled), got {pt.MIN_MINUTES_TO_GAME_START}'
    print(f'[OK] T18: MIN_ENTRY_PRICE=${pt.MIN_ENTRY_PRICE:.2f} floor active; '
          f'time-to-event filter disabled ({pt.MIN_MINUTES_TO_GAME_START}min)')

    conn.close()
    print('\n[ALL 19 TESTS PASSED]')

asyncio.run(run_tests())

# Cleanup
os.unlink(tmpfile.name)
