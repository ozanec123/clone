#!/usr/bin/env python3
"""
NEXUS INERTIA TRADER v4.8.2 — REV v3, every lane LIVE, EXPLOSION (v4.6.4
machine), flexible cooldowns, flat sessions, SGRIND (continuation, v4.6.4
behavior) + SGREV (reversal) running side by side, continuous k15 rotation.

Shadow ledger only. No exchange orders are submitted. Pre-registered: no
changes until lane gates or 21 days. Fresh ledger; prior ledgers preserved.

v4.8.2 CHANGES (user-directed):
- BOTH SGRIND flavors run in parallel for A/B: SGRIND is RESTORED to the
  v4.7.2/v4.6.4 continuation behavior (4-6 candles >=2%, 1 opposite candle,
  price extending it -> enter WITH the grind, forced SWING), and the v4.8.1
  redesign lives on as SGREV (>=4 candles any length, >=4% open-to-extreme,
  live break of the opposite candle's extreme -> enter AGAINST the grind).
  No head-on conflict: on any single setup the two are mutually exclusive —
  the continuation lane fires only when the break happens BEFORE cache
  discovery, the reversal lane only when it happens AFTER (its stale-break
  guard skips earlier breaks). The global one-position-per-symbol rule still
  applies as everywhere. SGREV has its own name, emoji, verdict counter and
  per-(coin,lane) flexible cooldown.
- k15 cache: CONTINUOUS oldest-first rotation instead of the 300s cycle
  (which dates back to v4.6.4 — kept until now for behavior preservation).
  Newly closed 15m candles are discovered within one sweep (~30-45s) instead
  of up to ~6 min; 1h candles (SQUEEZE only) rotate on a slower cadence.

v4.8.1 CHANGES (user-directed):
- SGRIND REDESIGNED from continuation to REVERSAL (the v4.8.0 'enter WITH the
  grind' behavior traded 0GUSDT against the user's intent). New spec: a grind
  of >=4 same-direction 15m candles (ANY length — 6, 10, 20 all qualify) with
  a >=4% move from the FIRST streak candle's OPEN to the streak's most
  extreme point; then ONE closed opposite candle; then during the NEXT candle
  a LIVE break of that opposite candle's extreme enters AGAINST the original
  grind at market price. Watch window = the candle after the opposite one; it
  dies on window close, on price crossing back past the opposite candle's
  other extreme (grind resuming), and never chases a break that already
  happened before discovery (k15 latency). Fresh verdict counter via version.

v4.8.0 CHANGES (all user-directed):
- REV v3: dump trigger 3.0→2.5%/60s on 1s closes, bounce 0.5→0.75%. Systemic
  filter KEPT; vertical guard REMOVED.
- SGRIND: pinned to v4.6.4 behavior end-to-end (forced SWING tier; its 2h
  cooldown replaced by the flexible money-based cooldown below).
- ALL lanes live: DEEPDIP v2 / FAILBREAK / SQUEEZE / FOLLOWER / BURST trade
  REAL (paper machinery retained for tests only). EXPLOSION restored to the
  v4.6.4 q24 minute-bucket machine with knife-guard fills — LIVE, full size;
  the aggTrade detection machine is deleted again (the aggTrade feed remains
  as the 1s recorder evidence layer).
- Sessions flat: SESSION_SIZES all 1.0.
- Direction balance REMOVED (no up/down caps at all).
- Vertical (10%/15m) veto REMOVED.
- Flexible cooldown (max 1h) replaces SGRIND 2h / PFADE 4h / FAILBREAK 2h /
  DEEPDIP v2 2h: after a close on (coin,lane): win → 5 min; loss → scaled by
  money lost, 10–60 min (full ~$45 stop = 60 min). REV/FSQZ/DIP-X stay
  cooldown-free. Episode scoping (one per crash/squeeze/burst) unchanged.

v4.7.2 REPAIR SET (bug fixes over v4.7.0/4.7.1; no strategy semantics changed):
- run() worker lifetime: signal_consumer self-returns outside the trader role,
  and asyncio.wait(FIRST_COMPLETED) treated ANY completed worker as shutdown —
  the process exited ~1s after startup and the supervisor restart-looped it
  forever (the Render loop). signal_consumer now exists only in the trader
  role; the Telegram "armed" message was being cancelled mid-send each cycle.
- open_position INSERT arity (17 values/15 cols/14 params) — every open raised
  OperationalError; the v4.7.0 build could never open a position.
- ux_open_symbol partial index now scoped to paper=0 — paper+real rows may
  share a symbol (as designed and as the smoke test demands); DROP+recreate
  self-heals ledgers built with the v4.7.0 index.
- _numeric_tests: int() pace windows + feed-live stamp — the selftest crashed
  uncaught and the pace regression never actually ran.
- Regime tag: dedicated BTC 1s series (px_windows retains 920s < the 30m
  lookback, so the tag was ALWAYS '+0.0%/calm'); stamped at ENTRY into
  shadow_positions.regime (additive column), not re-derived at close.
- Split mode: signals reader row_factory (consumer crash-looped on tuples);
  ticker feed + seeding shared to ALL roles (a trader was price-blind:
  stream_price None, _btc_hot pinned, _is_vertical fail-open); funding_ts_
  worker moved to the scanner (it reads scanner-side state); per-role
  heartbeat keys (one shared key let same-role duplicates coexist unseen).
- FAILBREAK trigger compared the close to a 12h high that INCLUDED the
  breakout candle's own high — mathematically impossible; now prior-12h high
  (window excludes the breakout bar).
- FOLLOWER: TP encoded at open (flw|<px>; FOLLOW_CAPTURE was dead), TP
  comparison sign fixed (a short would have closed instantly), own-volume
  guard implemented (FOLLOW_VOL_MAX_X was dead).
- PFADE re-long defers while the symbol is busy (its own short leg held the
  symbol; confirmed re-longs were dropped by symbol_busy).
- OI_FLUSH post-phase reachable (the disjoint() condition always failed);
  SQUEEZE volume median now 15m-vs-15m (was a 15m bar vs a 1h median),
  percentile windows exclude the live window, dead `< 0` cooldown removed.
- ENABLE_EXPLOSION / ENABLE_FUNDING_SQZ now honored (were decorative).
- Recorder backlog cap; report() ensures schema; waitroom promotion keeps the
  original payload/episode_id.

═══════════════════════════════════════════════════════════════════
ROSTER (12 live · 1 instrument · 2 log-only · 1 dead):
⚡ REV v3       LIVE, full: dump ≥2.5%/60s on 1s closes (live stream) → low
               tracked on BID (episode bookTicker) → 10s three-outcome watch →
               bid bounce ≥0.75% → LONG. Systemic filter kept (BTC ≤-0.40%/60s
               skips market-wide dumps); vertical guard REMOVED in v4.8.
               Gate n=40.
🧲 FUNDING_SQZ  LIVE, half: PINNED to the v4.6.4 skeleton (dump ≥2%/60s candle
               ticks + bounce ≥1% + funding ≤-0.30%). High-vol gate EXEMPT.
               2-week fill deadline, then redesign from arm-event logs.
🕳🕳 DEEPDIP_X   LIVE, half, unchanged: ≥18% within 45min (5m velocity) →
               stabilize 10m → bounce ≥2%. Scale decision at n=20.
🐌 SGRIND       LIVE, continuation (v4.6.4/v4.7.2 behavior restored): 4-6x
               15m candles ≥2% → 1 opposite closed → price extending it →
               enter WITH the grind. SWING, full. Flexible cd (≤1h). n=50.
🔄 SGREV       LIVE, reversal (the v4.8.1 redesign, own lane for A/B):
               ≥4x 15m candles (any length) with ≥4% open→extreme → 1 closed
               opposite candle → live break of its extreme next candle →
               enter AGAINST the grind. SWING, full. Flexible cd. Own n=20.
🚀 PFADE        LIVE, structure unchanged; re-long leg OI/funding-CONFIRMED:
               ≥20% over 24h low → 15m close < prior low → SHORT (half);
               2x 15m no-new-low AND (OI Δ≤-3% since trigger OR funding ≤-0.10%)
               → LONG. Per-leg verdict at n=20. Flexible cooldown (≤1h).
🕳 DEEPDIP v2    LIVE, full: ≥20% below rolling 24h high, latched at the low (expires
               24h) → 2 consecutive positive 15m candles (close>open) → LONG at
               2nd close. X owns velocity episodes (one crash, one lane). Stop =
               dump low -0.3%, skip if >4% from entry. Flexible cd, one per episode.
🪤 FAILBREAK    LIVE, full: 15m close ≥0.3% above 12h high with breakout volume <2x
               50-candle median → close back below within 2 candles → SHORT.
               Stop break-high +0.15%. Ladder exits. Flexible cd, one per episode.
               Mirror long coded, DISABLED until co-fire audit <30% REV overlap.
🧨 SQUEEZE_BRK  LIVE, full: 12x1h range width ≤20th pct of trailing 7d AND ≤1.5×
               ATR(1h) → 15m close outside w/ volume ≥2x 15m median → enter.
               SWING. One trade per squeeze episode.
🐦 FOLLOWER     LIVE, full: |BTC| ≥1.2%/5min → biggest liquid laggard
               (≤0.25x BTC move, own volume ≤3x median, spread ok) → 60s
               momentum ≥0.15% in BTC's direction → enter WITH BTC. 12-min time
               stop, 0.6x-capture target, 0.6% adverse stop. Systemic NOT applied.
🔆 BURST        LIVE, full: candle-1 15m body ≥±5% (close vs open) → candle-2
               opens same-direction (gap ≤2% beyond candle-1 close) and is
               currently the same color → LIVE tick breaking candle-1's high
               (LONG) / low (SHORT) enters WITH the move. Intrabar trigger —
               no candle-2 close wait. One per burst; flexible cd.
💥 EXPLOSION    RESTORED to the v4.6.4 q24 machine (user-directed): projected
               1-minute q24-delta volume ≥20x REST median + ≥1% move, fires
               seconds 3-10 of the minute, 0.7% pullback limit, knife-guard
               fills, TTL 120s. LIVE, full size. (The aggTrade detection
               machine is deleted; the aggTrade feed stays as the 1s recorder
               evidence layer.)
🫗 OI_FLUSH     INSTRUMENT: on every REV v2 dump episode, /fapi/v1/openInterest
               polled every 10s from trigger to +120s; OI Δ% + bounce outcome
               logged. Arms later only if OI-down dumps separate.
⏰ FUNDING_TS   LOG-ONLY: funding rate + nextFundingTime logged at every FS arm.
🌅 DAYOPEN_RCLM LOG-ONLY: first 15m close back above the UTC 00:00 open after
               trading below it.
🐋 WHALE_REV    DEAD — subsystem deleted. Not returning.

PROCESS ROLES (NEXUS_ROLE env): both (default) | scanner | trader.
  scanner: feeds, detectors, episodes, 1s recorder → emits SIGNALS to the ledger.
  trader:  consumes signals, re-verifies ALL DB-side admission (capacity, caps,
           dip budget, duplicate, chase, kill), owns opens/exits/NOW-measure/report.
  both:    everything, in-process (today's behavior, byte-for-byte semantics).
  Takeover is ROLE-AWARE: scanner+trader coexist on one ledger (same filesystem);
  same-role or both-vs-anything yields as before. Split mode is same-host only —
  the two functions that would need replacing for a network transport are
  _emit_signal() (Part 3) and signal_consumer() (Part 4); nothing else changes.
  TELEGRAM IDENTITY (v4.7.3): each role can speak as its own bot. Env precedence
  TG_TOKEN_<ROLE>/TG_CHAT_<ROLE> (SCANNER|TRADER|BOTH) → TRADER_TG_* →
  TELEGRAM_*. Unset role vars fall back to the shared bot. Trader bot carries
  OPEN/CLOSE/2h-report traffic; the scanner bot is startup-silent otherwise
  (detections go to the decision log).

SYSTEM RULES (v4.8):
- Dip budget: max 2 concurrent dip-class positions (REV/DEEPDIP_X/DEEPDIP_V2).
- Co-fire audit: ±10min same-coin tags on every signal (audit data only).
- Regime tags: BTC 30m move + vol bucket stamped at ENTRY.
- Direction balance: REMOVED (v4.8) — the book may be all-up, all-down, mixed.
- Vertical guard: REMOVED (v4.8).
- Sessions: flat 1.0x (v4.8).
- Flexible cooldown (max 1h): win → 5 min · loss → 60min × loss/$45, clamped
  10–60 min — on SGRIND / PFADE / FAILBREAK / DEEPDIP v2. REV/FSQZ/DIP-X
  cooldown-free. Episode scoping (one per crash/squeeze/burst) unchanged.
- Verdict rule: n=20 → decision; n=50 positive → full. REV override: n=40.
- No benching, no per-symbol stops, no daily breaker — DELIBERATE.
- 1s recorder: top-150 by q24 + episode symbols; SEPARATE DB, batched writes,
  7-day retention. Cannot be backfilled — the autopsy layer.

EXITS (validated in v4.6.4, untouched): stop -2.2% HARD all lanes · ladder arms
+0.8% (floor peak-0.5%; peak-1.0% ≥3.0%) · early cut -1.5% if ladder never
armed · SCALP 30m-if-winning/60m · SWING 60m · sizing $45/(2.2%+slip) ·
flat sessions (1.0x, v4.8) · cap $1500 · half size: FSQZ + DIP-X only — every
other lane trades FULL since v4.8 · paper rows (tests only) never consume
capacity.

CARRIED: behavioral selftest (numeric REV-v2/DEEPDIP-v2/ladder/cut/systemic
cases, migration smoke, wiring + REMOVAL assertions) · takeover (role-aware) ·
idempotent close · atomic capacity · task-death shutdown · WS failover ·
decision log (version-stamped) · killfile · NOW-measure (horizon_1m_v1) · 2h TG
scoreboard · report --all · sl_pct on every CLOSE · conservative-fill env.
"""

import asyncio, json, math, os, signal, sqlite3, statistics, sys, tempfile, time, traceback
from collections import deque, defaultdict
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote
import aiohttp, websockets

_shutdown = asyncio.Event()
SESSION_M = None          # global aiohttp session (set in main)
_SDB_PATH = None          # test override; None = production ledger

# ================================================================ CONFIG
SHADOW_DB_PATH = os.getenv("SHADOW_DB_PATH", "/data/shadow_v4_7.db")
MARKET_DB_PATH = os.getenv("NEXUS_MARKET_DB", "/data/market_1s_v4_7.db")
ALERT_DB = os.getenv("PERFORMANCE_DB_PATH", "/data/alerts_performance.db")
FAPI = "https://fapi.binance.com"
WS_BASES = ["wss://fstream.binance.com/market", "wss://fstream.binance.com"]
WS_PATH = "/ws/!miniTicker@arr"
# ---- process role: scanner detects · trader trades · both = single process ----
NEXUS_ROLE = os.getenv("NEXUS_ROLE", "both").strip().lower()
IS_SCANNER = NEXUS_ROLE in ("scanner", "both")
IS_TRADER  = NEXUS_ROLE in ("trader", "both")

# ---- Telegram: per-role bot identity ----
# Precedence: TG_TOKEN_<ROLE>/TG_CHAT_<ROLE> → TRADER_TG_* → TELEGRAM_*.
# Role vars unset → the shared bot (existing deployments unchanged). Set
# TG_TOKEN_SCANNER + TG_TOKEN_TRADER (+ TG_CHAT_*) and, in split mode, the
# scanner and trader speak as two DIFFERENT Telegram bots.
_ROLE_U = NEXUS_ROLE.upper()
TG_TOKEN = os.getenv(f"TG_TOKEN_{_ROLE_U}",
                     os.getenv("TRADER_TG_TOKEN", os.getenv("TELEGRAM_BOT_TOKEN", "")))
TG_CHAT = os.getenv(f"TG_CHAT_{_ROLE_U}",
                    os.getenv("TRADER_TG_CHAT", os.getenv("TELEGRAM_CHAT_ID", "")))
TG_ENABLED = bool(TG_TOKEN and TG_CHAT)

SIGNAL_TTL_SEC = 90.0          # a signal older than this is dead on arrival
SIGNAL_CLAIM_TTL = 45.0        # claimed-but-unfinished signals reclaimable after this
SIGNAL_POLL_SEC = 1.0

RISK_DOLLARS = 45.0
NOTIONAL_MAX = 1500.0
MAX_CONCURRENT = 10

# ---- exits (validated in v4.6.4, untouched) ----
SL_PCT = 2.2                                       # HARD, all lanes
LADDER_ARM_PCT, LADDER_GAP = 0.8, 0.5
LADDER_GAP_AT, LADDER_GAP_BIG = 3.0, 1.0
EARLY_CUT_PCT = 1.5
SCALP_HOLD_MIN, SWING_HOLD_MIN = 30.0, 60.0

# ---- REV v2 (tick design, locked) ----
REV_DUMP_PCT, REV_DUMP_WIN = 2.5, 60.0             # v4.8: 2.5%/60s on 1s closes
REV_BOUNCE_PCT = 0.75                              # v4.8: 0.75% bounce, ALL coins
REV_WATCH_SEC = 10.0                               # three-outcome watch after the low
REV_STALE_SEC = 120.0                              # no bounce this long after the low → dead
REV_CUTOVER_N = 40                                 # half size until n=40 (lane override)
REV_EPISODE_TAIL = 120.0                           # OI + recorder tail after resolution

# ---- FUNDING_SQZ (PINNED to v4.6.4 skeleton — REV v2 does not leak here) ----
FS_DUMP_PCT, FS_DUMP_WIN = 2.0, 60.0
FS_BOUNCE_PCT = 1.0
FUNDING_EXTREME = -0.0030                          # -0.30%
FUNDING_CHECK_TTL = 60.0
FS_FILL_DEADLINE_DAYS = 14

# ---- DEEPDIP_X (unchanged) ----
DIPX_DROP_PCT, DIPX_WINDOW_SEC = 18.0, 2700.0
DIPX_STABILIZE_SEC, DIPX_BOUNCE_PCT = 600.0, 2.0
DIPX_K5_LIMIT = 10                                 # 9 closed 5m bars = 45 min

# ---- DEEPDIP v2 (redesigned, paper) ----
DD2_DROP_PCT = 20.0                                # below rolling 24h high
DD2_LATCH_HRS = 24.0
DD2_CANDLES = 2                                    # consecutive positive 15m candles
DD2_STOP_BUF = 0.3
DD2_MAX_STOP_DIST = 4.0
DD2_CUTOVER_N = 20

# ---- SGRIND continuation (v4.6.4/v4.7.2 behavior restored) ----
SG_MIN_CANDLES, SG_MAX_CANDLES = 4, 6   # streak 4-6 candles (7+ rejected — v4.6.4 nuance)
SG_MIN_STREAK_PCT = 2.0                 # close-before-streak → last streak close ≥±2%
# ---- SGREV reversal (the v4.8.1 redesign, own lane for the A/B) ----
SGREV_MIN_CANDLES = 4           # streak: 4 or more same-direction candles (no upper limit)
SGREV_MIN_STREAK_PCT = 4.0      # first streak candle's OPEN → streak's most extreme point ≥±4%

# ---- PFADE (structure unchanged; re-long confirmation added) ----
PF_MIN_RUN_PCT = 20.0
PF_STAB_CANDLES = 2
PF_RELONG_OI_DROP = 3.0                            # OI Δ% since short trigger ≤ -3%
PF_RELONG_FUNDING = -0.10                          # OR funding ≤ -0.10%

# ---- EXPLOSION (v4.6.4 q24 machine, verbatim constants) ----
TRADE_SUB_LIMIT = 1024                             # aggTrade recorder feed cap
EXPLOSION_VOL_X = 20.0                             # projected minute pace ≥20x median
EXPLOSION_MIN_MOVE_PCT = 1.0
EXPLOSION_EARLY_SEC, EXPLOSION_EARLY_MIN_SEC = 10.0, 3.0
EXPLOSION_PULLBACK_PCT, EXPLOSION_PB_TTL_SEC = 0.7, 120
VOL_MEDIAN_REFRESH_SEC, VOL_MEDIAN_SYMS = 600.0, 300

# ---- systemic filter (recalibrated 0.75 → 0.40) ----
SYS_BTC_DROP_PCT = 0.40
MOVE_PCT, WIN_SEC = 2.0, 300
CHASE_MAX_PCT = 1.5
VETO_EXH_MAX = 85
ALIGN_CAL_BOOST = 0.60

# ---- 15m/1h kline cache (k15: DDv2/X, SGRIND, SGREV, PFADE, FAILBREAK, SQUEEZE, BURST) ----
K15_SYMS, K15_LIMIT = 150, 96
K15_PACE_SEC = 0.12            # continuous rotation pacing (v4.8.2: no 300s cycle)
K1H_LIMIT = 168                                    # 7d of 1h bars for SQUEEZE
K1H_REFRESH_SEC = 600.0        # 1h candles feed only SQUEEZE — slower cadence is fine
FAILBREAK_LOOKBACK_H = 12
SQUEEZE_LOOK_CANDLES, SQUEEZE_PCTILE = 12, 20
SQUEEZE_ATR_MULT = 1.5

# ---- FOLLOWER ----
FOLLOW_BTC_MOVE_PCT = 1.2
FOLLOW_BTC_WIN = 300.0
FOLLOW_LAG_MAX = 0.25
FOLLOW_VOL_MAX_X = 3.0
FOLLOW_CONFIRM_PCT = 0.15
FOLLOW_CAPTURE = 0.6
FOLLOW_TIME_STOP_MIN = 12.0
FOLLOW_ADVERSE_PCT = 0.6
FOLLOW_PRELIST_N = 120

# ---- BURST (PAPER — momentum continuation, intrabar break) ----
BURST_BODY_PCT = 5.0            # candle-1 body (close vs open) ≥±5%
BURST_GAP_MAX_PCT = 2.0         # candle-2 open may gap ≤2% beyond candle-1 close (with the move)
BURST_COOLDOWN_SEC = 7200.0     # 2h per symbol; one trade per burst

# ---- OI_FLUSH instrument ----
OI_POLL_SEC = 10.0
OI_WINDOW_PRE = 10.0
OI_WINDOW_POST = 120.0

# ---- 1s recorder ----
RECORDER_TOP_N = 150
RECORDER_FLUSH_SEC = 5.0
RECORDER_RETAIN_DAYS = 7
REC_BUF_CAP = 100000            # flush-buffer ceiling (backlog protection)

# ---- system ----
BTC_HOT_MOVE_PCT = 1.0                            # tick-line info only (caps removed v4.8)
DIP_BUDGET = 2
DIP_LANES = ("REV", "DEEPDIP_X", "DEEPDIP_V2")
CO_FIRE_WINDOW = 600.0
# v4.8 flexible cooldown (replaces SGRIND 2h / PFADE 4h / FAILBREAK 2h / DDv2 2h):
# after a close on (coin,lane): win → 5 min · loss → scaled by money lost,
# 10..60 min (a full ~$45 stop = 60 min). REV/FSQZ/DIP-X stay cooldown-free.
LANE_COOLDOWN_WIN_SEC = 300.0
LANE_COOLDOWN_MIN_SEC = 600.0
LANE_COOLDOWN_MAX_SEC = 3600.0
FLEX_CD_LANES = ("SGRIND", "SGREV", "PFADE", "FAILBREAK", "DEEPDIP_V2")
SESSION_SIZES = {h: 1.00 for h in range(24)}   # v4.8: flat — sessions no longer sized
POLL_SEC, WAITROOM_SEC = 3, 120
TAKER_FEE_PCT_SIDE, MAKER_FEE_PCT_SIDE = 0.05, 0.02
SLIP_SPREAD_MULT, SLIP_MIN_PCT, SLIP_MAX_PCT = 3.0, 0.05, 0.30
KILL = os.getenv("NEXUS_SHADOW_KILL", "0") == "1"
KILLFILE = os.getenv("NEXUS_KILLFILE", "/data/nexus_v3_kill")
CONSERVATIVE_FILLS = os.getenv("NEXUS_CONSERVATIVE", "0") == "1"
def kill_now():
    if KILL: return True
    try: return os.path.exists(KILLFILE)
    except Exception: return False
TRIGGER_THROTTLE_S, RANK_REST_CAP = 10, 6
SEED_KLINES, SEED_PACE_SEC, SEED_FAIL_COOLDOWN = 16, 0.1, 60.0
PX_RETAIN_SEC = 920.0
KLINES_LIMIT = 70
HEARTBEAT_KEY, TAKEOVER_KEY, HEARTBEAT_TTL = "instance_heartbeat", "takeover_request", 30.0
TAKEOVER_REQ_TTL, TAKEOVER_PATIENCE_SEC = 60.0, 45.0
W_PRIO, W_HIST, W_REL, W_RAW, W_OI, W_BOOK, W_FRESH, W_ALIGN = 0.20, 0.15, 0.15, 0.10, 0.15, 0.10, 0.10, 0.15

# lane enable flags — LIVE
ENABLE_REV, ENABLE_FUNDING_SQZ, ENABLE_DEEPDIP_X = True, True, True
ENABLE_SGRIND, ENABLE_PFADE = True, True
# v4.8: every lane trades LIVE (paper machinery retained for tests only)
ENABLE_DEEPDIP_V2 = True
ENABLE_FAILBREAK = True
ENABLE_SQUEEZE = True
ENABLE_FOLLOWER = True
ENABLE_BURST = True             # 5% body → intrabar break
ENABLE_EXPLOSION = True         # v4.6.4 q24 machine, full size
# LOG-ONLY
ENABLE_FUNDING_TS = True
ENABLE_DAYOPEN = True

LANE_PRIO = {
    "FUNDING_SQZ": 0.95, "PFADE": 0.90, "REV": 0.85, "EXPLOSION": 0.85,
    "FOLLOWER": 0.85, "SGRIND": 0.80, "FAILBREAK": 0.80, "SQUEEZE": 0.80,
    "BURST": 0.80, "SGREV": 0.80, "DEEPDIP_X": 0.75, "DEEPDIP_V2": 0.70,
}
STABLE_SYMBOLS = {
    "USDCUSDT","FDUSDUSDT","TUSDUSDT","USDPUSDT","DAIUSDT","EURUSDT","AEURUSDT",
    "EURIUSDT","USD1USDT","BUSDUSDT","USTUSDT","USDEUSDT","PYUSDUSDT","USDSUSDT",
    "RLUSDUSDT","FRAXUSDT","USDDUSDT","GUSDUSDT","PAXGUSDT","XUSDUSDT","BFUSDUSDT",
}
NOW_MEASURE_MIN_AGE_SEC = 300.0
NOW_MEASURE_MAX_AGE_SEC = 7200.0
NOW_MEASURE_METHOD = "horizon_1m_v1"
RULES_VER = "4.8.2"

def now_ts(): return time.time()
def hms(): return datetime.now().strftime("%H:%M:%S")
def _host():
    try: return os.uname().nodename
    except Exception: return "unknown"
def make_trade_link(symbol):
    import base64
    sym_q = quote(symbol.upper(), safe="")
    dp = base64.b64encode(f"bnc://app.binance.com/trade/trade?at=futures&symbol={sym_q}".encode()).decode()
    return f"https://app.binance.com/en/futures/{sym_q}?%5Fdp={dp}"

# ================================================================ DB (trading ledger)
def sdb():
    path = _SDB_PATH or SHADOW_DB_PATH
    if _SDB_PATH is None:
        Path(SHADOW_DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(path, timeout=30)
    c.execute("PRAGMA busy_timeout=15000")
    try: c.execute("PRAGMA journal_mode=WAL")
    except Exception: pass
    return c

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS shadow_positions(
    id INTEGER PRIMARY KEY AUTOINCREMENT, lane TEXT, tier TEXT, symbol TEXT,
    direction TEXT, entry_ts REAL, entry_price REAL, notional REAL, sl_price REAL,
    sl_pct REAL, spread_pct REAL, opened_day TEXT, status TEXT DEFAULT 'OPEN',
    t30_done INTEGER DEFAULT 0, dead_checked INTEGER DEFAULT 0,
    mfe_pct REAL DEFAULT 0, mae_pct REAL DEFAULT 0, peak_ts REAL,
    paper INTEGER DEFAULT 0, entry_ver TEXT, episode_id TEXT, regime TEXT);
CREATE TABLE IF NOT EXISTS shadow_trades(
    id INTEGER PRIMARY KEY AUTOINCREMENT, lane TEXT, tier TEXT, symbol TEXT, direction TEXT,
    entry_ts REAL, entry_price REAL, exit_ts REAL, exit_price REAL, side TEXT,
    move_pct REAL, gross_pnl REAL, costs REAL, net_pnl REAL, hold_min REAL, day TEXT,
    hour_utc INTEGER, rules_ver TEXT, mfe_pct REAL, mae_pct REAL, maker_exit INTEGER,
    slip_vs_level REAL, notional REAL, sl_pct REAL, spread_pct REAL,
    paper INTEGER DEFAULT 0, entry_ver TEXT, regime TEXT);
CREATE TABLE IF NOT EXISTS shadow_daily(day TEXT PRIMARY KEY, realized REAL DEFAULT 0, trades INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS decisions(key TEXT PRIMARY KEY, ts REAL, symbol TEXT, kind TEXT, reason TEXT,
    data TEXT, rules_ver TEXT);
CREATE TABLE IF NOT EXISTS now_measurements(
    alert_id INTEGER, symbol TEXT, dir TEXT, cal REAL, call_ts REAL, call_px REAL,
    px_5m REAL, final_mv REAL, age_s REAL, sample_ts REAL, measured_ts REAL,
    method TEXT DEFAULT 'legacy_live', PRIMARY KEY(alert_id));
CREATE TABLE IF NOT EXISTS shadow_meta(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS oi_log(
    id INTEGER PRIMARY KEY AUTOINCREMENT, episode_id TEXT, symbol TEXT, ts REAL,
    oi REAL, oi_delta_pct REAL, phase TEXT);
CREATE TABLE IF NOT EXISTS signals(
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, lane TEXT, symbol TEXT,
    direction TEXT, sig_px REAL, payload TEXT, state TEXT DEFAULT 'new',
    claimed_by TEXT, claimed_ts REAL, rules_ver TEXT);
"""

# ---- market recorder: SEPARATE DB so 1s writes never stall trading writes ----
def mdb():
    Path(MARKET_DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(MARKET_DB_PATH, timeout=30)
    c.execute("PRAGMA busy_timeout=15000")
    try:
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
    except Exception: pass
    return c

MARKET_SCHEMA = """
CREATE TABLE IF NOT EXISTS bars_1s(
    sym TEXT, ts INTEGER, o REAL, h REAL, l REAL, c REAL, q REAL,
    PRIMARY KEY(sym, ts));
CREATE TABLE IF NOT EXISTS bids_1s(
    sym TEXT, ts INTEGER, bid REAL, PRIMARY KEY(sym, ts));
"""

def _ensure_columns(c, table, cols):
    have = {r[1] for r in c.execute(f"PRAGMA table_info({table})").fetchall()}
    for name, ddl in cols.items():
        if name not in have:
            c.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")
            print(f"[db   ] migrated: {table}.{name} added")

def _validate_ledger_schema(c):
    """Validate the CONFIGURED ledger. Additive migrations cover historical
    fields; missing core economic columns cannot be fabricated — stop."""
    problems = []
    with closing(sqlite3.connect(":memory:")) as expected:
        expected.executescript(SCHEMA_SQL)
        tables = expected.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall()
        for (table,) in tables:
            required = {r[1] for r in expected.execute(f'PRAGMA table_info("{table}")')}
            actual = {r[1] for r in c.execute(f'PRAGMA table_info("{table}")').fetchall()}
            missing = sorted(required - actual)
            if missing:
                problems.append(f"{table}: missing required columns {', '.join(missing)}")
    if problems:
        raise RuntimeError("Ledger schema incompatible: " + "; ".join(problems))

def _ensure_schema(c):
    c.executescript(SCHEMA_SQL)
    _ensure_columns(c, "shadow_positions", {
        "lane": "TEXT", "tier": "TEXT", "sl_pct": "REAL", "spread_pct": "REAL",
        "opened_day": "TEXT", "t30_done": "INTEGER DEFAULT 0",
        "dead_checked": "INTEGER DEFAULT 0", "mfe_pct": "REAL DEFAULT 0",
        "mae_pct": "REAL DEFAULT 0", "peak_ts": "REAL",
        "paper": "INTEGER DEFAULT 0", "entry_ver": "TEXT", "episode_id": "TEXT",
        "regime": "TEXT"})
    _ensure_columns(c, "shadow_trades", {
        "lane": "TEXT", "tier": "TEXT", "hour_utc": "INTEGER", "rules_ver": "TEXT",
        "mfe_pct": "REAL", "mae_pct": "REAL", "maker_exit": "INTEGER",
        "slip_vs_level": "REAL", "notional": "REAL", "sl_pct": "REAL", "spread_pct": "REAL",
        "paper": "INTEGER DEFAULT 0", "entry_ver": "TEXT", "regime": "TEXT"})
    _ensure_columns(c, "decisions", {"rules_ver": "TEXT"})
    _ensure_columns(c, "now_measurements", {"age_s": "REAL", "sample_ts": "REAL",
        "measured_ts": "REAL", "method": "TEXT DEFAULT 'legacy_live'"})
    _validate_ledger_schema(c)
    c.execute("""UPDATE shadow_positions
        SET sl_pct=ROUND(ABS(sl_price / entry_price - 1.0)*100.0, 10)
        WHERE sl_pct IS NULL AND entry_price>0 AND sl_price>0 AND sl_price!=entry_price""")
    # partial-duplicate guard: REAL rows only. v4.7.0 indexed (symbol) alone,
    # which blocked paper+real coexistence on one symbol (the smoke test's
    # required semantics); DROP+recreate self-heals ledgers with the old index.
    c.execute("DROP INDEX IF EXISTS ux_open_symbol")
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_open_symbol ON shadow_positions(symbol) "
              "WHERE status='OPEN' AND paper=0")
    c.commit()

def mdb_init():
    with closing(mdb()) as c:
        c.executescript(MARKET_SCHEMA)
        c.commit()
    print(f"[db   ] market recorder: {MARKET_DB_PATH} (1s bars + bids, {RECORDER_RETAIN_DAYS}d retention)")

def sdb_init():
    path = _SDB_PATH or SHADOW_DB_PATH
    existed = Path(path).exists()
    with closing(sdb()) as c:
        _ensure_schema(c)
    print(f"[db   ] ledger: {path} "
          f"({'new file' if not existed else 'existing — schema ensured/migrated'})")
    print("[db   ] configured ledger schema verified")

def meta_get(k, d=None):
    c = sdb(); r = c.execute("SELECT value FROM shadow_meta WHERE key=?", (k,)).fetchone(); c.close()
    return r[0] if r else d
def meta_set(k, v):
    c = sdb(); c.execute("INSERT OR REPLACE INTO shadow_meta(key,value) VALUES(?,?)", (k, str(v))); c.commit(); c.close()
def meta_del(k):
    c = sdb(); c.execute("DELETE FROM shadow_meta WHERE key=?", (k,)); c.commit(); c.close()
def decide(kind, symbol, reason, data="", lane=None):
    """Decision log — version-stamped (funnel data was previously
    unattributable across build eras)."""
    try:
        c = sdb()
        c.execute("INSERT OR IGNORE INTO decisions(key,ts,symbol,kind,reason,data,rules_ver) "
                  "VALUES(?,?,?,?,?,?,?)",
                  (f"{kind}|{symbol}|{now_ts():.3f}", now_ts(), symbol, kind, reason,
                   json.dumps(data, default=str), RULES_VER))
        c.commit(); c.close()
    except Exception as e: print(f"[warn ] decision: {e}")
def today_utc(): return datetime.now(timezone.utc).strftime("%Y-%m-%d")
def daily_realized(day):
    c = sdb(); r = c.execute("SELECT COALESCE(SUM(net_pnl),0) FROM shadow_trades WHERE day=? AND paper=0", (day,)).fetchone(); c.close()
    return float(r[0])
def total_stats():
    c = sdb(); r = c.execute("SELECT COUNT(*), COALESCE(SUM(net_pnl),0) FROM shadow_trades WHERE paper=0").fetchone(); c.close()
    return int(r[0] or 0), float(r[1] or 0.0)
def open_positions(real_only=True):
    """real_only=True (default): REAL rows only — capacity, direction caps and
    the dip budget never see paper positions."""
    c = sdb(); c.row_factory = sqlite3.Row
    q = "SELECT * FROM shadow_positions WHERE status='OPEN'"
    if real_only: q += " AND paper=0"
    rows = c.execute(q).fetchall(); c.close()
    return [dict(r) for r in rows]
def all_open_positions():
    return open_positions(real_only=False)
def symbol_busy(sym):
    c = sdb(); n = c.execute("SELECT COUNT(*) FROM shadow_positions WHERE symbol=? AND status='OPEN' AND paper=0", (sym,)).fetchone()[0]; c.close()
    return n > 0
def symbol_history_exp(sym):
    try:
        c = sdb()
        r = c.execute("SELECT AVG(net_pnl) FROM (SELECT net_pnl FROM shadow_trades WHERE symbol=? AND paper=0 ORDER BY exit_ts DESC LIMIT 20)", (sym,)).fetchone()
        c.close(); return float(r[0]) if r and r[0] is not None else 0.0
    except Exception: return 0.0
def _dip_budget_ok():
    """Shared dip budget: max DIP_BUDGET concurrent REAL dip-class positions."""
    dips = sum(1 for p in open_positions() if p.get("lane") in DIP_LANES)
    return dips < DIP_BUDGET
def _regime_tag():
    """Regime stamp: BTC 30m move + realized-vol bucket, read from the
    dedicated BTC 1s series. v4.7.0 asked px_windows for a 1800s lookback it
    could never span (920s retention) — the tag was ALWAYS '+0.0%/calm'."""
    dq = SS.btc_px
    if not dq or now_ts() - dq[0][0] < 1800.0: return "na/warmup"
    p, px = dq[0][1], dq[-1][1]
    mv = (px / p - 1.0) * 100.0 if (p > 0 and px > 0) else 0.0
    vol = abs(mv)
    bucket = "calm" if vol < 1.0 else ("normal" if vol < 2.5 else "hot")
    return f"{mv:+.1f}%/{bucket}"

# ================================================================ LIFECYCLE (role-aware)
def _write_heartbeat(): meta_set(f"{HEARTBEAT_KEY}:{NEXUS_ROLE}", json.dumps(
    {"pid": os.getpid(), "host": _host(), "ts": now_ts(), "role": NEXUS_ROLE}))
def _read_hbs():
    """All recorded heartbeats. v4.7.1: PER-ROLE keys — scanner and trader
    coexisting stomped each other's single heartbeat every 3s, so a duplicate
    same-role instance could slip in unseen. The legacy single key is still
    read (pre-split builds) and treated as role 'both'."""
    out = []
    for k in (f"{HEARTBEAT_KEY}:scanner", f"{HEARTBEAT_KEY}:trader",
              f"{HEARTBEAT_KEY}:both", HEARTBEAT_KEY):
        v = meta_get(k)
        if not v: continue
        try:
            d = json.loads(v)
            out.append({"pid": int(d.get("pid") or 0), "host": str(d.get("host") or ""),
                        "ts": float(d.get("ts") or 0), "role": str(d.get("role") or "both")})
        except Exception: pass
    return out

def _roles_conflict(my_role, other_role):
    """Complementary roles coexist on one ledger (scanner+trader IS the split).
    Same roles, or any 'both' vs anything, conflict — single-writer preserved."""
    if my_role == other_role: return True
    if "both" in (my_role, other_role): return True
    return False

def _hb_is_self(hb): return hb["host"] == _host() and hb["pid"] == os.getpid()
def _hb_fresh(hb): return (now_ts() - hb["ts"]) < HEARTBEAT_TTL

def _companion_alive():
    """A fresh heartbeat from a NON-conflicting role (scanner+trader coexist)."""
    return next((hb for hb in _read_hbs()
                 if not _hb_is_self(hb) and _hb_fresh(hb)
                 and not _roles_conflict(NEXUS_ROLE, hb["role"])), None)

def _conflicting_fresh():
    """True ONLY if a fresh heartbeat from a CONFLICTING role exists. A scanner
    never yields to a trader heartbeat and vice versa; two same-role instances
    now always see each other (both write the same per-role key)."""
    return any(not _hb_is_self(hb) and _hb_fresh(hb)
               and _roles_conflict(NEXUS_ROLE, hb["role"]) for hb in _read_hbs())

def _clear_hb():
    try: meta_del(f"{HEARTBEAT_KEY}:{NEXUS_ROLE}")
    except Exception: pass

def takeover():
    comp = _companion_alive()
    if comp:
        print(f"[takeover] complementary {comp['role']} instance live "
              f"(pid {comp['pid']}@{comp['host']}) — coexisting on the shared ledger")
    if _conflicting_fresh():
        hb = next(hb for hb in _read_hbs()
                  if not _hb_is_self(hb) and _hb_fresh(hb)
                  and _roles_conflict(NEXUS_ROLE, hb["role"]))
        print(f"[takeover] live instance (pid {hb['pid']}@{hb['host']}, role {hb['role']}) — requesting handoff")
        meta_set(TAKEOVER_KEY, json.dumps({"req_pid": os.getpid(), "req_host": _host(), "ts": now_ts()}))
        deadline = now_ts() + TAKEOVER_PATIENCE_SEC
        while now_ts() < deadline:
            time.sleep(5)
            if not _conflicting_fresh(): break
        else:
            print("[takeover] previous did not yield — exiting"); sys.exit(0)
    else:
        if not comp: print("[takeover] no live instance — taking ledger")
    try: meta_del(TAKEOVER_KEY)
    except Exception: pass
    _write_heartbeat()
    print(f"[takeover] pid={os.getpid()}@{_host()} role={NEXUS_ROLE} owns the ledger")

def poll_takeover():
    req = meta_get(TAKEOVER_KEY)
    if not req: return
    try: d = json.loads(req)
    except Exception: return
    if now_ts() - float(d.get("ts") or 0) > TAKEOVER_REQ_TTL: return
    if d.get("req_host") == _host() and int(d.get("req_pid") or 0) == os.getpid(): return
    print(f"[{hms()}] [takeover] superseded by pid {d.get('req_pid')}@{d.get('req_host')} — exiting")
    _shutdown.set()

# ================================================================ TELEGRAM
_tg_next = 0.0; _tg_tasks = set()
def _tg_done(t):
    _tg_tasks.discard(t)
    if not t.cancelled() and t.exception(): print(f"[tg   ] error: {t.exception()!r}")
async def tg_send(session, text, retries=1, backoff=5.0):
    global _tg_next
    if not TG_ENABLED or not session: return False
    for attempt in range(retries):
        w = _tg_next - time.time()
        if w > 0: await asyncio.sleep(w)
        try:
            async with session.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                json={"chat_id": TG_CHAT, "text": text[:4000], "disable_web_page_preview": True},
                timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200: _tg_next = time.time() + 1.1; return True
                if r.status == 429:
                    try: ra = float((await r.json()).get("parameters", {}).get("retry_after") or 2.0)
                    except Exception: ra = 2.0
                    _tg_next = time.time() + ra + 0.5
                    if attempt < retries - 1: await asyncio.sleep(ra + 0.5)
                    continue
                print(f"[tg   ] HTTP {r.status} (attempt {attempt+1}/{retries})")
                if attempt < retries - 1: await asyncio.sleep(backoff)
        except Exception as e:
            print(f"[tg   ] {e} (attempt {attempt+1}/{retries})")
            if attempt < retries - 1: await asyncio.sleep(backoff)
    return False
def notify(session, text):
    if not TG_ENABLED: return
    try:
        t = asyncio.ensure_future(tg_send(session, text)); _tg_tasks.add(t); t.add_done_callback(_tg_done)
    except Exception: pass
_entry_tasks = set()
def _entry_done(t):
    _entry_tasks.discard(t)
    if not t.cancelled() and t.exception(): print(f"[entry] error: {t.exception()!r}")
def spawn_entry(c):
    t = asyncio.ensure_future(c); _entry_tasks.add(t); t.add_done_callback(_entry_done); return t
_bg_tasks = set()
def _bg_done(t):
    _bg_tasks.discard(t)
    if not t.cancelled() and t.exception(): print(f"[bg   ] error: {t.exception()!r}")
def _bg(coro):
    t = asyncio.ensure_future(coro); _bg_tasks.add(t); t.add_done_callback(_bg_done); return t

# ================================================================ BRAIN (alignment + exhaustion only; direction veto DEAD)
_brain_cache = {"ts": 0.0, "now": {}, "30m": {}, "exh": {}}
def _brain_snapshot(minutes=25):
    """Latest call per (symbol,direction) — ranked BEFORE any confidence filter."""
    global _brain_cache
    now = now_ts()
    if now - _brain_cache["ts"] < 10: return _brain_cache
    now_calls, m30, exh = {}, {}, {}
    try:
        c = sqlite3.connect(ALERT_DB, timeout=15); c.execute("PRAGMA busy_timeout=15000")
        for band, store in (("now", now_calls), ("30m", m30)):
            rows = c.execute("""
                SELECT symbol, direction, p_final, detected_ts FROM (
                    SELECT a.symbol AS symbol, a.direction AS direction,
                           ap.p_final AS p_final, a.detected_ts AS detected_ts,
                           ROW_NUMBER() OVER (PARTITION BY a.symbol, a.direction
                                              ORDER BY a.detected_ts DESC, a.id DESC) AS rn
                    FROM alert_predictions ap JOIN alerts a ON a.id=ap.alert_id
                    WHERE ap.band=? AND a.detected_ts>=?
                      AND a.source LIKE 'live_nexus%'
                ) WHERE rn=1""",
                (band, now - minutes * 60)).fetchall()
            for sym, d, p, ts in rows:
                store.setdefault(sym, []).append((d, float(p or 0.0), ts))
        rows2 = c.execute("""SELECT a.symbol, a.features FROM alerts a
            WHERE a.detected_ts>=? AND a.source LIKE 'live_nexus%' AND a.features IS NOT NULL""",
            (now - 900,)).fetchall()
        c.close()
        for sym, feat in rows2:
            try:
                f = json.loads(feat or "{}"); e = f.get("exh_score")
                if e is not None: exh[sym] = max(exh.get(sym, 0.0), float(e))
            except Exception: pass
    except Exception as e: print(f"[warn ] brain read: {e}")
    _brain_cache = {"ts": now, "now": now_calls, "30m": m30, "exh": exh}
    return _brain_cache

def _brain_agrees(sym, direction):
    """Latest NOW call agrees ≥ALIGN_CAL_BOOST → SCALP tier. Tier routing +
    ranking boost ONLY — the direction veto is dead (1,549 measurements:
    the Brain's calls anti-predict this horizon)."""
    calls = _brain_snapshot()["now"].get(sym) or []
    if not calls: return False
    d, p, _ts = max(calls, key=lambda x: x[2])
    return d == direction and p >= ALIGN_CAL_BOOST

# ================================================================ MARKET DATA
_fc_fail = {}
def _log_fc_fail(sym, msg):
    now = now_ts()
    if now < _fc_fail.get(sym, 0): return
    _fc_fail[sym] = now + 60; print(f"[{hms()}] [warn ] REST {sym}: {msg}")
async def fetch_vol_and_high(session, symbol):
    try:
        async with session.get(f"{FAPI}/fapi/v1/klines", params={"symbol": symbol, "interval": "1m", "limit": 62},
                               timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200: _log_fc_fail(symbol, f"HTTP {r.status}"); return None
            kl = await r.json()
        if not isinstance(kl, list) or len(kl) < 22: return None
        qvs = [float(k[7]) for k in kl[:-1]]
        med = statistics.median(qvs[:-1]) if len(qvs) > 5 else 0.0
        rel = (qvs[-1] / med) if med > 0 else None
        hi1h = max(float(k[2]) for k in kl[:-1])
        px = float(kl[-1][4])
        return qvs[-1], rel, bool(px > 0 and hi1h > 0 and px >= hi1h)
    except Exception as e: _log_fc_fail(symbol, repr(e)); return None
async def fetch_oi_change(session, symbol):
    """openInterestHist — 5m granularity, reporting latency. Ranking only."""
    try:
        async with session.get(f"{FAPI}/futures/data/openInterestHist",
            params={"symbol": symbol, "period": "5m", "limit": 7}, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200: return None
            arr = await r.json()
        if not isinstance(arr, list) or len(arr) < 2: return None
        f, l = float(arr[0].get("sumOpenInterestValue") or 0), float(arr[-1].get("sumOpenInterestValue") or 0)
        return (l / f - 1) * 100 if (f > 0 and l > 0) else None
    except Exception: return None
async def fetch_oi_now(session, symbol):
    """CURRENT open interest (coin quantity) — no reporting latency. The
    OI_FLUSH instrument and PFADE's re-long confirmation poll this live."""
    try:
        async with session.get(f"{FAPI}/fapi/v1/openInterest", params={"symbol": symbol},
                               timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status != 200: return None
            d = await r.json()
        oi = float(d.get("openInterest") or 0)
        return oi if oi > 0 else None
    except Exception: return None
async def fetch_book_data(session, symbol):
    try:
        async with session.get(f"{FAPI}/fapi/v1/depth", params={"symbol": symbol, "limit": 5},
                               timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200: return None, None
            d = await r.json()
        bids, asks = d.get("bids") or [], d.get("asks") or []
        if not bids or not asks: return None, None
        b = sum(float(x[1]) for x in bids[:5]); a = sum(float(x[1]) for x in asks[:5])
        imb = (b - a) / (b + a) if (b + a) > 0 else None
        bb, ba = float(bids[0][0]), float(asks[0][0])
        spread = (ba - bb) / ((ba + bb) / 2) * 100 if (bb > 0 and ba >= bb) else None
        return imb, spread
    except Exception as e: _log_fc_fail(symbol, repr(e)); return None, None
async def fetch_funding_rate(session, symbol):
    """(funding_rate_decimal, next_funding_ts). FUNDING_TS logger consumes
    nextFundingTime. Single definition."""
    try:
        async with session.get(f"{FAPI}/fapi/v1/premiumIndex", params={"symbol": symbol},
                               timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status != 200: return None, None
            d = await r.json()
        fr = float(d.get("lastFundingRate") or 0)
        nf = d.get("nextFundingTime")
        return fr, (int(nf) / 1000.0 if nf else None)
    except Exception: return None, None

def clamp(x, lo, hi): return max(lo, min(hi, x))

async def rank_candidate(session, w):
    sym, side, etype = w["symbol"], (1 if w["direction"] == "up" else -1), w["etype"]
    age = now_ts() - w["ts"]
    comp = {"prio": LANE_PRIO.get(etype, 0.70), "history": 0.0, "relvol": 0.0, "rawvol": 0.0,
            "oi": 0.0, "book": 0.0, "fresh": clamp(1.0 - age / WAITROOM_SEC, 0, 1), "align": 0.0}
    comp["history"] = clamp(symbol_history_exp(sym) / 15.0, -1, 1)
    if _brain_agrees(sym, w["direction"]):
        comp["align"] = 0.15
        w["aligned"] = True
    vh = await fetch_vol_and_high(session, sym)
    if vh:
        qv, rel, is_1h_high = vh
        if rel is not None: comp["relvol"] = clamp((rel - 1.0) / 3.0, -1, 1)
        comp["rawvol"] = clamp(math.log10(max(qv, 1.0) / 1000.0) / 3.0, 0, 1)
        if etype in ("EXPLOSION", "SQUEEZE") and is_1h_high: comp["prio"] = max(comp["prio"], 0.9)
    oi = await fetch_oi_change(session, sym)
    if oi is not None: comp["oi"] = clamp(abs(oi) / 5.0, 0, 1) * (1 if oi * side > 0 else -1)
    imb, spread = await fetch_book_data(session, sym)
    if imb is not None: comp["book"] = clamp(imb * side, -1, 1)
    if spread is not None: w["spread"] = spread
    score = (W_PRIO * comp["prio"] + W_HIST * comp["history"] + W_REL * comp["relvol"] +
             W_RAW * comp["rawvol"] + W_OI * comp["oi"] + W_BOOK * comp["book"] +
             W_FRESH * comp["fresh"] + comp["align"])
    return score, comp

# ================================================================ SIZING & PURE FUNCTIONS
_btc_hot = False

def notional_for(symbol, spread_pct=None, size_mult=1.0):
    """Single sizing source: $45 / (2.2% + slippage), session multiplier,
    size_mult (half-size lanes), cap $1500. Uniform hard stop."""
    sp = spread_pct if (spread_pct is not None and spread_pct > 0) else 0.10
    slip = min(max(SLIP_SPREAD_MULT * sp, SLIP_MIN_PCT), SLIP_MAX_PCT)
    notional = min(RISK_DOLLARS / ((SL_PCT + slip) / 100.0), NOTIONAL_MAX)
    notional *= SESSION_SIZES.get(datetime.now(timezone.utc).hour, 1.0)
    return max(notional * size_mult, 50.0)

def _ladder_floor(peak):
    """THE profit ladder (pure — numerically self-tested, incl. the RAREUSDT
    case): peak < 0.8 → None · 0.8–3.0 → peak-0.5 · ≥3.0 → peak-1.0.
    Crossing 3.0 widens the gap (floor may drop — intentional breathing)."""
    if peak < LADDER_ARM_PCT: return None
    gap = LADDER_GAP_BIG if peak >= LADDER_GAP_AT else LADDER_GAP
    return peak - gap

def _early_cut(peak, fav):
    """Early cut (pure): the ladder never armed AND price ≤ -1.5%."""
    return peak < LADDER_ARM_PCT and fav <= -EARLY_CUT_PCT

def _is_systemic(btc_drop_pct):
    """Systemic-dump filter (pure): BTC fell ≥0.40% in the window → the coin's
    dump is beta. Recalibrated 0.75→0.40 (0 firings in 1,097 dumps = decorative)."""
    return btc_drop_pct <= -SYS_BTC_DROP_PCT

async def live_price(session, symbol):
    try:
        async with session.get(f"{FAPI}/fapi/v1/ticker/price", params={"symbol": symbol},
                               timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status == 200: return float((await r.json())["price"])
    except Exception: pass
    return None
def roundtrip_cost(notional, spread_pct):
    sp = spread_pct if (spread_pct is not None and spread_pct > 0) else 0.03
    slip = min(max(SLIP_SPREAD_MULT * sp, SLIP_MIN_PCT), SLIP_MAX_PCT)
    return notional * (2 * TAKER_FEE_PCT_SIDE + slip) / 100.0
_lane_cd = {}
def _set_lane_cooldown(symbol, lane, net_pnl):
    """v4.8 flexible cooldown, max 1h: win → 5 min; loss → scaled by the
    money lost (a full ~$45 stop = 60 min, tiny losses hit the 10 min floor).
    Applies only to the lanes that had timed cooldowns before; REV/FSQZ/DIP-X
    remain cooldown-free. Episode scoping is separate and unchanged."""
    if net_pnl >= 0:
        cd = LANE_COOLDOWN_WIN_SEC
    else:
        cd = LANE_COOLDOWN_MAX_SEC * min(1.0, (-net_pnl) / RISK_DOLLARS)
        cd = clamp(cd, LANE_COOLDOWN_MIN_SEC, LANE_COOLDOWN_MAX_SEC)
    _lane_cd[(symbol, lane)] = now_ts() + cd
def _lane_cooldown_ok(symbol, lane, now):
    if lane not in FLEX_CD_LANES: return True
    end = _lane_cd.get((symbol, lane))
    return end is None or now >= end

# ═══════════════════════════════ END PART 1/4 ═══════════════════════════════
# Part 2/4 begins at: # ============ STREAM STATE + REV v2 EPISODES
# ================================================================ STREAM STATE
# v4.7: whale/liq attrs, vol-stop cache, q_hist — ABSENT (deleted lanes/machines).
class StreamState:
    def __init__(self):
        # miniTicker-derived
        self.px_windows = {}       # sym → deque[(ts, px)]   2s-sampled, 920s retention
        self.btc_px = deque()      # (ts, px) 1s BTC closes, 30m+ retention — regime tag source
        self.q24 = {}              # sym → latest 24h quote volume
        self.rev_1s = {}           # sym → deque[(sec, close)]  maxlen 75 — REV v2 detection
        self.watch = {}            # sym → {"low","started","etype"}   FSQZ pinned skeleton only
        self.last_check = {}       # FS dump throttle
        self.funding_checked = {}  # sym → ts (FS REST throttle)
        self.funding_ts_pends = {} # sym → (settle_ts, rate)  FUNDING_TS logger
        # REV v2 episodes
        self.rev_eps = {}          # sym → episode dict (state machine, _rev_tick_ep)
        self.bid = {}              # sym → (ts, bid)   from episode bookTicker
        # aggTrade subsystem (1s recorder evidence layer)
        self.t1s = {}              # sym → {"last_id","started","dq","cur"}  per-second OHLC+q
        self.trade_last_msg = 0.0
        # EXPLOSION — v4.6.4 q24 machine
        self.pendings = {}         # sym → explosion pullback {dir, level, ts, trigger_px}
        self.exp_acc = {}          # sym → [minute_bucket, vol, first_px]
        self.exp_fired = {}        # sym → minute bucket last fired
        # recorder
        self.rec_syms = set()      # top-150 scope (refreshed by recorder worker)
        self.rec_episodes = set()  # episode-scope escalation
        self.rec_buf = []          # completed 1s bars awaiting flush
        self.rec_bid_buf = []      # completed 1s bids awaiting flush
        # shared
        self.confirming = set()
        self.waitroom = {}
        self.seeding = set()
        self.msgs = 0
        self.tickers = 0
        self.sig_log = deque(maxlen=500)   # co-fire audit: (ts, sym, lane)
SS = StreamState()
_seed_q: asyncio.Queue = asyncio.Queue(); _seed_fail = {}

def _ws_reset(feed):
    """Reconnect hygiene. No volume/price delta may span an outage."""
    if feed == "ticker":
        SS.px_windows.clear(); SS.q24.clear(); SS.rev_1s.clear(); SS.btc_px.clear()
        SS.rev_eps.clear()     # book_manager unsubscribes via wanted-set diff
        SS.watch.clear(); SS.last_check.clear()
        SS.pendings.clear(); SS.exp_acc.clear(); SS.exp_fired.clear()
    elif feed == "trades":
        SS.t1s.clear(); SS.trade_last_msg = 0.0
    decide("system", "*", "ws_reset", {"feed": feed})
    print(f"[{hms()}] [ws   ] {feed} state reset (reconnect hygiene)")

def _spawn_seed(sym):
    if sym in SS.seeding or now_ts() < _seed_fail.get(sym, 0): return
    SS.seeding.add(sym); _seed_q.put_nowait(sym)
def _record_tick(sym, px, q24, now):
    """miniTicker ingest: price windows, q24, seeding, and the REV v2 1s-close
    series (miniTicker fires ~1/s per symbol → one close per second bucket)."""
    if px <= 0 or sym in STABLE_SYMBOLS: return
    SS.q24[sym] = q24; SS.tickers += 1
    dq = SS.px_windows.get(sym)
    if dq is None: dq = SS.px_windows[sym] = deque()
    if not dq or now - dq[-1][0] >= 2.0: dq.append((now, px))
    while dq and now - dq[0][0] > PX_RETAIN_SEC: dq.popleft()
    if sym == "BTCUSDT":
        # dedicated 30m BTC series for the regime tag (px_windows can't span it)
        SS.btc_px.append((now, px))
        while SS.btc_px and now - SS.btc_px[0][0] > 1860.0: SS.btc_px.popleft()
    if len(dq) < SEED_KLINES: _spawn_seed(sym)
    sec = int(now)
    r1 = SS.rev_1s.get(sym)
    if r1 is None: r1 = SS.rev_1s[sym] = deque(maxlen=75)
    if r1 and r1[-1][0] == sec: r1[-1] = (sec, px)
    else: r1.append((sec, px))
async def _seed_one(session, sym) -> bool:
    try:
        async with session.get(f"{FAPI}/fapi/v1/klines",
            params={"symbol": sym, "interval": "1m", "limit": SEED_KLINES},
            timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200: return False
            kl = await r.json()
        if not isinstance(kl, list) or len(kl) < 2: return False
        now = now_ts(); seeds = []
        for k in kl[:-1]:
            t = int(k[6]) / 1000.0; p = float(k[4])
            if p > 0 and t < now - 1.0: seeds.append((t, p))
        dq = SS.px_windows.get(sym)
        if dq is None: return True
        newest = seeds[-1][0] if seeds else 0.0
        live = [(t, p) for (t, p) in dq if t > newest]
        merged = sorted(seeds + live, key=lambda x: x[0])
        dq.clear(); dq.extend(merged)
        return True
    except Exception: return False
async def seed_worker(session):
    while not _shutdown.is_set():
        try: sym = await asyncio.wait_for(_seed_q.get(), timeout=1.0)
        except asyncio.TimeoutError: continue
        try:
            ok = await _seed_one(session, sym)
            if not ok: _seed_fail[sym] = now_ts() + SEED_FAIL_COOLDOWN
        except Exception: _seed_fail[sym] = now_ts() + SEED_FAIL_COOLDOWN
        finally:
            SS.seeding.discard(sym); _seed_q.task_done()
        await asyncio.sleep(SEED_PACE_SEC)
def _px_ago(sym, now, win):
    dq = SS.px_windows.get(sym)
    if not dq or now - dq[0][0] < win: return None
    t = now - win
    for ts, p in dq:
        if ts >= t: return p
    return None
def _extremes(sym, now, win):
    dq = SS.px_windows.get(sym)
    if not dq: return None, None, None, None
    lo = hi = None; lo_t = hi_t = None
    for t, p in dq:
        if t < now - win: continue
        if lo is None or p < lo: lo, lo_t = p, t
        if hi is None or p > hi: hi, hi_t = p, t
    return lo, lo_t, hi, hi_t
def stream_price(sym):
    dq = SS.px_windows.get(sym)
    if dq:
        t, p = dq[-1]
        if p > 0 and now_ts() - t <= 5.0: return p
    return None

# ================================================================ CO-FIRE AUDIT
def _mark_signal(sym, lane, now):
    """Co-fire audit ledger: every real signal tagged. Paper-lane arming and
    the report read this; ±10min same-coin overlap is the metric."""
    SS.sig_log.append((now, sym, lane))
def _cofire_overlap(sym, lane, window=CO_FIRE_WINDOW):
    """Fraction of this lane's signals on sym that co-fire with another lane
    within ±window seconds. Used by the paper-arming audit."""
    mine = [(t, l) for t, s, l in SS.sig_log if s == sym and l == lane]
    if not mine: return 0.0
    hits = 0
    for t, _l in mine:
        others = [1 for t2, s2, l2 in SS.sig_log
                  if s2 == sym and l2 != lane and abs(t2 - t) <= window]
        if others: hits += 1
    return hits / len(mine)

# ================================================================ REV v2 — TICK EPISODE ENGINE
def _rev_tick_ep(ep, px, now):
    """The three-outcome watch — PURE state machine on the episode dict
    (numerically self-tested in Part 4). Mutates ep; returns:
      'enter'  bid bounced ≥0.5% off the low
      'dead'   10s watch elapsed with neither new low nor bounce
      'stale'  episode older than REV_STALE_SEC from trigger (falling-forever cap)
      None     keep watching
    New low → resets low + 10s clock (your #5: no entering a second leg)."""
    if now - ep["trig_ts"] > REV_STALE_SEC: return "stale"
    if px is None or px <= 0: return None
    if px < ep["low"]:
        ep["low"] = px; ep["low_ts"] = now; ep["resets"] = ep.get("resets", 0) + 1
        return None
    if px >= ep["low"] * (1 + REV_BOUNCE_PCT / 100.0): return "enter"
    if now - ep["low_ts"] > REV_WATCH_SEC: return "dead"
    return None

def _rev_resolve(session, sym, ep, outcome, px=None):
    """End an episode: log, tag, release bookTicker (wanted-set diff), let the
    OI poller finish its post-window."""
    SS.rev_eps.pop(sym, None)
    ep["resolved"] = outcome; ep["resolved_ts"] = now_ts()
    decide("rev_episode", sym, f"resolved_{outcome}",
           {"ep": ep["ep_id"], "low": ep["low"], "resets": ep.get("resets", 0),
            "dur": round(now_ts() - ep["trig_ts"], 1)})
    if outcome == "enter":
        _mark_signal(sym, "REV", now_ts())
        print(f"[{hms()}] [sig  ] {sym} ⚡REV v2: bid +{REV_BOUNCE_PCT}% off {ep['low']:.6g} "
              f"(dump {ep['dump']:.1f}%/60s, {ep.get('resets', 0)} new-low resets)")
        # _signal defined Part 3 — episode_id rides into the position row
        _signal(session, sym, "REV", "up", px, now_ts(), episode_id=ep["ep_id"])

def _rev_episode_scan(session, now):
    """1s-close dump detection (trigger) + episode ticking. Runs at miniTicker
    cadence (~1/s). Detection: current 1s close ≤ -3% vs the highest 1s close
    of the trailing 60s. Systemic dumps are skipped (REV only; FSQZ exempt)."""
    if not ENABLE_REV or not IS_SCANNER: return
    # tick active episodes first
    for sym in list(SS.rev_eps.keys()):
        ep = SS.rev_eps[sym]
        bid = SS.bid.get(sym)
        px = bid[1] if (bid and now_ts() - bid[0] <= 5.0) else stream_price(sym)
        out = _rev_tick_ep(ep, px, now)
        if out == "enter": _rev_resolve(session, sym, ep, "enter", px)
        elif out in ("dead", "stale"): _rev_resolve(session, sym, ep, out)
    # trigger new episodes
    if kill_now() or _shutdown.is_set(): return
    for sym, dq in list(SS.rev_1s.items()):
        if sym in SS.rev_eps or len(dq) < 45: continue
        c = dq[-1][1]
        t60 = int(now) - 60
        hi = max(p for s, p in dq if s >= t60)
        if hi <= 0: continue
        dump = (c / hi - 1.0) * 100.0
        if dump > -REV_DUMP_PCT: continue
        # systemic filter (recalibrated 0.40): BTC's own 60s drop
        b5 = _px_ago("BTCUSDT", now, 60); btc = stream_price("BTCUSDT")
        btc_drop = (btc / b5 - 1.0) * 100.0 if (b5 and btc and b5 > 0) else 0.0
        if _is_systemic(btc_drop):
            decide("skip", sym, "rev2_systemic", {"dump": round(dump, 2), "btc60": round(btc_drop, 2)})
            continue
        if symbol_busy(sym):
            decide("skip", sym, "rev2_symbol_busy", {"dump": round(dump, 2)}); continue
        if not _dip_budget_ok():
            decide("skip", sym, "rev2_dip_budget", {"dump": round(dump, 2)}); continue
        ep_id = f"rev2|{sym}|{int(now)}"
        SS.rev_eps[sym] = {"ep_id": ep_id, "trig_ts": now, "low_ts": now,
                           "low": c, "dump": -dump, "resets": 0}
        SS.rec_episodes.add(sym)   # recorder escalation + bid recording
        print(f"[{hms()}] [sig  ] {sym} ⚡REV v2 armed: -{abs(dump):.1f}%/60s on 1s closes → bid watch")
        decide("watch", sym, "rev2_dump_armed", {"ep": ep_id, "dump": round(dump, 2)})
        _bg(_rev_oi_instrument(session, sym, ep_id))

async def _rev_oi_instrument(session, sym, ep_id):
    """OI_FLUSH (Phase 0, log-only): poll CURRENT open interest every 10s from
    trigger to +120s after resolution. Rows go to oi_log; the arm decision at
    2 weeks / 40 events compares OI-down vs OI-up dump outcomes."""
    t0 = None
    try:
        for _ in range(24):                       # hard cap ≈ 4 min of polling
            if _shutdown.is_set(): break
            oi = await fetch_oi_now(session, sym)
            if oi:
                if t0 is None: t0 = oi
                with closing(sdb()) as c, c:
                    c.execute("INSERT INTO oi_log(episode_id,symbol,ts,oi,oi_delta_pct,phase) "
                              "VALUES(?,?,?,?,?,?)",
                              (ep_id, sym, now_ts(), oi, (oi / t0 - 1.0) * 100.0 if t0 else None,
                               "live"))
            await asyncio.sleep(OI_POLL_SEC)
            if sym not in SS.rev_eps:
                # episode resolved: OI_WINDOW_POST more seconds, then stop.
                # (v4.7.0 also required rec_episodes to be disjoint — but this
                # coroutine itself holds the symbol there until its finally,
                # so the post phase was unreachable.)
                for _ in range(int(OI_WINDOW_POST / OI_POLL_SEC)):
                    if _shutdown.is_set(): break
                    oi = await fetch_oi_now(session, sym)
                    if oi and t0:
                        with closing(sdb()) as c, c:
                            c.execute("INSERT INTO oi_log(episode_id,symbol,ts,oi,oi_delta_pct,phase) "
                                      "VALUES(?,?,?,?,?,'post')",
                                      (ep_id, sym, now_ts(), oi, (oi / t0 - 1.0) * 100.0))
                    await asyncio.sleep(OI_POLL_SEC)
                break
    except Exception as e:
        print(f"[{hms()}] [warn ] oi instrument {sym}: {e!r}")
    finally:
        SS.rec_episodes.discard(sym)

# ================================================================ EPISODE bookTicker MANAGER
async def book_manager():
    """Dynamic per-episode bookTicker subscriptions. REV v2 tracks its low on
    BID; a standing full-universe bookTicker feed would be Binance's heaviest
    stream, so we subscribe exactly the symbols with active episodes and drop
    them at resolution. Also feeds bids_1s for the recorder (episodes only)."""
    wanted = set()
    idx = 0
    while not _shutdown.is_set():
        try:
            async with websockets.connect(f"{WS_BASES[idx % len(WS_BASES)]}/stream",
                ping_interval=20, ping_timeout=20, max_queue=2048, open_timeout=15) as ws:
                print("[book  ] episode bid stream connected")
                req_id = 0
                while not _shutdown.is_set():
                    need = set(SS.rev_eps.keys())
                    add, rem = need - wanted, wanted - need
                    if add:
                        req_id += 1
                        await ws.send(json.dumps({"method": "SUBSCRIBE",
                            "params": [s.lower() + "@bookTicker" for s in add], "id": req_id}))
                        wanted |= add
                    if rem:
                        req_id += 1
                        await ws.send(json.dumps({"method": "UNSUBSCRIBE",
                            "params": [s.lower() + "@bookTicker" for s in rem], "id": req_id}))
                        wanted -= rem
                    try: msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError: continue
                    try: d = json.loads(msg)
                    except Exception: continue
                    if "result" in d and d.get("id") is not None: continue   # ack frames
                    stream = d.get("stream") or ""
                    if not stream.endswith("@bookTicker"): continue
                    data = d.get("data") or {}
                    sym = data.get("s") or ""
                    if not sym or sym not in SS.rev_eps: continue
                    try: bid = float(data.get("b") or 0)
                    except Exception: continue
                    if bid <= 0: continue
                    now = now_ts()
                    SS.bid[sym] = (now, bid)
                    sec = int(now)
                    if SS.rec_bid_buf and SS.rec_bid_buf[-1][0] == sym and SS.rec_bid_buf[-1][1] == sec:
                        SS.rec_bid_buf[-1] = (sym, sec, bid)   # keep latest bid in-second
                    else:
                        SS.rec_bid_buf.append((sym, sec, bid))
        except asyncio.CancelledError: raise
        except Exception as e:
            print(f"[book  ] down ({e}); retry 5s")
            idx += 1
            await asyncio.sleep(5)

# ================================================================ EXPLOSION — v4.6.4 q24 MACHINE (restored by directive)
def _record_trade(sym, price, quantity, trade_id, now):
    """aggTrade ingest — v4.6.3 exactly-once semantics, extended to per-second
    OHLC+notional buckets (the recorder's evidence layer)."""
    if sym in STABLE_SYMBOLS or not sym.endswith("USDT"): return
    try:
        usd = float(price) * float(quantity)
        tid = int(trade_id)
    except Exception: return
    if not math.isfinite(usd) or usd <= 0: return
    st = SS.t1s.get(sym)
    if st is None: st = SS.t1s[sym] = {"last_id": -1, "started": 0.0, "dq": deque(), "cur": None}
    if tid <= st["last_id"]: return                      # duplicate suppression (verbatim)
    st["last_id"] = tid
    if not st["started"]: st["started"] = now
    SS.trade_last_msg = now
    sec = int(now)
    cur = st["cur"]
    if cur is None or cur["sec"] != sec:
        if cur is not None:
            st["dq"].append((cur["sec"], cur["o"], cur["h"], cur["l"], cur["c"], cur["q"]))
            if sym in SS.rec_syms or sym in SS.rec_episodes:
                SS.rec_buf.append((sym, cur["sec"], cur["o"], cur["h"], cur["l"], cur["c"], cur["q"]))
            while st["dq"] and st["dq"][0][0] < sec - 200:   # pace machine removed v4.8
                st["dq"].popleft()
        cur = st["cur"] = {"sec": sec, "o": price, "h": price, "l": price, "c": price, "q": usd}
    else:
        cur["h"] = max(cur["h"], price); cur["l"] = min(cur["l"], price)
        cur["c"] = price; cur["q"] += usd

# ---- v4.6.4 q24 machine (user-directed restore; verbatim logic) ----
_vol_medians = {}
async def _vol_median_refresher(session):
    """Sparse REST: per-symbol median 1m quote volume, top symbols, 10 min."""
    while not _shutdown.is_set():
        try:
            syms = [s for s, _q in sorted(SS.q24.items(), key=lambda kv: -kv[1])[:VOL_MEDIAN_SYMS]]
            for sym in syms:
                if _shutdown.is_set(): break
                try:
                    async with session.get(f"{FAPI}/fapi/v1/klines",
                        params={"symbol": sym, "interval": "1m", "limit": 10},
                        timeout=aiohttp.ClientTimeout(total=8)) as r:
                        if r.status == 200:
                            kl = await r.json()
                            if isinstance(kl, list) and len(kl) >= 6:
                                qvs = [float(k[7]) for k in kl[:-1]]
                                _vol_medians[sym] = statistics.median(qvs)
                except Exception: pass
                await asyncio.sleep(0.05)
        except Exception as e: print(f"[warn ] vol-median refresh: {e}")
        await asyncio.sleep(VOL_MEDIAN_REFRESH_SEC)

def _explosion_accumulate(sym, vol_delta, px, now):
    """v4.6.4 verbatim: q24 deltas consumed EXACTLY ONCE (stream_reader passes
    them atomically with the tick). Negative rolling-window deltas ignored
    (documented q24 limitation). Projects the minute's volume pace; fires
    seconds 3–10 on ≥20x median AND ≥1% move; one shot per minute bucket."""
    if not ENABLE_EXPLOSION or not IS_SCANNER: return
    if vol_delta is None or vol_delta <= 0: return
    med = _vol_medians.get(sym)
    if not med or med <= 0: return
    bucket = int(now // 60)
    st = SS.exp_acc.get(sym)
    if st is None or st[0] != bucket:
        dq = SS.px_windows.get(sym)
        first_px = dq[0][1] if (dq and dq[0][0] >= bucket * 60 - 5) else px
        st = SS.exp_acc[sym] = [bucket, 0.0, first_px]
    st[1] += vol_delta
    elapsed = now - bucket * 60.0
    if elapsed < EXPLOSION_EARLY_MIN_SEC or elapsed > EXPLOSION_EARLY_SEC: return
    if SS.exp_fired.get(sym) == bucket: return
    if not px or px <= 0: return
    projected = (st[1] / elapsed) * 60.0
    mult = projected / med
    if mult < EXPLOSION_VOL_X: return
    first_px = st[2]
    if not first_px or first_px <= 0: return
    move = (px / first_px - 1.0) * 100.0
    if abs(move) < EXPLOSION_MIN_MOVE_PCT:
        decide("skip", sym, "explosion_no_move", {"mult": mult, "move": move}); return
    direction = "up" if move > 0 else "down"
    SS.exp_fired[sym] = bucket
    print(f"[{hms()}] [sig  ] {sym} 💥EXPLOSION-IN-PROGRESS: ${st[1]:,.0f}$ in {elapsed:.0f}s "
          f"(pace {mult:.0f}x median), move {move:+.2f}% → {direction.upper()}")
    if SS.pendings.get(sym) or symbol_busy(sym): return
    limit_px = px * (1 - EXPLOSION_PULLBACK_PCT / 100.0) if direction == "up" \
        else px * (1 + EXPLOSION_PULLBACK_PCT / 100.0)
    SS.pendings[sym] = {"symbol": sym, "direction": direction, "level": limit_px,
                        "trigger_px": px, "ts": now}
    decide("explosion_place", sym, "pullback_parked",
           {"dir": direction, "pace_mult": mult, "move": move, "limit": limit_px})
    print(f"[{hms()}] [pb   ] {sym} explosion pullback "
          f"{'BUY' if direction == 'up' else 'SELL'} @{limit_px:.6g} parked")

def _check_explosion_fills(session, now):
    """v4.6.4 hybrid fill WITH knife-guard (user-directed restore). Fills book
    the limit price after a favorable (decelerating) tick; a falling (for a
    BUY) touch does not fill. TTL 120s."""
    for sym, p in list(SS.pendings.items()):
        if now - p["ts"] > EXPLOSION_PB_TTL_SEC:
            del SS.pendings[sym]; decide("expire", sym, "explosion_pb_timeout", {}); continue
        dq = SS.px_windows.get(sym)
        if not dq or len(dq) < 2: continue
        px = dq[-1][1]; prev = dq[-2][1]
        hit = (px <= p["level"]) if p["direction"] == "up" else (px >= p["level"])
        if not hit: continue
        if p["direction"] == "up" and px < prev: continue    # knife-guard
        if p["direction"] == "down" and px > prev: continue  # knife-guard
        del SS.pendings[sym]
        decide("explosion_fill", sym, "filled", {"limit": p["level"], "px": px})
        print(f"[{hms()}] [sig  ] {sym} 💥EXPLOSION pullback fill @{px:.6g} (knife-guard OK)")
        spawn_entry(_explosion_open(session, p))

async def _explosion_open(session, p):
    """v4.6.4 fill-time admission — ALL checks re-run (detection up to 120s
    stale). No direction caps (v4.8). Full size."""
    key = (p["symbol"], "EXPLOSION")
    if key in SS.confirming: return
    SS.confirming.add(key)
    try:
        if kill_now() or _shutdown.is_set(): decide("skip", p["symbol"], "killed", {}); return
        if symbol_busy(p["symbol"]): decide("reject", p["symbol"], "explosion_busy", {}); return
        if len(open_positions()) >= MAX_CONCURRENT:
            decide("reject", p["symbol"], "explosion_capacity", {}); return
        _imb, spread = await fetch_book_data(SESSION_M, p["symbol"])
        tier = _final_admission(p["symbol"], "EXPLOSION", p["direction"], p["level"])
        if tier is None:
            decide("reject", p["symbol"], "explosion_final_admission", {}); return
        open_position("EXPLOSION", tier, p["symbol"], p["direction"], p["level"],
                      spread=spread, trigger_ts=now_ts(), btc_regime=_regime_tag())
    except Exception as e: print(f"[{hms()}] [err  ] explosion open {p['symbol']}: {e!r}")
    finally: SS.confirming.discard(key)

# ================================================================ 1s RECORDER (evidence layer)
async def market_recorder():
    """Flushes completed 1s bars (top-150 by q24 + episode symbols) and episode
    bids to the SEPARATE market DB. Batched every 5s; 7-day pruning. This data
    cannot be backfilled — it is the replay/autopsy layer for REV v2 and the
    EXPLOSION validation week."""
    mdb_init()
    last_prune = 0.0
    last_scope = 0.0
    while not _shutdown.is_set():
        try:
            now = now_ts()
            if now - last_scope > 300.0:
                SS.rec_syms = {s for s, _q in sorted(SS.q24.items(), key=lambda kv: -kv[1])[:RECORDER_TOP_N]}
                last_scope = now
            bars = SS.rec_buf; SS.rec_buf = []
            bids = SS.rec_bid_buf; SS.rec_bid_buf = []
            if len(bars) > REC_BUF_CAP:      # persistent DB failure → bounded loss
                print(f"[{hms()}] [warn ] recorder: dropping {len(bars) - REC_BUF_CAP} backlogged bars")
                bars = bars[-REC_BUF_CAP:]
            # drain stale partial seconds so nothing waits >1 flush cycle
            for sym, st in list(SS.t1s.items()):
                cur = st.get("cur")
                if cur and now - cur["sec"] >= 2 and (sym in SS.rec_syms or sym in SS.rec_episodes):
                    bars.append((sym, cur["sec"], cur["o"], cur["h"], cur["l"], cur["c"], cur["q"]))
                    st["cur"] = None
            if bars or bids:
                with closing(mdb()) as c, c:
                    if bars:
                        c.executemany("INSERT OR REPLACE INTO bars_1s(sym,ts,o,h,l,c,q) "
                                      "VALUES(?,?,?,?,?,?,?)", bars)
                    if bids:
                        c.executemany("INSERT OR REPLACE INTO bids_1s(sym,ts,bid) "
                                      "VALUES(?,?,?)", bids)
            if now - last_prune > 3600.0:
                cutoff = now - RECORDER_RETAIN_DAYS * 86400.0
                with closing(mdb()) as c, c:
                    c.execute("DELETE FROM bars_1s WHERE ts < ?", (cutoff,))
                    c.execute("DELETE FROM bids_1s WHERE ts < ?", (cutoff,))
                last_prune = now
        except Exception as e:
            print(f"[{hms()}] [warn ] recorder: {e!r}")
        await asyncio.sleep(RECORDER_FLUSH_SEC)

# ================================================================ FUNDING_SQZ — PINNED v4.6.4 SKELETON + FUNDING_TS
async def _funding_squeeze_arm(session, sym, now):
    """FSQZ qualification on the PINNED skeleton. High-vol gate EXEMPT (it is
    deleted book-wide). Also feeds the FUNDING_TS log-only lane: every arm
    with |funding| ≥0.10% records the settlement timestamp for drift logging."""
    try:
        last = SS.funding_checked.get(sym, 0.0)
        if now - last < FUNDING_CHECK_TTL: return
        SS.funding_checked[sym] = now
        fr, nf = await fetch_funding_rate(session, sym)
        if fr is None: return
        if ENABLE_FUNDING_TS and abs(fr) >= 0.0010:
            decide("log", sym, "funding_ts_sample",
                   {"funding": fr, "next_settle": nf, "ts": now})
            if nf: SS.funding_ts_pends[sym] = (float(nf), fr)
        if fr > FUNDING_EXTREME: return
        w = SS.watch.get(sym)
        if not w or w.get("etype") != "FS_CAND": return
        w["etype"] = "FUNDING_SQZ"
        print(f"[{hms()}] [watch] {sym} 🧲FUNDING-SQZ upgrade: funding {fr*100:.3f}% — trapped shorts")
        decide("watch", sym, "funding_sqz_armed", {"funding": fr, "low": w["low"]})
    except Exception as e:
        print(f"[{hms()}] [warn ] funding arm {sym}: {e!r}")

async def funding_ts_worker(session):
    """LOG-ONLY: for symbols with |funding| ≥0.10% at arm time, record the
    price at settlement and +30min → the settlement-drift measurement."""
    while not _shutdown.is_set():
        try:
            now = now_ts()
            for sym, (nf, fr) in list(SS.funding_ts_pends.items()):
                if now < nf: continue
                del SS.funding_ts_pends[sym]
                p0 = await live_price(session, sym)
                if not p0: continue
                async def _drift(sym=sym, nf=nf, fr=fr, p0=p0):
                    await asyncio.sleep(1800)
                    p1 = await live_price(SESSION_M, sym)
                    if p1:
                        mv = (p1 / p0 - 1.0) * 100.0
                        decide("log", sym, "funding_ts_drift",
                               {"funding": fr, "settle": nf, "mv_30m": round(mv, 4)})
                _bg(_drift())
        except Exception as e: print(f"[warn ] funding_ts: {e!r}")
        await asyncio.sleep(60)

# ================================================================ STREAM TRIGGERS (miniTicker cadence)
def _check_stream_triggers(session, now):
    """Per-miniTicker-batch scan: explosion detect/fills, REV v2 engine, and
    the FSQZ pinned skeleton (2%/60s tick dump → funding arm → 1% bounce).
    Detection is SCANNER-only: in split mode the trader runs the same ticker
    feed for prices (stream_price/_btc_hot/regime) but never detects."""
    if not IS_SCANNER: return
    _check_explosion_fills(session, now)
    _rev_episode_scan(session, now)
    for sym, dq in list(SS.px_windows.items()):
        if not dq: continue
        px = dq[-1][1]
        if px <= 0: continue
        # ── FSQZ (pinned skeleton; separate from REV v2 by design) ──
        w = SS.watch.get(sym)
        if w:
            w["low"] = min(w["low"], px)
            bounce = (px / w["low"] - 1.0) * 100.0
            if bounce >= FS_BOUNCE_PCT:
                del SS.watch[sym]
                if w["etype"] == "FUNDING_SQZ":
                    print(f"[{hms()}] [sig  ] {sym} 🧲FUNDING-SQZ bounce +{bounce:.2f}% off {w['low']:.6g}")
                    _mark_signal(sym, "FUNDING_SQZ", now)
                    _signal(session, sym, "FUNDING_SQZ", "up", px, now)
                else:
                    decide("skip", sym, "fs_unqualified_bounce", {"bounce": round(bounce, 2)})
                continue
            if now - w["started"] > 300.0:
                del SS.watch[sym]; decide("expire", sym, "fs_timeout", {"etype": w["etype"]})
            else: continue
        if not ENABLE_FUNDING_SQZ: continue
        lc = SS.last_check.get(sym, 0.0)
        if now - lc < TRIGGER_THROTTLE_S: continue
        p60 = _px_ago(sym, now, FS_DUMP_WIN)
        if p60 and p60 > 0 and (px / p60 - 1.0) * 100.0 <= -FS_DUMP_PCT:
            SS.last_check[sym] = now
            wlo, _a, _b, _c = _extremes(sym, now, FS_DUMP_WIN)
            SS.watch[sym] = {"low": wlo if (wlo and wlo > 0) else px,
                             "started": now, "etype": "FS_CAND"}
            decide("watch", sym, "fs_dump", {"low": SS.watch[sym]["low"]})
            _bg(_funding_squeeze_arm(session, sym, now))
            continue

# ================================================================ STREAM READERS
async def stream_reader():
    """miniTicker feed: price windows, q24, REV 1s series, trigger scan.
    Failover across WS_BASES; full reset on any reconnect."""
    idx, fails = 0, 0
    while not _shutdown.is_set():
        url = f"{WS_BASES[idx]}{WS_PATH}"
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20,
                                          max_queue=4096, open_timeout=15) as ws:
                print(f"[feed ] connected {url} — tick-driven scan")
                fails = 0
                async for msg in ws:
                    if _shutdown.is_set(): break
                    now = now_ts()
                    try: arr = json.loads(msg)
                    except Exception: continue
                    SS.msgs += 1
                    for item in arr:
                        sym = item.get("s") or ""
                        if not sym.endswith("USDT"): continue
                        px = float(item.get("c") or 0)
                        q24 = float(item.get("q") or 0)
                        if px <= 0: continue
                        prev_q = SS.q24.get(sym)
                        _record_tick(sym, px, q24, now)
                        if prev_q is not None:
                            _explosion_accumulate(sym, q24 - prev_q, px, now)
                    _check_stream_triggers(SESSION_M, now_ts())
        except asyncio.CancelledError: raise
        except Exception as e:
            fails += 1
            if fails >= 2: idx = (idx + 1) % len(WS_BASES); fails = 0; print(f"[feed ] flipping to {WS_BASES[idx]}")
            print(f"[feed ] down ({e}); retry 5s")
        finally:
            _ws_reset("ticker")
        if not _shutdown.is_set(): await asyncio.sleep(5)

async def trade_reader():
    """aggTrade feed — the 1s recorder evidence layer (bars_1s for the replay/
    autopsy DB; FOLLOWER's volume guard reads it too). Infra fixes carried:
      · staleness is ONE-SIDED (only past-stale >30s restarts; future-skewed
        host clocks can no longer cause an infinite reconnect loop)
      · subscription overshoot DEGRADES (top-by-q24 within the 1024 cap,
        logged once) instead of killing the process."""
    idx = 0
    capped_logged = False
    while not _shutdown.is_set():
        pending, subscribed = {}, set()
        request_id, next_sub = 0, 0.0
        try:
            async with websockets.connect(f"{WS_BASES[idx % len(WS_BASES)]}/ws", ping_interval=20,
                ping_timeout=20, max_queue=8192, open_timeout=15) as ws:
                print("[trades] aggregate-trade stream connected; warming pace windows")
                while not _shutdown.is_set():
                    now = now_ts()
                    if now >= next_sub:
                        inflight = {sym for syms, _ts in pending.values() for sym in syms}
                        remaining = sorted(set(SS.q24) - subscribed - inflight - STABLE_SYMBOLS,
                                           key=lambda s: -SS.q24.get(s, 0))
                        room = TRADE_SUB_LIMIT - len(subscribed) - len(inflight)
                        batch = remaining[:min(100, max(0, room))]
                        if remaining and room <= 0:
                            if not capped_logged:
                                print(f"[trades] universe >{TRADE_SUB_LIMIT} — degrading: "
                                      f"tracking top-{TRADE_SUB_LIMIT} by q24 (explosion/recorder scope)")
                                capped_logged = True
                        if batch:
                            request_id += 1
                            pending[request_id] = (batch, now)
                            await ws.send(json.dumps({"method": "SUBSCRIBE",
                                "params": [sym.lower() + "@aggTrade" for sym in batch], "id": request_id}))
                        next_sub = now + 2.0
                    if any(now - ts > 15 for _syms, ts in pending.values()):
                        raise ConnectionError("aggregate-trade subscription acknowledgement timed out")
                    try: msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError: continue
                    event = json.loads(msg)
                    if "code" in event: raise ConnectionError(f"subscription rejected: {event}")
                    if event.get("id") in pending:
                        batch, _sent = pending.pop(event["id"])
                        if event.get("result") is not None:
                            raise ConnectionError(f"unexpected subscription response: {event}")
                        subscribed.update(batch)
                        for sym in batch: pass   # started stamped on first print
                    elif event.get("e") == "aggTrade":
                        received = now_ts()
                        # ONE-SIDED staleness fix: only PAST-stale restarts the feed
                        if received - float(event["E"]) / 1000.0 > 30.0:
                            raise ConnectionError("stale aggregate-trade feed; restarting pace warmup")
                        _record_trade(event["s"], float(event.get("p") or 0),
                                      float(event.get("q") or 0), event.get("a") or 0, received)
        except asyncio.CancelledError: raise
        except Exception as e:
            print(f"[trades] down ({e}); retry 5s")
            idx += 1
        finally:
            _ws_reset("trades")
        if not _shutdown.is_set(): await asyncio.sleep(5)

# ═══════════════════════════════ END PART 2/4 ═══════════════════════════════
# Part 3/4 begins at: # ============ 15M/1H KLINE CACHE + LANES
# ================================================================ 15M/1H KLINE CACHE
_k15 = {}      # sym → {"bars": deque[(open_ms,o,h,l,c,q)] closed 15m, "hi24": px, "lo24": px}
_k1h = {}      # sym → deque[(open_ms,o,h,l,c,q)] closed 1h, 168 = 7d (SQUEEZE)
_k1h_fail = {}
_k15_seen = {} # sym → ts of last 15m fetch attempt (rotation order)
_k1h_seen = {} # sym → ts of last 1h fetch
def _cdir(bar): return 1 if bar[4] > bar[1] else (-1 if bar[4] < bar[1] else 0)

async def k_cache_refresher(session):
    """v4.8.2: CONTINUOUS rotation, stalest symbol first — the 300s cycle is
    gone. The 15m close is trigger food for SGRIND/SGREV/PFADE/DDv2/
    FAILBREAK/SQUEEZE/BURST, and a newly closed candle used to sit
    undiscovered for up to ~6 min (the break-trigger lanes skip stale
    setups, so slow discovery = lost trades). Worst-case staleness is now
    one sweep (~30-45s for the top-150). 1h candles (SQUEEZE only) ride the
    same loop on a slower K1H_REFRESH_SEC cadence."""
    last_note = 0.0
    while not _shutdown.is_set():
        try:
            syms = [s for s, _q in sorted(SS.q24.items(), key=lambda kv: -kv[1])[:K15_SYMS]]
            if not syms:
                await asyncio.sleep(1.0); continue
            syms.sort(key=lambda s: _k15_seen.get(s, 0.0))   # stalest first
            for sym in syms:
                if _shutdown.is_set(): break
                now = now_ts()
                try:
                    async with session.get(f"{FAPI}/fapi/v1/klines",
                        params={"symbol": sym, "interval": "15m", "limit": K15_LIMIT},
                        timeout=aiohttp.ClientTimeout(total=8)) as r:
                        if r.status == 200:
                            kl = await r.json()
                            if isinstance(kl, list) and len(kl) >= 40:
                                bars = deque(((int(k[0]), float(k[1]), float(k[2]), float(k[3]),
                                               float(k[4]), float(k[5])) for k in kl[:-1]), maxlen=K15_LIMIT)
                                _k15[sym] = {"bars": bars,
                                             "hi24": max(b[2] for b in bars),
                                             "lo24": min(b[3] for b in bars)}
                except Exception: pass
                _k15_seen[sym] = now
                if now - _k1h_seen.get(sym, 0.0) > K1H_REFRESH_SEC:
                    try:
                        async with session.get(f"{FAPI}/fapi/v1/klines",
                            params={"symbol": sym, "interval": "1h", "limit": K1H_LIMIT},
                            timeout=aiohttp.ClientTimeout(total=8)) as r:
                            if r.status == 200:
                                kl = await r.json()
                                if isinstance(kl, list) and len(kl) >= 40:
                                    _k1h[sym] = deque(((int(k[0]), float(k[1]), float(k[2]), float(k[3]),
                                                        float(k[4]), float(k[5])) for k in kl[:-1]), maxlen=K1H_LIMIT)
                    except Exception: pass
                    _k1h_seen[sym] = now
                await asyncio.sleep(K15_PACE_SEC)
            if _shutdown.is_set(): break
            if _k15 and now_ts() - last_note > 300.0:
                print(f"[{hms()}] [k15  ] rotating: {len(_k15)} symbols, oldest-first (continuous)")
                last_note = now_ts()
        except Exception as e:
            print(f"[warn ] k cache: {e!r}")
            await asyncio.sleep(5)

# ================================================================ DEEPDIP v2 (PAPER — the 2-green-candle relaunch)
_dd2_watch = {}
def _dd2_scan(session, now):
    """Latch ≥20% below rolling 24h high (measured at the low; expires 24h) →
    X owns velocity episodes → 2 consecutive positive 15m candles (close>open)
    → LONG at the 2nd close (PAPER). One trade per dump episode."""
    if not ENABLE_DEEPDIP_V2 or not IS_SCANNER: return
    for sym in list(_k15.keys()):
        px = stream_price(sym)
        if not px or px <= 0: continue
        cache = _k15[sym]; bars = list(cache["bars"])
        w = _dd2_watch.get(sym)
        if w:
            # latch expiry
            if now - w["low_ts"] > DD2_LATCH_HRS * 3600.0:
                del _dd2_watch[sym]; decide("expire", sym, "dd2_latch_expired", {}); continue
            # stop-of-record check on the live price (kill the thesis)
            if px <= w["low"] * (1 - DD2_STOP_BUF / 100.0):
                del _dd2_watch[sym]
                decide("skip", sym, "dd2_new_low", {"low": w["low"]}); continue
            # 2 consecutive positive candles on CLOSED bars
            last_open = bars[-1][0]
            if w.get("last_open") != last_open:
                w["last_open"] = last_open
                b1, b2 = bars[-2], bars[-1]
                if _cdir(b1) > 0 and _cdir(b2) > 0:
                    stop_px = w["low"] * (1 - DD2_STOP_BUF / 100.0)
                    dist = (px / stop_px - 1.0) * 100.0
                    if dist > DD2_MAX_STOP_DIST:
                        del _dd2_watch[sym]
                        decide("skip", sym, "dd2_stop_too_far", {"dist": round(dist, 2)}); continue
                    del _dd2_watch[sym]
                    print(f"[{hms()}] [sig  ] {sym} 🕳DEEPDIP v2: 2x green 15m off "
                          f"{w['low']:.6g} (latched -{w['drop']:.0f}%) → LONG")
                    _mark_signal(sym, "DEEPDIP_V2", now)
                    _signal(session, sym, "DEEPDIP_V2", "up", px, now,
                            episode_id=w["ep_id"])
            continue
        hi = cache.get("hi24")
        if hi and (px / hi - 1.0) * 100.0 <= -DD2_DROP_PCT:
            # X ownership is classified async at arm (5m klines, below)
            w = {"low": px, "low_ts": now, "ep_id": f"dd2|{sym}|{int(now)}",
                 "drop": -(px / hi - 1.0) * 100.0, "last_open": None}
            _dd2_watch[sym] = w
            print(f"[{hms()}] [watch] {sym} 🕳DEEPDIP v2 latch: -{w['drop']:.0f}% off 24h high")
            decide("watch", sym, "dd2_latched", {"low": px, "drop": round(w['drop'], 1)})
            _bg(_dd2_classify(session, sym, now, w))

async def _dd2_classify(session, sym, now, w):
    """Velocity routing: ≥18% within 45min (9 closed 5m bars) → X owns the
    episode; v2 stands down. One crash, one lane."""
    try:
        async with session.get(f"{FAPI}/fapi/v1/klines",
            params={"symbol": sym, "interval": "5m", "limit": DIPX_K5_LIMIT},
            timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200: return
            kl = await r.json()
        if not isinstance(kl, list) or len(kl) < DIPX_K5_LIMIT - 1: return
        px = stream_price(sym)
        if not px or px <= 0: return
        peak = max([float(k[2]) for k in kl[:-1]] + [px])
        drop = (1 - px / peak) * 100.0 if peak > 0 else 0.0
        if drop >= DIPX_DROP_PCT:
            if _dd2_watch.get(sym) is w:
                del _dd2_watch[sym]
                print(f"[{hms()}] [watch] {sym} DIP-X owns this episode (-{drop:.0f}%/45m) — v2 stands down")
                decide("skip", sym, "dd2_yields_to_x", {"drop": round(drop, 2)})
    except Exception as e:
        print(f"[{hms()}] [warn ] dd2 classify {sym}: {e!r}")

# ================================================================ DEEPDIP_X (unchanged)
_dipx_watch = {}; _dipx_cooldown = {}
def _dipx_scan(session, now):
    """≥18% within 45min (via k15-backcheck on closed bars) → stabilize 10m
    (no new low) → bounce ≥2% → LONG. LIVE. 2h cooldown."""
    if not ENABLE_DEEPDIP_X or not IS_SCANNER: return
    for sym in list(_k15.keys()):
        px = stream_price(sym)
        if not px or px <= 0: continue
        cache = _k15[sym]; bars = list(cache["bars"])
        w = _dipx_watch.get(sym)
        if w:
            if px < w["low"]: w["low"] = px; w["last_new_low"] = now
            bounce = (px / w["low"] - 1.0) * 100.0
            if bounce >= DIPX_BOUNCE_PCT and now - w["last_new_low"] >= DIPX_STABILIZE_SEC:
                del _dipx_watch[sym]; _dipx_cooldown[sym] = now
                print(f"[{hms()}] [sig  ] {sym} 🕳🕳DIP-X bounce +{bounce:.1f}% off {w['low']:.6g} (stabilized)")
                _mark_signal(sym, "DEEPDIP_X", now)
                _signal(session, sym, "DEEPDIP_X", "up", px, now)
            elif now - w["started"] > 4 * 3600:
                del _dipx_watch[sym]
            continue
        if _dipx_cooldown.get(sym, 0) and now - _dipx_cooldown[sym] < 7200.0: continue
        # velocity on closed 15m bars: 18% within the last 3 bars = 45min
        if len(bars) >= 3:
            peak = max(b[2] for b in bars[-3:])
            if peak > 0 and (px / peak - 1.0) * 100.0 <= -DIPX_DROP_PCT:
                _dipx_watch[sym] = {"low": px, "started": now, "last_new_low": now}
                print(f"[{hms()}] [watch] {sym} 🕳🕳DIP-X armed (≥{DIPX_DROP_PCT:.0f}% in 45m)")
                decide("watch", sym, "dipx_armed", {"low": px})

# ================================================================ SGRIND (continuation — v4.6.4/v4.7.2 behavior restored)
_sg_last_bar = {}
def _sg_scan(session, now):
    """4-6 consecutive same-direction 15m candles (>=2% close-to-close), then
    1 opposite CLOSED candle + price extending it → enter WITH the grind.
    v4.6.4 nuance kept: a 7+ candle streak is rejected. Forced SWING;
    flexible per-coin cooldown (v4.8)."""
    if not ENABLE_SGRIND or not IS_SCANNER: return
    for sym, cache in list(_k15.items()):
        bars = list(cache["bars"])
        if len(bars) < SG_MIN_CANDLES + 2: continue
        last_open = bars[-1][0]
        if _sg_last_bar.get(sym) == last_open: continue   # evaluate once per closed candle
        _sg_last_bar[sym] = last_open
        d = _cdir(bars[-2])                    # direction of the candle before the opposite one
        if d == 0 or _cdir(bars[-1]) != -d: continue    # need 1 closed OPPOSITE candle
        streak = 0; i = len(bars) - 2
        while i >= 0 and _cdir(bars[i]) == d and streak <= SG_MAX_CANDLES:
            streak += 1; i -= 1
        if not (SG_MIN_CANDLES <= streak <= SG_MAX_CANDLES): continue
        start = len(bars) - 1 - streak          # first streak bar index
        base = bars[start - 1][4] if start >= 1 else bars[start][1]
        grind = (bars[len(bars) - 2][4] / base - 1.0) * 100.0
        if d > 0 and grind < SG_MIN_STREAK_PCT: continue
        if d < 0 and grind > -SG_MIN_STREAK_PCT: continue
        px = stream_price(sym)
        if not px or px <= 0: continue
        # "1.5 candles": price must be extending the pullback beyond the
        # opposite candle's extreme
        if d > 0 and px > bars[-1][3]: continue     # up-grind: price below the down-candle's low
        if d < 0 and px < bars[-1][2]: continue     # down-grind: price above the up-candle's high
        direction = "up" if d > 0 else "down"
        print(f"[{hms()}] [sig  ] {sym} 🐌SGRIND {streak}x15m {grind:+.1f}% → pullback "
              f"{'LONG' if direction=='up' else 'SHORT'} (with trend)")
        _mark_signal(sym, "SGRIND", now)
        _signal(session, sym, "SGRIND", direction, px, now)

# ================================================================ SGREV (reversal — the v4.8.1 redesign, own A/B lane)
_sgrev_last_bar = {}; _sgrev_watch = {}
def _sgrev_scan(session, now):
    """SGREV — REVERSAL, not continuation: a grind of >=4 same-direction
    15m candles (any length) with a >=4% move from the first streak candle's
    OPEN to the streak's most extreme point, then ONE closed opposite candle
    → during the NEXT candle, a live break of that opposite candle's extreme
    enters AGAINST the original grind (the turn, not the resumption).
    Guards: the watch lives only for the candle after the opposite one; it
    dies if price crosses back past the opposite candle's other extreme
    (grind resuming); a break that already happened before discovery is
    skipped, never chased (k15 cache latency). One setup per grind; flexible
    per-coin cooldown after each trade."""
    if not ENABLE_SGRIND or not IS_SCANNER: return
    for sym, cache in list(_k15.items()):
        bars = list(cache["bars"])
        if len(bars) < SGREV_MIN_CANDLES + 1: continue
        px = stream_price(sym)
        w = _sgrev_watch.get(sym)
        if w:
            if w["c1_open"] != bars[-1][0]:
                del _sgrev_watch[sym]                 # cache advanced — window over
                w = None
            elif now * 1000.0 >= w["c2_end_ms"]:
                del _sgrev_watch[sym]; decide("expire", sym, "sgrev_window_closed", {}); w = None
            elif not px or px <= 0:
                pass
            elif (w["trade_dir"] == "down" and px > w["kill_level"]) or \
                 (w["trade_dir"] == "up" and px < w["kill_level"]):
                del _sgrev_watch[sym]; decide("skip", sym, "sgrev_reversal_failed", {}); w = None
            elif (w["trade_dir"] == "down" and px < w["level"]) or \
                 (w["trade_dir"] == "up" and px > w["level"]):
                del _sgrev_watch[sym]
                print(f"[{hms()}] [sig  ] {sym} 🔄SGREV reversal: {w['streak']}x15m grind "
                      f"{w['move']:+.1f}% broke the {'low' if w['trade_dir'] == 'down' else 'high'} "
                      f"{w['level']:.6g} → {'SHORT' if w['trade_dir'] == 'down' else 'LONG'} (against the grind)")
                _mark_signal(sym, "SGREV", now)
                _signal(session, sym, "SGREV", w["trade_dir"], px, now)
                w = None
            continue                                  # while watching, do not re-arm
        # arm on a newly closed candle: it must be OPPOSITE to a >=4 grind
        last_open = bars[-1][0]
        if _sgrev_last_bar.get(sym) == last_open: continue
        _sgrev_last_bar[sym] = last_open
        d = _cdir(bars[-1])                           # just-closed opposite candle
        if d == 0: continue
        streak = 0; i = len(bars) - 2                 # count the grind before it
        while i >= 0 and _cdir(bars[i]) == -d:
            streak += 1; i -= 1
        if streak < SGREV_MIN_CANDLES: continue       # 4 or more — no upper limit
        start = i + 1                                 # first streak bar index
        base = bars[start][1]                         # first streak candle's OPEN
        if base <= 0: continue
        if d < 0:      # up-grind exhausted; opposite candle red → SHORT setup
            extreme = max(b[2] for b in bars[start:len(bars) - 1])
            move = (extreme / base - 1.0) * 100.0
            if move < SGREV_MIN_STREAK_PCT: continue
            level, kill_level, trade_dir = bars[-1][3], bars[-1][2], "down"
        else:          # down-grind exhausted; opposite candle green → LONG setup
            extreme = min(b[3] for b in bars[start:len(bars) - 1])
            move = (extreme / base - 1.0) * 100.0
            if move > -SGREV_MIN_STREAK_PCT: continue
            level, kill_level, trade_dir = bars[-1][2], bars[-1][3], "up"
        if px and ((trade_dir == "down" and px <= level) or (trade_dir == "up" and px >= level)):
            decide("skip", sym, "sgrev_stale_break", {"move": round(move, 1)}); continue
        _sgrev_watch[sym] = {"c1_open": bars[-1][0],
                             "c2_end_ms": bars[-1][0] + 2 * 15 * 60 * 1000,
                             "level": level, "kill_level": kill_level,
                             "trade_dir": trade_dir, "streak": streak, "move": round(move, 1)}
        print(f"[{hms()}] [watch] {sym} 🔄SGREV reversal armed: {streak}x15m grind {move:+.1f}% "
              f"→ live break of {'low' if trade_dir == 'down' else 'high'} {level:.6g} "
              f"→ {'SHORT' if trade_dir == 'down' else 'LONG'} (against the grind)")
        decide("watch", sym, "sgrev_reversal_armed",
               {"streak": streak, "move": round(move, 1), "level": level})

# ================================================================ BURST (PAPER — intrabar momentum continuation)
_burst_watch = {}; _burst_cooldown = {}
def _c2_open(sym, c2_open_ms):
    """Candle-2 open ≈ the first retained live tick at/after the candle-2
    boundary (px_windows is 2s-sampled → within ~2s of the true open)."""
    dq = SS.px_windows.get(sym)
    if not dq: return None
    t0 = c2_open_ms / 1000.0
    for t, p in dq:
        if t >= t0: return p
    return None

def _burst_scan(session, now):
    """BURST: candle-1 15m body ≥±5% (close vs open) → candle-2 opens
    with the move (gap ≤2% beyond candle-1 close) → LIVE tick breaking
    candle-1's HIGH while candle-2 runs green → LONG; mirror on the LOW while
    red → SHORT. Intrabar by design: entry at the break moment, no candle-2
    close wait. Guards: candle-2 color must agree at the break; a level ALREADY
    broken when the setup is first seen is skipped (no chasing a stale break).
    Latency note: a qualifying candle-1 is discovered 0-6 min into candle 2
    (k15 cache refreshes every 300s) — only breaks that occur after discovery
    fire. One trade per burst; 2h per-symbol cooldown."""
    if not ENABLE_BURST or not IS_SCANNER: return
    for sym, cache in list(_k15.items()):
        bars = list(cache["bars"])
        if len(bars) < 2: continue
        c1 = bars[-1]                      # last CLOSED 15m candle
        c2_open_ms = c1[0] + 15 * 60 * 1000
        c2_end_ms = c2_open_ms + 15 * 60 * 1000
        w = _burst_watch.get(sym)
        if w:
            if w["c1_open"] != c1[0]:
                del _burst_watch[sym]      # cache advanced past this burst
                w = None
            elif now * 1000.0 >= c2_end_ms:
                del _burst_watch[sym]; decide("expire", sym, "burst_candle_closed", {}); continue
        if w is None:
            if _burst_cooldown.get(sym, 0) and now - _burst_cooldown[sym] < BURST_COOLDOWN_SEC:
                continue
            if now * 1000.0 >= c2_end_ms: continue       # candle 2 already over
            body = (c1[4] / c1[1] - 1.0) * 100.0 if c1[1] > 0 else 0.0
            if abs(body) < BURST_BODY_PCT: continue
            d = 1 if body > 0 else -1
            open2 = _c2_open(sym, c2_open_ms)
            if open2 is None or open2 <= 0: continue     # no candle-2 boundary tick yet
            gap = (open2 / c1[4] - 1.0) * 100.0 * d     # gap WITH the move (signed)
            if gap > BURST_GAP_MAX_PCT:
                decide("skip", sym, "burst_gap_spent", {"body": round(body, 1),
                                                        "gap": round(gap, 2)}); continue
            level = c1[2] if d > 0 else c1[3]            # candle-1 high (long) / low (short)
            px = stream_price(sym)
            if px and ((d > 0 and px > level) or (d < 0 and px < level)):
                decide("skip", sym, "burst_stale_break", {"body": round(body, 1)}); continue
            _burst_watch[sym] = {"c1_open": c1[0], "dir": d, "level": level,
                                 "open2": open2, "body": body}
            side = "high" if d > 0 else "low"
            print(f"[{hms()}] [watch] {sym} 🔆BURST armed: candle-1 body {body:+.1f}% → "
                  f"live break of candle-1 {side} {level:.6g} triggers "
                  f"{'LONG' if d > 0 else 'SHORT'}")
            decide("watch", sym, "burst_armed", {"body": round(body, 1), "level": level})
            continue
        # watching for the live cross
        px = stream_price(sym)
        if not px or px <= 0: continue
        broke = (w["dir"] > 0 and px > w["level"]) or (w["dir"] < 0 and px < w["level"])
        if not broke: continue
        color_ok = (w["dir"] > 0 and px > w["open2"]) or (w["dir"] < 0 and px < w["open2"])
        if not color_ok: continue                          # level broke against candle-2 color
        del _burst_watch[sym]; _burst_cooldown[sym] = now
        direction = "up" if w["dir"] > 0 else "down"
        side = "high" if w["dir"] > 0 else "low"
        print(f"[{hms()}] [sig  ] {sym} 🔆BURST: candle-2 broke candle-1 {side} "
              f"{w['level']:.6g} (body {w['body']:+.1f}%) → {'LONG' if direction == 'up' else 'SHORT'}")
        _mark_signal(sym, "BURST", now)
        _signal(session, sym, "BURST", direction, px, now)

# ================================================================ PFADE (re-long leg OI/funding-CONFIRMED)
_pf_relong = {}
def _pf_scan(session, now):
    """SHORT: ≥20% over 24h low → first 15m close below prior 15m low (SCALP,
    half). Re-long: 2x 15m no-new-low AND (OI Δ≤-3% since trigger OR funding
    ≤-0.10%) → LONG (SWING, full). Confirmed re-long per v4.7 spec. Flexible cd (v4.8)."""
    if not ENABLE_PFADE or not IS_SCANNER: return
    for sym, cache in list(_k15.items()):
        bars = list(cache["bars"])
        if len(bars) < 3: continue
        px = stream_price(sym)
        if not px or px <= 0: continue
        rl = _pf_relong.get(sym)
        if rl:
            # housekeeping: a re-long that never confirms is dropped after 8h
            if now - rl.get("trigger_ts", now) > 8 * 3600.0:
                del _pf_relong[sym]; decide("expire", sym, "pf_relong_stale", {}); continue
            # confirmed re-long: DEFERS while the symbol is busy (usually our
            # own short leg, which holds up to 60m) — v4.7.0 consumed the
            # episode first and symbol_busy threw it away
            if rl.get("confirmed") and rl["ok"] >= PF_STAB_CANDLES:
                if symbol_busy(sym): continue          # retry next poll (3s)
                del _pf_relong[sym]
                print(f"[{hms()}] [sig  ] {sym} 🚀PFADE re-long CONFIRMED "
                      f"({rl['confirm_reason']}) after {PF_STAB_CANDLES}x15m no-new-low")
                _mark_signal(sym, "PFADE", now)
                _signal(session, sym, "PFADE", "up", px, now)
                continue
            if rl.get("last_open") == bars[-1][0]: continue
            rl["last_open"] = bars[-1][0]
            lo = bars[-1][3]
            if lo <= rl["trigger_low"]:
                rl["trigger_low"] = lo; rl["ok"] = 0
            else:
                rl["ok"] += 1
            if rl["ok"] >= PF_STAB_CANDLES and not rl.get("confirmed"):
                # confirmation fetch (async-ish: mark pending, fetch in bg)
                rl["confirming"] = True
                _bg(_pf_confirm_relong(session, sym, now, rl))
            continue
        lo24 = min(cache.get("lo24") or 0, px)
        if lo24 <= 0: continue
        run = (px / lo24 - 1.0) * 100.0
        if run < PF_MIN_RUN_PCT: continue
        if bars[-1][4] < bars[-2][3]:
            print(f"[{hms()}] [sig  ] {sym} 🚀PFADE short: +{run:.0f}% over 24h low, "
                  f"15m close {bars[-1][4]:.6g} < prior low {bars[-2][3]:.6g}")
            _mark_signal(sym, "PFADE", now)
            _signal(session, sym, "PFADE", "down", px, now)
            _pf_relong[sym] = {"trigger_low": min(bars[-1][3], px), "ok": 0,
                               "last_open": bars[-1][0], "oi_at_trigger": None,
                               "trigger_ts": now}

async def _pf_confirm_relong(session, sym, now, rl):
    """Re-long confirmation (either passes → confirmed): OI down ≥3% since the
    short trigger (longs flushed) OR funding ≤ -0.10% (crowd already paying)."""
    try:
        oi = await fetch_oi_now(session, sym)
        if rl.get("oi_at_trigger") is None and oi:
            # first confirmation attempt: store trigger OI if we never got it
            rl["oi_at_trigger"] = oi
        if oi and rl.get("oi_at_trigger"):
            drop = (oi / rl["oi_at_trigger"] - 1.0) * 100.0
            if drop <= -PF_RELONG_OI_DROP:
                rl["confirmed"] = True; rl["confirm_reason"] = f"OI {drop:+.1f}%"
                return
        fr, _nf = await fetch_funding_rate(session, sym)
        if fr is not None and fr <= PF_RELONG_FUNDING:
            rl["confirmed"] = True; rl["confirm_reason"] = f"funding {fr*100:.3f}%"
            return
        rl["confirming"] = False   # stay unconfirmed; re-checked on next candle
    except Exception as e:
        print(f"[{hms()}] [warn ] pf confirm {sym}: {e!r}")
        rl["confirming"] = False

# ================================================================ FAILBREAK (PAPER)
_fb_state = {}
def _fb_scan(session, now):
    """15m close ≥0.3% above rolling 12h high with breakout volume <2× the
    50-candle median → close back below the 12h high within 2 candles →
    SHORT (PAPER). Stop = break high +0.15% (position-level, recorded in
    payload). Mirror long coded, DISABLED until co-fire audit."""
    if not ENABLE_FAILBREAK or not IS_SCANNER: return
    for sym, cache in list(_k15.items()):
        bars = list(cache["bars"])
        if len(bars) < 53: continue
        px = stream_price(sym)
        if not px or px <= 0: continue
        st = _fb_state.get(sym)
        if st:
            if now - st["break_ts"] > 4 * 3600:
                del _fb_state[sym]; continue
            last_open = bars[-1][0]
            if st.get("last_open") == last_open: continue
            st["last_open"] = last_open
            c1 = bars[-1]
            if st["closes"] == 0 and c1[4] < st["hi12"]:
                # failed within the 2-candle window → SHORT (PAPER)
                del _fb_state[sym]; _fb_cooldown[sym] = now
                print(f"[{hms()}] [sig  ] {sym} 🪤FAILBREAK: break to {st['break_px']:.6g} "
                      f"(vol {st['vol_x']:.1f}x) failed → SHORT")
                _mark_signal(sym, "FAILBREAK", now)
                _signal(session, sym, "FAILBREAK", "down", px, now,
                        payload={"stop_px": st["break_px"] * 1.0015})
            elif st["closes"] >= 2:
                del _fb_state[sym]      # held above — real breakout, stand down
            else:
                st["closes"] += 1
            continue
        # prior-12h high EXCLUDES the breakout candle itself: v4.7.0 compared
        # the close against a max that included the candle's own high, which
        # close ≤ high can never satisfy — the lane could not arm at all
        hi12 = max(b[2] for b in bars[-(FAILBREAK_LOOKBACK_H * 4 + 1):-1])
        c1 = bars[-1]
        if hi12 > 0 and c1[4] >= hi12 * 1.003:
            med_q = statistics.median([b[5] for b in bars[-50:]])
            vol_x = c1[5] / med_q if med_q > 0 else 0.0
            if vol_x < 2.0:            # weak break only
                _fb_state[sym] = {"hi12": hi12, "break_px": c1[4], "break_ts": now,
                                  "vol_x": vol_x, "closes": 0, "last_open": None}
                print(f"[{hms()}] [watch] {sym} 🪤FAILBREAK armed: broke 12h high on {vol_x:.1f}x volume")
                decide("watch", sym, "failbreak_armed", {"hi": hi12, "vol_x": round(vol_x, 2)})

# ================================================================ SQUEEZE (PAPER)
_sq_armed = {}
def _sq_scan(session, now):
    """12×1h range width ≤20th percentile of trailing 7d AND ≤1.5×ATR(1h) →
    armed. 15m close outside the range with 15m volume ≥2× the 15m median →
    enter (PAPER). One trade per squeeze episode (episode-scoped: the armed
    state is consumed on fire; no extra time cooldown, per the roster)."""
    if not ENABLE_SQUEEZE or not IS_SCANNER: return
    for sym, cache in list(_k15.items()):
        bars15 = list(cache["bars"])
        bars1h = _k1h.get(sym)
        if not bars1h or len(bars1h) < 60: continue
        h1h = list(bars1h)
        px = stream_price(sym)
        if not px or px <= 0: continue
        st = _sq_armed.get(sym)
        if st:
            last15 = bars15[-1]
            last_open = last15[0]
            if st.get("last_open") == last_open: continue
            st["last_open"] = last_open
            # like-for-like: 15m breakout volume vs 15m median (v4.7.0 compared
            # a 15m bar against a median of 1h bars — ~4-8x too strict)
            med_q = statistics.median([b[5] for b in bars15[-50:]])
            vol_ok = last15[5] >= 2.0 * med_q if med_q > 0 else False
            broke_up = last15[4] > st["hi"]
            broke_dn = last15[4] < st["lo"]
            if vol_ok and (broke_up or broke_dn):
                del _sq_armed[sym]
                d = "up" if broke_up else "down"
                print(f"[{hms()}] [sig  ] {sym} 🧨SQUEEZE: {st['width_pct']:.1f}% 12h range "
                      f"broke {'UP' if broke_up else 'DOWN'} on {last15[5]/med_q:.1f}x volume")
                _mark_signal(sym, "SQUEEZE", now)
                _signal(session, sym, "SQUEEZE", d, px, now,
                        payload={"stop_px": st["mid"], "range": [st["lo"], st["hi"]]})
            elif now - st["ts"] > 24 * 3600:
                del _sq_armed[sym]
            continue
        look = h1h[-SQUEEZE_LOOK_CANDLES:]
        hi = max(b[2] for b in look); lo = min(b[3] for b in look)
        mid = (hi + lo) / 2.0
        width_pct = (hi / lo - 1.0) * 100.0 if lo > 0 else 999.0
        atrs = [(b[2] - b[3]) for b in h1h[-20:]]
        atr = sum(atrs) / len(atrs) if atrs else 0.0
        if width_pct <= 0 or width_pct > SQUEEZE_ATR_MULT * (atr / lo * 100.0 if lo > 0 else 999):
            continue
        # 20th percentile of trailing 7d widths (non-overlapping 12h windows,
        # EXCLUDING the live window — v4.7.0 let the tested window dilute its
        # own threshold)
        widths = []
        for i in range(0, len(h1h) - 2 * SQUEEZE_LOOK_CANDLES + 1, SQUEEZE_LOOK_CANDLES):
            w_ = h1h[i:i + SQUEEZE_LOOK_CANDLES]
            wl = max(b[2] for b in w_); ws = min(b[3] for b in w_)
            if ws > 0: widths.append((wl / ws - 1.0) * 100.0)
        if len(widths) < 5: continue
        widths.sort()
        idx20 = max(0, int(len(widths) * SQUEEZE_PCTILE / 100.0) - 1)
        if width_pct <= widths[idx20]:
            _sq_armed[sym] = {"hi": hi, "lo": lo, "mid": mid, "width_pct": width_pct,
                              "ts": now, "last_open": None}
            print(f"[{hms()}] [watch] {sym} 🧨SQUEEZE armed: {width_pct:.1f}% 12h width "
                  f"(≤{SQUEEZE_PCTILE}th pct) → brackets {lo:.6g}/{hi:.6g}")
            decide("watch", sym, "squeeze_armed", {"width": round(width_pct, 2)})

# ================================================================ FOLLOWER (PAPER, pre-list)
_follower_prelist = []; _follower_btc = None; _follower_cand = {}
async def _follower_build_prelist(session):
    """Liquid-alt pre-list: top-N by 24h quote volume, excludes BTC + stables.
    Built once at startup; refreshed hourly."""
    global _follower_prelist
    while not _shutdown.is_set():
        try:
            syms = [s for s, _q in sorted(SS.q24.items(), key=lambda kv: -kv[1])[:FOLLOW_PRELIST_N + 5]]
            _follower_prelist = [s for s in syms if s not in STABLE_SYMBOLS][:FOLLOW_PRELIST_N]
            print(f"[{hms()}] [follow] pre-list: {len(_follower_prelist)} liquid alts")
        except Exception as e: print(f"[warn ] prelist: {e!r}")
        await asyncio.sleep(3600.0)
def _follower_scan(session, now):
    """|BTC| ≥1.2%/5min → rank pre-list laggards (≤0.25× BTC move) → biggest
    laggard with own volume ≤3× median → 60s momentum ≥0.15% with BTC → enter
    WITH BTC (PAPER). Systemic filter NOT applied (this lane trades it)."""
    global _follower_btc
    if not ENABLE_FOLLOWER or not IS_SCANNER: return
    if not _follower_prelist: return
    b_now = stream_price("BTCUSDT")
    b_then = _px_ago("BTCUSDT", now, FOLLOW_BTC_WIN)
    if not b_now or not b_then or b_then <= 0: return
    bmv = (b_now / b_then - 1.0) * 100.0
    if abs(bmv) < FOLLOW_BTC_MOVE_PCT: return
    if _follower_btc and now - _follower_btc[0] < 300.0: return   # one wave per 5 min
    direction = "up" if bmv > 0 else "down"
    best, best_gap = None, 0.0
    for sym in _follower_prelist:
        a_now = stream_price(sym); a_then = _px_ago(sym, now, FOLLOW_BTC_WIN)
        if not a_now or not a_then or a_then <= 0: continue
        amv = (a_now / a_then - 1.0) * 100.0
        expected = bmv * FOLLOW_LAG_MAX
        if direction == "up" and amv >= expected: continue
        if direction == "down" and amv <= expected: continue
        gap = abs(bmv - amv)
        if gap > best_gap: best, best_gap = sym, gap
    if not best: return
    # own-volume guard (spec: own volume ≤3x median — v4.7.0 never checked
    # it): recent aggTrade pace vs the q24-implied $/s baseline. An
    # idiosyncratic spike means the alt is moving on its own news, not lagging.
    st = SS.t1s.get(best)
    if not st or not st["started"] or now - SS.trade_last_msg > 5.0: return
    pace = sum(r[5] for r in st["dq"] if r[0] >= int(now) - 60) / 60.0
    if pace > FOLLOW_VOL_MAX_X * max(SS.q24.get(best, 0) / 86400.0, 1.0): return
    dq = SS.px_windows.get(best)
    if not dq or len(dq) < 2: return
    _follower_btc = (now, bmv)
    _follower_cand[best] = {"dir": direction, "btc_mv": bmv, "ts": now}
    print(f"[{hms()}] [watch] {best} 🐦FOLLOWER candidate: BTC {bmv:+.1f}%/5m, "
          f"alt lagged (gap {best_gap:.1f}%) → awaiting 60s confirmation")
    decide("watch", best, "follower_candidate", {"btc_mv": round(bmv, 2), "gap": round(best_gap, 2)})

def _follower_confirm(session, now):
    """60s momentum ≥0.15% in BTC's direction → enter (PAPER)."""
    for sym, c in list(_follower_cand.items()):
        if now - c["ts"] > 180.0:
            del _follower_cand[sym]; continue
        px = stream_price(sym); p60 = _px_ago(sym, now, 60.0)
        if not px or not p60 or p60 <= 0: continue
        mv = (px / p60 - 1.0) * 100.0
        ok = (c["dir"] == "up" and mv >= FOLLOW_CONFIRM_PCT) or \
             (c["dir"] == "down" and mv <= -FOLLOW_CONFIRM_PCT)
        if ok:
            del _follower_cand[sym]
            sign = 1.0 if c["dir"] == "up" else -1.0
            tp = px * (1.0 + sign * FOLLOW_CAPTURE * abs(c["btc_mv"]) / 100.0)
            print(f"[{hms()}] [sig  ] {sym} 🐦FOLLOWER: confirmed {c['dir']} "
                  f"(60s {mv:+.2f}%, BTC {c['btc_mv']:+.1f}%) → TP {tp:.6g}")
            _mark_signal(sym, "FOLLOWER", now)
            _signal(session, sym, "FOLLOWER", c["dir"], px, now,
                    payload={"btc_mv": c["btc_mv"]}, episode_id=f"flw|{tp}")

# ================================================================ DAYOPEN_RECLAIM (LOG-ONLY)
_dayopen_logged = {}
def _dayopen_scan(now):
    """First 15m close back above the UTC 00:00 open after trading below it."""
    if not ENABLE_DAYOPEN or not IS_SCANNER: return
    today = today_utc()
    for k in [k for k in _dayopen_logged if k[1] != today]:   # prune prior days
        del _dayopen_logged[k]
    for sym, cache in list(_k15.items()):
        bars = list(cache["bars"])
        if len(bars) < 8: continue
        opens = [b for b in bars if datetime.fromtimestamp(b[0] / 1000.0, tz=timezone.utc).day
                 == datetime.now(timezone.utc).day]
        if len(opens) < 2: continue
        day_open = opens[0][1]
        px = stream_price(sym)
        if not px: continue
        was_below = any(b[4] < day_open for b in opens[:-1])
        closed_above = opens[-1][4] > day_open
        key = (sym, today)
        if was_below and closed_above and key not in _dayopen_logged:
            _dayopen_logged[key] = True
            decide("log", sym, "dayopen_reclaim", {"day_open": day_open, "px": px})

# ================================================================ ADMISSION + OPENING
def _final_admission(sym, etype, direction, px, paper=False):
    """Called AFTER the last await, immediately before insertion. Zero awaits.
    Paper lanes: capacity/caps/dip-budget are NOT consumed by paper positions,
    but paper rows still respect symbol_busy to avoid double-signals."""
    if kill_now() or _shutdown.is_set(): return None
    if sym in STABLE_SYMBOLS: return None
    if symbol_busy(sym): return None
    if not paper:
        if len(open_positions()) >= MAX_CONCURRENT:
            return None
        if etype in DIP_LANES and not _dip_budget_ok(): return None
    if etype == "REV":
        brain = _brain_snapshot()
        _brain_cache["ts"] = 0.0
        if (brain["exh"].get(sym) or 0) >= VETO_EXH_MAX: return None
    if etype == "FUNDING_SQZ": return "SCALP"
    if etype == "DEEPDIP_X":   return "SCALP"
    if etype == "DEEPDIP_V2":  return "SWING"
    if etype == "FAILBREAK":   return "SCALP"
    if etype == "SQUEEZE":     return "SWING"
    if etype == "FOLLOWER":    return "SCALP"
    if etype == "PFADE":       return "SCALP" if direction == "down" else "SWING"
    if etype in ("SGRIND", "SGREV"): return "SWING"   # continuation v4.6.4 + reversal v4.8.1
    return "SCALP" if _brain_agrees(sym, direction) else "SWING"

LANE_SIZE_MULT = {
    # v4.8: only FSQZ + DIP-X stay half; every other lane trades FULL
    "FUNDING_SQZ": 0.5, "DEEPDIP_X": 0.5,
}

def _emit_signal(sym, etype, direction, sig_px, now, **payload):
    """Scanner→trader transport (split mode only). 'both' opens in-process."""
    if NEXUS_ROLE == "both": return False
    try:
        c = sdb()
        c.execute("INSERT INTO signals(ts,lane,symbol,direction,sig_px,payload,state,rules_ver) "
                  "VALUES(?,?,?,?,?,?,'new',?)",
                  (now, etype, sym, direction, sig_px,
                   json.dumps(payload, default=str), RULES_VER))
        c.commit(); c.close()
        decide("signal", sym, "emitted", {"lane": etype, "dir": direction})
        return True
    except Exception as e:
        print(f"[{hms()}] [warn ] signal emit {sym}: {e!r}"); return False

def _signal(session, sym, etype, direction, px, now, payload=None,
            paper=False, episode_id=None):
    """Single admission point. Scanner-role split: emit to the signals table
    and return (trader picks it up). 'both': confirm → open in-process."""
    if kill_now() or _shutdown.is_set(): return
    if sym in STABLE_SYMBOLS: return
    if symbol_busy(sym): decide("skip", sym, "symbol_busy", {"etype": etype}); return
    if not _lane_cooldown_ok(sym, etype, now):
        decide("skip", sym, "lane_cooldown", {"etype": etype}); return
    brain = _brain_snapshot()
    if etype == "REV" and (brain["exh"].get(sym) or 0) >= VETO_EXH_MAX:
        decide("veto", sym, "exhaustion", {"exh": brain["exh"].get(sym)}); return
    # v4.8: every lane trades REAL (paper plumbing kept for tests only)
    if NEXUS_ROLE == "scanner":
        if _emit_signal(sym, etype, direction, px, now,
                        paper=paper, episode_id=episode_id, **(payload or {})):
            return
        # emit failed → fall through to in-process (resilience)
    if len(open_positions()) >= MAX_CONCURRENT and not paper:
        key = f"{sym}|{etype}"
        if key not in SS.waitroom:
            SS.waitroom[key] = {"symbol": sym, "etype": etype, "direction": direction,
                                "px": px, "ts": now, "paper": paper,
                                "payload": payload, "episode_id": episode_id}
            decide("wait", sym, "book_full", {"etype": etype})
        return
    spawn_entry(_confirm_and_open(session, etype, sym, direction, px,
                                  paper=paper, episode_id=episode_id, payload=payload))

async def _confirm_and_open(session, etype, sym, direction, sig_px,
                            paper=False, episode_id=None, payload=None):
    key = (sym, etype)
    if key in SS.confirming: return
    SS.confirming.add(key)
    try:
        if symbol_busy(sym):
            decide("reject", sym, "busy_final", {"etype": etype}); return
        if kill_now() or _shutdown.is_set():
            decide("skip", sym, "killed", {"etype": etype}); return
        _imb, spread = await fetch_book_data(session, sym)
        px = stream_price(sym) or await live_price(session, sym)
        if not px or px <= 0:
            decide("reject", sym, "no_live_price", {"etype": etype}); return
        if abs(px / sig_px - 1.0) * 100.0 > CHASE_MAX_PCT and etype not in ("FOLLOWER",):
            decide("skip", sym, "chase", {"etype": etype}); return
        tier = _final_admission(sym, etype, direction, px, paper=paper)
        if tier is None:
            decide("reject", sym, "final_admission", {"etype": etype, "paper": paper}); return
        size_mult = LANE_SIZE_MULT.get(etype, 1.0)
        open_position(etype, tier, sym, direction, px, spread=spread,
                      trigger_ts=now_ts(), size_mult=size_mult,
                      paper=paper, episode_id=episode_id,
                      sl_price_override=(payload or {}).get("stop_px"),
                      btc_regime=_regime_tag())
    except Exception as e: print(f"[{hms()}] [err  ] confirm {sym}: {e!r}")
    finally: SS.confirming.discard(key)

# ================================================================ OPEN/CLOSE
def open_position(lane, tier, symbol, direction, entry, spread=None, trigger_ts=None,
                  sl_pct=None, size_mult=1.0, paper=False, episode_id=None,
                  sl_price_override=None, btc_regime=None):
    if kill_now() or _shutdown.is_set():
        decide("skip", symbol, "killed_or_shutdown", {}); return False
    if not math.isfinite(entry) or entry <= 0:
        print(f"[skip ] {symbol}: bad entry"); return False
    d = (direction or "").strip().lower()
    if d in ("up", "long"): direction = "up"
    elif d in ("down", "short"): direction = "down"
    else:
        print(f"[skip ] {symbol}: bad direction {direction!r}"); return False
    sign = 1.0 if direction == "up" else -1.0
    sl_pct = clamp(sl_pct if sl_pct is not None else SL_PCT, 0.5, 100.0)
    notional = notional_for(symbol, spread, size_mult)
    now = trigger_ts or time.time()
    if sl_price_override is not None and math.isfinite(sl_price_override):
        sl = sl_price_override
        sl_pct = round(abs(sl / entry - 1.0) * 100.0, 3)
    else:
        sl = entry * (1 - sign * sl_pct / 100.0)
    c = sdb()
    try:
        c.execute("BEGIN IMMEDIATE")
        # atomic REAL-capacity checks (paper rows never consume)
        n = c.execute("SELECT COUNT(*) FROM shadow_positions WHERE status='OPEN' AND paper=0").fetchone()[0]
        if not paper and n >= MAX_CONCURRENT:
            c.rollback(); decide("reject", symbol, "capacity_atomic", {}); return False
        if not paper and lane in DIP_LANES:
            dips = c.execute("SELECT COUNT(*) FROM shadow_positions WHERE status='OPEN' AND paper=0 AND lane IN "
                             + "(".join("?" * len(DIP_LANES)), DIP_LANES).fetchone()[0]
            if dips >= DIP_BUDGET:
                c.rollback(); decide("reject", symbol, "dip_budget_atomic", {}); return False
        # duplicate guard: REAL rows only (a paper open must be able to sit
        # alongside a real position on the same symbol — smoke-tested); real
        # opens are blocked by existing real rows
        if not paper and c.execute("SELECT 1 FROM shadow_positions WHERE symbol=? AND status='OPEN' AND paper=0",
                                   (symbol,)).fetchone():
            c.rollback(); decide("reject", symbol, "duplicate_atomic", {}); return False
        # (v4.7.0 shipped 16 '?'+literal = 17 values against 15 columns with
        # 14 params — every open raised OperationalError; arity fixed here and
        # locked by the smoke test)
        c.execute("""INSERT INTO shadow_positions(lane,tier,symbol,direction,entry_ts,entry_price,notional,
                     sl_price,sl_pct,spread_pct,opened_day,status,paper,entry_ver,episode_id,regime)
                     VALUES(?,?,?,?,?,?,?,?,?,?,?,'OPEN',?,?,?,?)""",
                  (lane, tier, symbol, direction, now, entry, notional, sl, sl_pct, spread,
                   today_utc(), 1 if paper else 0, RULES_VER, episode_id, btc_regime))
        c.commit()
        pfx = "PAPER " if paper else ""
        sp_s = f"{spread:.3f}%" if spread else "default"
        sz = f" ×{size_mult:g}" if size_mult != 1.0 else ""
        hold = SCALP_HOLD_MIN if tier == "SCALP" else SWING_HOLD_MIN
        print(f"[OPEN ] [{pfx}{lane}/{tier}] {symbol} {direction.upper()} @{entry:.6g} "
              f"notional ${notional:.0f}{sz} SL {sl:.6g} (-{sl_pct:.1f}% HARD) hold {hold:.0f}m spread {sp_s}")
        tag = {"REV": "⚡", "FUNDING_SQZ": "🧲", "DEEPDIP_X": "🕳🕳", "DEEPDIP_V2": "🕳",
               "SGRIND": "🐌", "SGREV": "🔄", "PFADE": "🚀", "FAILBREAK": "🪤", "SQUEEZE": "🧨",
               "FOLLOWER": "🐦", "EXPLOSION": "💥"}.get(lane, lane)
        notify(SESSION_M,
               f"{tag} {'PAPER · ' if paper else ''}{tier} — SHADOW OPEN {symbol} {direction.upper()}\n"
               f"Entry {entry:.6g} · SL {sl:.6g} (-{sl_pct:.1f}% HARD)\n"
               f"Notional ${notional:.0f}{sz} · ladder {LADDER_ARM_PCT}/{LADDER_GAP}→{LADDER_GAP_AT}/{LADDER_GAP_BIG} · "
               f"cut -{EARLY_CUT_PCT}% · {'30m/60m' if tier=='SCALP' else '60m'}\n"
               f"{'Open ' + str(len(open_positions())) + '/' + str(MAX_CONCURRENT) + ' · ' if not paper else ''}"
               f"Day PnL {daily_realized(today_utc()):+.2f}\n{make_trade_link(symbol)}")
        decide("open", symbol, "opened", {"lane": lane, "tier": tier, "dir": direction,
                                          "entry": entry, "notional": notional,
                                          "sl_pct": sl_pct, "paper": paper, "episode": episode_id})
        return True
    except sqlite3.IntegrityError:
        try: c.rollback()
        except Exception: pass
        print(f"[skip ] {symbol}: already open"); decide("reject", symbol, "duplicate_open", {}); return False
    finally: c.close()

def close_position(pos, exit_price, side, maker=False, level=None):
    now = time.time()
    sign = 1.0 if pos["direction"] == "up" else -1.0
    move = (exit_price / pos["entry_price"] - 1.0) * 100.0 * sign
    slip_vs = None
    if maker:
        costs = pos["notional"] * (TAKER_FEE_PCT_SIDE + MAKER_FEE_PCT_SIDE) / 100.0
        if level is not None: slip_vs = (exit_price / level - 1.0) * 100.0 * sign
    else:
        costs = roundtrip_cost(pos["notional"], pos.get("spread_pct"))
    net = pos["notional"] * move / 100.0 - costs
    day = today_utc(); hold_min = (now - pos["entry_ts"]) / 60.0
    hour_utc = int(datetime.fromtimestamp(pos["entry_ts"], tz=timezone.utc).hour)
    icon = {"SL": "🛑", "TRAIL": "🎯", "GUARD": "🛡", "TIME": "⏱", "WIN30": "✅",
            "DEAD": "🪦", "CUT": "✂️", "LADDER": "🪜", "FOLLOW_STOP": "🐦",
            "FOLLOW_TP": "🐦", "FOLLOW_TIME": "🐦"}.get(side, "•")
    mfe, mae = pos.get("_mfe_pct"), pos.get("_mae_pct")
    paper = 1 if pos.get("paper") else 0
    c = sdb()
    try:
        cur = c.execute("UPDATE shadow_positions SET status='CLOSED' WHERE id=? AND status='OPEN'", (pos["id"],))
        if cur.rowcount != 1:
            c.rollback(); decide("reject", pos["symbol"], "duplicate_close", {}); return
        c.execute("""INSERT INTO shadow_trades(lane,tier,symbol,direction,entry_ts,entry_price,exit_ts,exit_price,
                     side,move_pct,gross_pnl,costs,net_pnl,hold_min,day,hour_utc,rules_ver,mfe_pct,mae_pct,
                     maker_exit,slip_vs_level,notional,sl_pct,spread_pct,paper,entry_ver,regime)
                     VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                  (pos["lane"], pos.get("tier"), pos["symbol"], pos["direction"], pos["entry_ts"],
                   pos["entry_price"], now, exit_price, side, move, pos["notional"] * move / 100.0,
                   costs, net, hold_min, day, hour_utc, RULES_VER, mfe, mae,
                   1 if maker else 0, slip_vs, pos["notional"], pos.get("sl_pct"),
                   pos.get("spread_pct"), paper, pos.get("entry_ver"),
                   pos.get("regime") or _regime_tag()))
        if not paper:
            c.execute("""INSERT INTO shadow_daily(day,realized,trades) VALUES(?,?,1)
                         ON CONFLICT(day) DO UPDATE SET realized=realized+?, trades=trades+1""", (day, net, net))
        c.commit()
    finally: c.close()
    if pos["lane"] in FLEX_CD_LANES:
        _set_lane_cooldown(pos["symbol"], pos["lane"], net)
    n_trades, tot = total_stats()
    mk = " (maker)" if maker else ""
    pfx = "PAPER " if paper else ""
    print(f"[CLOSE] [{pfx}{pos['lane']}/{pos.get('tier')}] {pos['symbol']} {side}{mk} {move:+.2f}% "
          f"net {net:+.2f} (held {hold_min:.1f}m MFE {mfe or 0:+.2f}% MAE {mae or 0:+.2f}%) | day {daily_realized(day):+.2f}")
    tag = {"REV": "⚡", "FUNDING_SQZ": "🧲", "DEEPDIP_X": "🕳🕳", "DEEPDIP_V2": "🕳",
           "SGRIND": "🐌", "SGREV": "🔄", "PFADE": "🚀", "FAILBREAK": "🪤", "SQUEEZE": "🧨",
           "FOLLOWER": "🐦", "EXPLOSION": "💥"}.get(pos["lane"], pos["lane"])
    notify(SESSION_M, f"{icon} SHADOW CLOSE [{pfx}{tag}·{pos.get('tier')}] {pos['symbol']} "
                      f"{pos['direction'].upper()} — {side}{mk}\n"
                      f"{'💰' if net > 0 else '💸'} NET: {net:+.2f} (move {move:+.2f}% · held {hold_min:.1f}m)\n"
                      f"Entry {pos['entry_price']:.6g} → Exit {exit_price:.6g} · "
                      f"stop was -{(pos.get('sl_pct') or SL_PCT):.1f}% HARD · "
                      f"MFE {mfe or 0:+.2f}% · MAE {mae or 0:+.2f}%\n"
                      f"Day PnL {daily_realized(day):+.2f} · Total {n_trades}: {tot:+.2f}\n{make_trade_link(pos['symbol'])}")
    decide("close", pos["symbol"], side, {"net": net, "move": move, "mfe": mfe,
                                          "mae": mae, "maker": maker, "paper": paper})

def _set_pos_flag(pid, col):
    try:
        c = sdb(); c.execute(f"UPDATE shadow_positions SET {col}=1 WHERE id=?", (pid,)); c.commit(); c.close()
    except Exception as e: print(f"[{hms()}] [warn ] flag {col}: {e}")

def _save_excursions(pos, peak, trough, peak_ts=None):
    prior_peak = float(pos.get("mfe_pct") or 0)
    prior_trough = float(pos.get("mae_pct") or 0)
    peak, trough = max(prior_peak, peak), min(prior_trough, trough)
    observed = (now_ts() if peak_ts is None else peak_ts) if peak > prior_peak else pos.get("peak_ts")
    if peak != prior_peak or trough != prior_trough:
        with closing(sdb()) as c, c:
            c.execute("""UPDATE shadow_positions
                SET peak_ts=CASE WHEN COALESCE(mfe_pct,0)<? THEN ? ELSE peak_ts END,
                    mfe_pct=MAX(COALESCE(mfe_pct,0),?), mae_pct=MIN(COALESCE(mae_pct,0),?)
                WHERE id=? AND status='OPEN'""", (peak, observed, peak, trough, pos["id"]))
    pos["mfe_pct"], pos["mae_pct"], pos["peak_ts"] = peak, trough, observed

# ═══════════════════════════════ END PART 3/4 ═══════════════════════════════
# Part 4/4 begins at: # ============ EXIT ENGINE + WORKERS + SELFTEST
# ================================================================ EXIT ENGINE (ladder — validated — + FOLLOWER variant)
async def fetch_candles(session, symbol, start_ms, interval="1m"):
    try:
        async with session.get(f"{FAPI}/fapi/v1/klines",
            params={"symbol": symbol, "interval": interval, "startTime": start_ms, "limit": KLINES_LIMIT},
            timeout=aiohttp.ClientTimeout(total=12)) as r:
            if r.status != 200: _log_fc_fail(symbol, f"HTTP {r.status}"); return None
            kl = await r.json()
        if not isinstance(kl, list): return None
        return [(int(k[0]), float(k[2]), float(k[3]), float(k[4])) for k in kl]
    except Exception as e: _log_fc_fail(symbol, repr(e)); return None

async def _manage_follower(session, pos):
    """FOLLOWER's own exit stack (spec): 0.6x-capture TP · 12-min time stop ·
    0.6% adverse stop · 2.2% hard stop as catastrophic backstop. TP price is
    encoded in episode_id as 'flw|<px>' (set at open). Live-managed every poll."""
    sign = 1.0 if pos["direction"] == "up" else -1.0
    entry = pos["entry_price"]
    tp = None
    try:
        if str(pos.get("episode_id") or "").startswith("flw|"):
            tp = float(pos["episode_id"].split("|", 1)[1])
    except Exception: tp = None
    def _close(px, side):
        fav_x = sign * (px / entry - 1.0) * 100.0
        pos["_mfe_pct"] = round(max(float(pos.get("mfe_pct") or 0), fav_x), 4)
        pos["_mae_pct"] = round(min(float(pos.get("mae_pct") or 0), fav_x), 4)
        close_position(pos, px, side)
    px = stream_price(pos["symbol"]) or await live_price(session, pos["symbol"])
    elapsed = time.time() - pos["entry_ts"]
    if px and px > 0:
        fav = sign * (px / entry - 1.0) * 100.0
        slp = pos.get("sl_pct") or SL_PCT
        if fav <= -slp:
            _close(px if CONSERVATIVE_FILLS else pos["sl_price"], "SL"); return True
        if fav <= -FOLLOW_ADVERSE_PCT:
            _close(px, "FOLLOW_STOP"); return True
        if tp and fav >= sign * ((tp / entry - 1.0) * 100.0):
            # (v4.7.0 had sign*(...)*sign — sign² = 1, so a SHORT with a TP
            # below entry would have closed instantly; single sign is correct)
            _close(px if CONSERVATIVE_FILLS else tp, "FOLLOW_TP"); return True
        if elapsed >= FOLLOW_TIME_STOP_MIN * 60.0:
            _close(px, "FOLLOW_TIME"); return True
        _save_excursions(pos, max(fav, 0.0), min(fav, 0.0))
        return False
    if elapsed >= (FOLLOW_TIME_STOP_MIN + 5.0) * 60.0:
        if candles := await fetch_candles(session, pos["symbol"], int(pos["entry_ts"] * 1000)):
            _close(candles[-1][3], "FOLLOW_TIME"); return True
    return False

async def manage_position(session, pos):
    """The validated v4.6.4 ladder engine, unchanged, applied to REAL and PAPER
    rows alike: -2.2% HARD · early cut -1.5% (ladder never armed) · ladder floor
    peak-0.5 (peak-1.0 ≥3.0), ratcheting · SCALP 30m-if-winning · SWING 60m +
    dead-money check. FOLLOWER routes to its own stack."""
    if pos.get("lane") == "FOLLOWER":
        return await _manage_follower(session, pos)
    sign = 1.0 if pos["direction"] == "up" else -1.0
    entry = pos["entry_price"]
    tier = pos.get("tier") or "SWING"
    scalp = tier == "SCALP"
    cap_sec = SWING_HOLD_MIN * 60.0
    sl_pct = pos.get("sl_pct") or SL_PCT
    sl_price = pos["sl_price"]
    saved_peak = float(pos.get("mfe_pct") or 0.0)
    saved_mae = float(pos.get("mae_pct") or 0.0)
    saved_peak_ts = pos.get("peak_ts")
    running_peak, running_mae, running_peak_ts = 0.0, 0.0, None
    boundary_close = None
    def _close(px, side, maker=False, level=None):
        fav_x = sign * (px / entry - 1.0) * 100.0
        pos["_mfe_pct"] = round(max(saved_peak, running_peak, fav_x), 4)
        pos["_mae_pct"] = round(min(saved_mae, running_mae, fav_x), 4)
        close_position(pos, px, side, maker=maker, level=level)
    candles = await fetch_candles(session, pos["symbol"], int(pos["entry_ts"] * 1000))
    if candles:
        for t_ms, high, low, close in candles:
            cts = t_ms / 1000.0
            if saved_peak_ts is not None and cts >= saved_peak_ts and saved_peak > running_peak:
                running_peak, running_peak_ts = saved_peak, saved_peak_ts
            fav_hi = sign * ((high if sign > 0 else low) / entry - 1.0) * 100.0
            fav_lo = sign * ((low if sign > 0 else high) / entry - 1.0) * 100.0
            if _early_cut(running_peak, fav_lo):          # -1.5% crossed before -2.2% chronologically
                cut_px = close if CONSERVATIVE_FILLS else entry * (1 - sign * EARLY_CUT_PCT / 100.0)
                _close(cut_px, "CUT"); return True
            if fav_lo <= -sl_pct:
                running_mae = min(running_mae, fav_lo)
                _close(close if CONSERVATIVE_FILLS else sl_price, "SL"); return True
            fl = _ladder_floor(running_peak)
            if fl is not None and fav_lo <= fl:
                lvl = entry * (1 + sign * fl / 100.0)
                if CONSERVATIVE_FILLS: _close(close, "LADDER", maker=False)
                elif fav_lo < fl: _close(lvl, "LADDER", maker=True, level=lvl)
                else: _close(lvl, "LADDER")
                return True
            if fav_hi > running_peak:
                running_peak, running_peak_ts = fav_hi, min(cts + 60.0, now_ts())
            running_mae = min(running_mae, fav_lo)
            if scalp and cts - pos["entry_ts"] >= SCALP_HOLD_MIN * 60.0 and boundary_close is None:
                boundary_close = close
            if cts - pos["entry_ts"] >= cap_sec: break
    if saved_peak > running_peak:
        running_peak, running_peak_ts = saved_peak, saved_peak_ts
    running_mae = min(running_mae, saved_mae)
    _save_excursions(pos, running_peak, running_mae, running_peak_ts)
    elapsed = time.time() - pos["entry_ts"]
    if scalp and elapsed >= SCALP_HOLD_MIN * 60.0 and not pos.get("t30_done"):
        px = stream_price(pos["symbol"]) or await live_price(session, pos["symbol"])
        if px and px > 0:
            net_now = pos["notional"] * ((px / entry - 1.0) * 100.0 * sign) / 100.0 - \
                      roundtrip_cost(pos["notional"], pos.get("spread_pct"))
            if net_now > 0: _close(px, "WIN30"); return True
            _set_pos_flag(pos["id"], "t30_done"); pos["t30_done"] = 1
            print(f"[{hms()}] [t30  ] {pos['symbol']} SCALP at 30m not winning — continuing to 60m cap")
    if not scalp and elapsed >= 30.0 * 60.0 and not pos.get("dead_checked") and running_peak < 0.5:
        px = stream_price(pos["symbol"]) or await live_price(session, pos["symbol"])
        if px and px > 0: _close(px, "DEAD"); return True
        if candles: _close(candles[-1][3], "DEAD"); return True
        _set_pos_flag(pos["id"], "dead_checked"); pos["dead_checked"] = 1
    if elapsed >= cap_sec:
        px = stream_price(pos["symbol"]) or await live_price(session, pos["symbol"])
        if px and px > 0: _close(px, "TIME"); return True
        if boundary_close: _close(boundary_close, "TIME"); return True
        if candles: _close(candles[-1][3], "TIME"); return True
        print(f"[{hms()}] [warn ] {pos['symbol']} cap due but no price — retrying"); return False
    px = stream_price(pos["symbol"]) or await live_price(session, pos["symbol"])
    if px and px > 0:
        fav = sign * (px / entry - 1.0) * 100.0
        if _early_cut(running_peak, fav):
            _close(px if CONSERVATIVE_FILLS else entry * (1 - sign * EARLY_CUT_PCT / 100.0), "CUT"); return True
        if fav <= -sl_pct:
            _close(px if CONSERVATIVE_FILLS else sl_price, "SL"); return True
        fl = _ladder_floor(running_peak)
        if fl is not None and fav <= fl:
            lvl = entry * (1 + sign * fl / 100.0)
            if CONSERVATIVE_FILLS: _close(px, "LADDER", maker=False)
            else: _close(lvl, "LADDER", maker=True, level=lvl)
            return True
        _save_excursions(pos, max(running_peak, fav), min(running_mae, fav))
    return False

# ================================================================ SIGNAL CONSUMER (trader role)
async def signal_consumer():
    """Split-mode trader: claims scanner-emitted signals from the ledger and
    opens them after RE-VERIFYING every DB-side admission check. Signals older
    than SIGNAL_TTL_SEC expire; claimed-but-stalled rows reclaim after
    SIGNAL_CLAIM_TTL. 'both' mode never starts this worker."""
    if NEXUS_ROLE != "trader": return
    print(f"[{hms()}] [signal] consumer live (trader role)")
    while not _shutdown.is_set():
        try:
            now = now_ts()
            with closing(sdb()) as c:
                c.execute("UPDATE signals SET state='new', claimed_by=NULL, claimed_ts=NULL "
                          "WHERE state='claimed' AND claimed_ts < ?", (now - SIGNAL_CLAIM_TTL,))
                c.commit()
                c.row_factory = sqlite3.Row   # (v4.7.0: plain tuples — r["..."] raised)
                rows = c.execute("SELECT * FROM signals WHERE state='new' ORDER BY id LIMIT 20").fetchall()
            for r in rows:
                with closing(sdb()) as c:
                    cur = c.execute("UPDATE signals SET state='claimed',claimed_by=?,claimed_ts=? "
                                    "WHERE id=? AND state='new'",
                                    (f"{_host()}:{os.getpid()}", now, r["id"]))
                    c.commit()
                    if cur.rowcount != 1: continue   # another trader claimed it
                if now - r["ts"] > SIGNAL_TTL_SEC:
                    with closing(sdb()) as c, c:
                        c.execute("UPDATE signals SET state='expired' WHERE id=?", (r["id"],))
                    decide("expire", r["symbol"], "signal_ttl", {"lane": r["lane"]}); continue
                try: payload = json.loads(r["payload"] or "{}")
                except Exception: payload = {}
                # DB-side re-verification (the trader trusts nothing)
                if kill_now() or _shutdown.is_set():
                    with closing(sdb()) as c, c:
                        c.execute("UPDATE signals SET state='skipped' WHERE id=?", (r["id"],))
                    continue
                if symbol_busy(r["symbol"]) or len(open_positions()) >= MAX_CONCURRENT \
                   or (r["lane"] in DIP_LANES and not _dip_budget_ok()):
                    with closing(sdb()) as c, c:
                        c.execute("UPDATE signals SET state='rejected' WHERE id=?", (r["id"],))
                    decide("reject", r["symbol"], "signal_admission", {"lane": r["lane"]}); continue
                paper = bool(payload.get("paper"))
                spawn_entry(_confirm_and_open(SESSION_M, r["lane"], r["symbol"],
                                              r["direction"], r["sig_px"],
                                              paper=paper,
                                              episode_id=payload.get("episode_id"),
                                              payload=payload))
                with closing(sdb()) as c, c:
                    c.execute("UPDATE signals SET state='done' WHERE id=?", (r["id"],))
        except Exception as e: print(f"[{hms()}] [warn ] signal consumer: {e!r}")
        await asyncio.sleep(SIGNAL_POLL_SEC)

# ================================================================ POLL LOOP (role-gated)
async def poll_loop(session):
    """Exits run whenever this process trades (trader/both); scanners run
    whenever it detects (scanner/both). Paper positions are managed wherever
    exits run — they need the same machinery."""
    global _btc_hot
    while not _shutdown.is_set():
        try:
            now = time.time(); day = today_utc()
            _write_heartbeat(); poll_takeover()
            if _shutdown.is_set(): break
            b5 = _px_ago("BTCUSDT", now, 300)
            if b5 and b5 > 0:
                btc_px = stream_price("BTCUSDT")
                if btc_px and btc_px > 0:
                    _btc_hot = abs((btc_px / b5 - 1.0) * 100.0) >= BTC_HOT_MOVE_PCT
            # ── exits (trader/both): REAL + PAPER rows ──
            if IS_TRADER:
                for pos in all_open_positions():
                    try: await manage_position(session, pos)
                    except Exception as e: print(f"[{hms()}] [err  ] manage {pos['symbol']}: {e!r}")
            # ── scanners (scanner/both) ──
            if IS_SCANNER:
                for key in [k for k, w in SS.waitroom.items() if now - w["ts"] > WAITROOM_SEC]:
                    w = SS.waitroom.pop(key); decide("expire", w["symbol"], "waitroom_timeout", {})
                opens = open_positions()
                if not kill_now() and SS.waitroom and len(opens) < MAX_CONCURRENT:
                    ranked = []
                    for w in sorted(SS.waitroom.values(), key=lambda w: w["ts"])[:RANK_REST_CAP]:
                        try: score, comp = await rank_candidate(session, w)
                        except Exception: score, comp = 0.0, {}
                        ranked.append((score, comp, w))
                    ranked.sort(key=lambda x: -x[0])
                    score, comp, w = ranked[0]
                    SS.waitroom.pop(f"{w['symbol']}|{w['etype']}", None)
                    print(f"[{hms()}] [rank ] {w['symbol']} {w['etype']} wins slot score {score:+.2f}")
                    px = stream_price(w["symbol"]) or w["px"]
                    _signal(session, w["symbol"], w["etype"], w["direction"], px, now_ts(),
                            paper=w.get("paper", False), payload=w.get("payload"),
                            episode_id=w.get("episode_id"))
                try: _dd2_scan(session, now)
                except Exception as e: print(f"[warn ] dd2: {e}")
                try: _dipx_scan(session, now)
                except Exception as e: print(f"[warn ] dipx: {e}")
                try: _sg_scan(session, now)
                except Exception as e: print(f"[warn ] sgrind: {e}")
                try: _sgrev_scan(session, now)
                except Exception as e: print(f"[warn ] sgrev: {e}")
                try: _burst_scan(session, now)
                except Exception as e: print(f"[warn ] burst: {e}")
                try: _pf_scan(session, now)
                except Exception as e: print(f"[warn ] pfade: {e}")
                try: _fb_scan(session, now)
                except Exception as e: print(f"[warn ] failbreak: {e}")
                try: _sq_scan(session, now)
                except Exception as e: print(f"[warn ] squeeze: {e}")
                _follower_scan(session, now)
                _follower_confirm(session, now)
                _dayopen_scan(now)
            # ── tick line ──
            opens = open_positions(); papers = [p for p in all_open_positions() if p.get("paper")]
            syms = len(SS.px_windows); warm = 0
            for sym, dq in SS.px_windows.items():
                if dq and now - dq[0][0] >= WIN_SEC: warm += 1
            tick = (f"[{hms()}] [tick ] role={NEXUS_ROLE} open={len(opens)} paper={len(papers)} "
                    f"dayPnL={daily_realized(day):+.2f} killed={kill_now()} btcHot={_btc_hot} "
                    f"syms={syms} warm={warm} eps={len(SS.rev_eps)} pb={len(SS.pendings)} "
                    f"k15={len(_k15)} watching={len(SS.watch)} msgs={SS.msgs}")
            if opens: tick += " · " + ", ".join(f"{p['symbol']}/{p.get('tier','?')}" for p in opens)
            print(tick)
        except Exception as e:
            print(f"[err  ] {e!r}"); traceback.print_exc()
        await asyncio.sleep(POLL_SEC)

# ================================================================ NOW-MEASURE (carried, horizon_1m_v1)
async def fetch_horizon_price(session, symbol, target_ts):
    """Last completed 1m close at/before the requested horizon; never current price."""
    end_ms = int(target_ts // 60) * 60000 - 1
    start_ms = end_ms - 59999
    try:
        async with session.get(f"{FAPI}/fapi/v1/klines",
            params={"symbol": symbol, "interval": "1m", "startTime": start_ms,
                    "endTime": end_ms, "limit": 1}, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200: return None
            rows = await r.json()
        if not isinstance(rows, list) or len(rows) != 1: return None
        k = rows[0]
        if int(k[0]) != start_ms or int(k[6]) != end_ms: return None
        px = float(k[4])
        if not math.isfinite(px) or px <= 0: return None
        return px, end_ms / 1000.0
    except Exception as e:
        _log_fc_fail(symbol, f"horizon price: {e!r}"); return None

async def now_measure_worker(session):
    """Durable due-queue; short transactions; no DB connection spans a network
    await. The Brain's report card (1,549+ samples and counting)."""
    while not _shutdown.is_set():
        try:
            now = now_ts()
            with closing(sqlite3.connect(ALERT_DB, timeout=15)) as c:
                c.execute("PRAGMA busy_timeout=15000")
                rows = c.execute("""SELECT a.id, a.symbol, a.direction, ap.p_final, a.detected_ts,
                    COALESCE(a.entry_price_10s, a.detected_price, a.alert_price)
                    FROM alert_predictions ap JOIN alerts a ON a.id=ap.alert_id
                    WHERE ap.band='now' AND ap.p_final>=? AND a.detected_ts<=?
                      AND a.detected_ts>=? AND a.source LIKE 'live_nexus%'
                    ORDER BY a.detected_ts ASC""",
                    (ALIGN_CAL_BOOST, now - NOW_MEASURE_MIN_AGE_SEC,
                     now - NOW_MEASURE_MAX_AGE_SEC)).fetchall()
            with closing(sdb()) as c:
                done_ids = {r[0] for r in c.execute(
                    "SELECT alert_id FROM now_measurements WHERE method=?", (NOW_MEASURE_METHOD,))}
            attempted = 0
            for aid, sym, d, cal, det_ts, det_px in rows:
                if _shutdown.is_set() or attempted >= 200: break
                if aid in done_ids or d not in ("up", "down"): continue
                try: det_px = float(det_px)
                except (ValueError, TypeError): continue
                if not math.isfinite(det_px) or det_px <= 0: continue
                attempted += 1
                sample = await fetch_horizon_price(session, sym, det_ts + NOW_MEASURE_MIN_AGE_SEC)
                if sample is None: continue
                px, sample_ts = sample
                sign = 1.0 if d == "up" else -1.0
                mv = (px / det_px - 1.0) * 100.0 * sign
                with closing(sdb()) as c, c:
                    c.execute("""INSERT INTO now_measurements
                        (alert_id,symbol,dir,cal,call_ts,call_px,px_5m,final_mv,age_s,sample_ts,measured_ts,method)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(alert_id) DO UPDATE SET
                        symbol=excluded.symbol, dir=excluded.dir, cal=excluded.cal,
                        call_ts=excluded.call_ts, call_px=excluded.call_px, px_5m=excluded.px_5m,
                        final_mv=excluded.final_mv, age_s=excluded.age_s, sample_ts=excluded.sample_ts,
                        measured_ts=excluded.measured_ts, method=excluded.method
                        WHERE now_measurements.method IS NULL OR now_measurements.method!=excluded.method""",
                        (aid, sym, d, cal, det_ts, det_px, px, mv, sample_ts - det_ts,
                         sample_ts, now_ts(), NOW_MEASURE_METHOD))
                done_ids.add(aid)
        except Exception as e: print(f"[warn ] now-measure: {e}")
        await asyncio.sleep(60)

# ================================================================ REPORT
def build_report_text(all_versions=False):
    c = sdb(); c.row_factory = sqlite3.Row
    ver_q = "" if all_versions else " AND rules_ver=?"
    params = () if all_versions else (RULES_VER,)
    real = [dict(r) for r in c.execute(
        f"SELECT * FROM shadow_trades WHERE paper=0{ver_q} ORDER BY exit_ts ASC", params).fetchall()]
    pap = [dict(r) for r in c.execute(
        f"SELECT * FROM shadow_trades WHERE paper=1{ver_q} ORDER BY exit_ts ASC", params).fetchall()]
    opens_r = [dict(r) for r in c.execute("SELECT * FROM shadow_positions WHERE status='OPEN' AND paper=0").fetchall()]
    opens_p = [dict(r) for r in c.execute("SELECT * FROM shadow_positions WHERE status='OPEN' AND paper=1").fetchall()]
    try:
        nm = dict(c.execute("SELECT COUNT(*) n, ROUND(AVG(final_mv),3) avg_mv FROM now_measurements WHERE method=?",
                            (NOW_MEASURE_METHOD,)).fetchone())
    except Exception: nm = None
    try:
        oi_eps = c.execute("SELECT COUNT(DISTINCT episode_id) FROM oi_log").fetchone()[0]
    except Exception: oi_eps = 0
    c.close()
    ver_lbl = "ALL versions" if all_versions else f"rules_ver {RULES_VER}"
    lines = [f"📊 NEXUS v{RULES_VER} SCOREBOARD — {len(real)} closed trades ({ver_lbl}) · "
             f"stop -{SL_PCT}% HARD · ladder {LADDER_ARM_PCT}/{LADDER_GAP}→{LADDER_GAP_AT}/{LADDER_GAP_BIG} · "
             f"cut -{EARLY_CUT_PCT}% · role {NEXUS_ROLE}"]
    for p in opens_r + opens_p:
        tag = {"REV": "⚡", "FUNDING_SQZ": "🧲", "DEEPDIP_X": "🕳🕳", "DEEPDIP_V2": "🕳",
               "SGRIND": "🐌", "SGREV": "🔄", "PFADE": "🚀", "FAILBREAK": "🪤", "SQUEEZE": "🧨",
               "FOLLOWER": "🐦", "EXPLOSION": "💥"}.get(p.get("lane"), "")
        age = (time.time() - p["entry_ts"]) / 60.0
        pfx = "PAPER " if p.get("paper") else ""
        lines.append(f"🔵 {'🟡' if p.get('paper') else ''}OPEN: [{pfx}{tag}{p.get('lane')}·{p.get('tier')}] "
                     f"{p['symbol']} {p['direction'].upper()} @{p['entry_price']:.6g} "
                     f"({age:.1f}m) peak {(p.get('mfe_pct') or 0):+.2f}%")
    def avg(v):
        v = [x for x in v if x is not None]
        return sum(v) / len(v) if v else 0.0
    def block(name, ts_):
        if not ts_: return None
        n_ = len(ts_); wins = sum(1 for t in ts_ if t["net_pnl"] > 0); net = sum(t["net_pnl"] for t in ts_)
        return (f"{name:<12} {n_:>3} tr · win {wins/n_*100:.0f}% · net ${net:+.2f} · exp ${net/n_:+.2f} · "
                f"MFE {avg([t['mfe_pct'] for t in ts_]):+.2f}% MAE {avg([t['mae_pct'] for t in ts_]):+.2f}%")
    LIVE_LANES = ("REV", "FUNDING_SQZ", "DEEPDIP_X", "SGRIND", "SGREV", "PFADE", "EXPLOSION",
                  "DEEPDIP_V2", "FAILBREAK", "SQUEEZE", "FOLLOWER", "BURST")
    PAPER_LANES = ()
    if not real and not pap:
        lines.append("No closed trades yet for this version.")
        lines.append("Live (all 12): REV v3 · FSQZ · DIP-X · SGRIND(cont) · SGREV(rev) · PFADE · "
                     "DEEPDIP v2 · FAILBREAK · SQUEEZE · FOLLOWER · BURST · EXPLOSION(v4.6.4)")
        return "\n".join(lines)
    lines.append("")
    for ln in LIVE_LANES:
        txt = block(ln, [t for t in real if t.get("lane") == ln])
        if txt: lines.append(txt)
    for ln in PAPER_LANES:
        lt = [t for t in pap if t.get("lane") == ln]
        if lt:
            lines.append("📄 " + (block(ln + " (paper)", lt) or ""))
    pf = [t for t in real if t.get("lane") == "PFADE"]
    for lbl, d_ in (("  PFADE short", "down"), ("  PFADE re-long", "up")):
        txt = block(lbl, [t for t in pf if t.get("direction") == d_])
        if txt: lines.append(txt)
    lines.append("── tier split (real) ──")
    for tn in ("SCALP", "SWING"):
        txt = block(tn + " tier", [t for t in real if t.get("tier") == tn])
        if txt: lines.append(txt)
    hours = defaultdict(list)
    for t in real:
        if t.get("hour_utc") is not None: hours[t["hour_utc"]].append(t)
    if len(hours) >= 4:
        sess = [("ASIA [0-7)", range(0, 7)), ("EU [7-13)", range(7, 13)),
                ("US [13-20)", range(13, 20)), ("LATE [20-24)", range(20, 24))]
        sparts = [f"{nm2} {len(ts_):>3}tr ${sum(t['net_pnl'] for t in ts_):+.0f}"
                  for nm2, hrs in sess if len(ts_ := [t for t in real if t.get("hour_utc") in hrs]) >= 5]
        if sparts: lines.append("🕒 Sessions: " + " · ".join(sparts))
    doors = defaultdict(list)
    for t in real: doors[t["side"]].append(t)
    order = ["LADDER", "CUT", "SL", "WIN30", "DEAD", "TIME",
             "FOLLOW_TP", "FOLLOW_STOP", "FOLLOW_TIME"]
    parts = [f"{s} {len(v)} (${sum(t['net_pnl'] for t in v):+.2f})"
             for s in order if (v := doors.get(s))]
    parts += [f"{s} {len(v)} (${sum(t['net_pnl'] for t in v):+.2f})"
              for s, v in doors.items() if s not in order]
    if parts: lines.append("Exits: " + " · ".join(parts))
    makers = [t for t in real if t.get("maker_exit")]
    if makers:
        lines.append(f"Maker exits: {len(makers)} · avg slip vs level {avg([t['slip_vs_level'] for t in makers]):+.3f}%")
    GATES = {"REV": (REV_CUTOVER_N, "ge"), "FUNDING_SQZ": (20, "gt"), "DEEPDIP_X": (20, "gt"),
             "SGRIND": (50, "gt"), "SGREV": (20, "gt"), "PFADE": (20, "gt"), "EXPLOSION": (20, "gt"),
             "BURST": (20, "gt"), "DEEPDIP_V2": (20, "gt"), "FAILBREAK": (20, "gt"),
             "SQUEEZE": (20, "gt"), "FOLLOWER": (20, "gt")}
    gparts = []
    for ln, (need, mode) in GATES.items():
        lt = [t for t in real if t.get("lane") == ln]
        if len(lt) >= need:
            e = sum(t["net_pnl"] for t in lt) / len(lt)
            ok = (e >= 0) if mode == "ge" else (e > 0)
            gparts.append(f"{ln} {'PASS' if ok else 'FAIL'} ({e:+.2f}/tr)")
    if gparts: lines.append("🚦 Gates: " + " · ".join(gparts))
    pparts = [f"{ln} {len([t for t in pap if t.get('lane')==ln])}/20"
              for ln in PAPER_LANES if any(t.get("lane") == ln for t in pap)]
    if pparts: lines.append("📄 Paper progress: " + " · ".join(pparts))
    n = len(real); wins = sum(1 for t in real if t["net_pnl"] > 0); net = sum(t["net_pnl"] for t in real)
    peak = cum = dd = 0.0
    for t in real:
        cum += t["net_pnl"]; peak = max(peak, cum); dd = max(dd, peak - cum)
    lines.append(f"ALL(real): win {wins}/{n} ({wins/n*100:.0f}%) · net ${net:+.2f} · exp ${net/n:+.2f} · "
                 f"maxDD ${dd:.2f} (realized-only)" if n else "ALL(real): no trades yet")
    if pap:
        pnet = sum(t["net_pnl"] for t in pap)
        lines.append(f"Paper book: {len(pap)} trades · net ${pnet:+.2f} (excluded from real PnL)")
    if nm and nm.get("n"):
        lines.append(f"🔬 NOW-measure: {nm['n']} logged · 5m target (1m closes) {nm['avg_mv']:+.3f}%")
    if oi_eps:
        lines.append(f"🫗 OI_FLUSH instrument: {oi_eps} episodes logged (arm decision at 2wks/40 events)")
    lines.append(f"Pre-registered: no changes until lane gates or 21 days ({ver_lbl})")
    return "\n".join(lines)
async def tg_report_worker(session):
    while not _shutdown.is_set():
        try:
            nowv = datetime.now(timezone.utc)
            mins = (120 - ((nowv.hour * 60 + nowv.minute) % 120)) or 120
            wait = max(60.0, mins * 60.0 - nowv.second)
        except Exception: wait = 7200.0
        slept = 0.0
        while slept < wait and not _shutdown.is_set():
            c = min(10.0, wait - slept); await asyncio.sleep(c); slept += c
        if _shutdown.is_set(): return
        try:
            ok = await tg_send(session, build_report_text(False), retries=5, backoff=8.0)
            if not ok: print("[tg   ] report FAILED after retries")
        except Exception as e: print(f"[tg   ] report: {e}")
def report(all_versions=False):
    sdb_init()   # fresh/missing ledger: ensure schema before querying (v4.7.1)
    print(build_report_text(all_versions))

# ================================================================ SELFTEST
def _smoke_test_db():
    """Round-trip: real open/close, PAPER open on the SAME symbol (must be
    allowed — paper never consumes capacity), size_mult, LADDER+CUT sides,
    capacity enforcement. Isolated temp ledger."""
    global _SDB_PATH, KILL, KILLFILE, TG_ENABLED, _shutdown, NEXUS_ROLE
    prev = _SDB_PATH, KILL, KILLFILE, TG_ENABLED, _shutdown
    KILL, KILLFILE, TG_ENABLED, _shutdown = False, "", False, asyncio.Event()
    tmp = tempfile.NamedTemporaryFile(prefix="nexus_smoke_", suffix=".db", delete=False); tmp.close()
    _SDB_PATH = tmp.name
    try:
        c = sdb(); _ensure_schema(c); c.close()
        if not open_position("SMOKE", "SWING", "SMOKUSDT", "up", 100.0, spread=0.05):
            print("[FATAL] smoke: real open failed"); return False
        if not open_position("SMOKE", "SCALP", "SMOKUSDT", "up", 100.0, paper=True):
            print("[FATAL] smoke: PAPER open on same symbol failed (paper must not collide)"); return False
        rows = all_open_positions()
        if len(rows) != 2: print("[FATAL] smoke: expected real+paper rows"); return False
        paper_row = [p for p in rows if p["paper"]][0]
        if not open_position("SMOKE2", "SCALP", "SMOKUSDT", "down", 100.0, size_mult=0.5, paper=True):
            print("[FATAL] smoke: half-size paper open failed"); return False
        rows = all_open_positions()
        half = [p for p in rows if p["symbol"] == "SMOKUSDT" and p["direction"] == "down"][0]
        full = [p for p in rows if p["symbol"] == "SMOKUSDT" and p["direction"] == "up" and not p["paper"]][0]
        if abs(half["notional"] * 2 - full["notional"]) > 2.0:
            print(f"[FATAL] smoke: size_mult not applied ({half['notional']} vs {full['notional']})"); return False
        real_row = full
        close_position(real_row, 102.0, "LADDER", maker=True, level=102.0)
        close_position(paper_row, 97.0, "CUT")
        c = sdb()
        sides = {r[0] for r in c.execute("SELECT DISTINCT side FROM shadow_trades")}
        n_open = c.execute("SELECT COUNT(*) FROM shadow_positions WHERE status='OPEN'").fetchone()[0]
        c.close()
        if n_open != 1: print("[FATAL] smoke: closes left wrong open count"); return False
        if not {"LADDER", "CUT"} <= sides: print(f"[FATAL] smoke: sides missing ({sides})"); return False
        c = sdb()
        for i in range(MAX_CONCURRENT):
            c.execute("""INSERT INTO shadow_positions(lane,tier,symbol,direction,entry_ts,entry_price,
                         notional,sl_price,sl_pct,spread_pct,opened_day,status,paper)
                         VALUES('X','SWING',?,?,0,1,100,0.9,2.2,0.05,'2000-01-01','OPEN',0)""",
                       (f"CAP{i:02d}USDT", "up" if i % 2 == 0 else "down"))
        c.commit(); c.close()
        if open_position("SMOKE", "SWING", "CAPTESTUSDT", "up", 100.0):
            print("[FATAL] smoke: capacity cap NOT enforced"); return False
        return True
    except Exception as e:
        print(f"[FATAL] DB smoke test failed: {e!r}"); return False
    finally:
        _SDB_PATH, KILL, KILLFILE, TG_ENABLED, _shutdown = prev
        try: os.unlink(tmp.name)
        except Exception: pass

def _migration_test_db():
    """Old-schema ledger → _ensure_schema → sl_pct recovered from sl_price →
    legacy close succeeds. The RARE-class regression test."""
    global _SDB_PATH, KILL, KILLFILE, TG_ENABLED, _shutdown
    prev = _SDB_PATH, KILL, KILLFILE, TG_ENABLED, _shutdown
    KILL, KILLFILE, TG_ENABLED, _shutdown = False, "", False, asyncio.Event()
    tmp = tempfile.NamedTemporaryFile(prefix="nexus_mig_", suffix=".db", delete=False); tmp.close()
    _SDB_PATH = tmp.name
    try:
        c = sqlite3.connect(tmp.name)
        c.executescript("""
        CREATE TABLE shadow_positions(id INTEGER PRIMARY KEY AUTOINCREMENT, lane TEXT, tier TEXT,
            symbol TEXT, direction TEXT, entry_ts REAL, entry_price REAL, notional REAL, sl_price REAL,
            spread_pct REAL, opened_day TEXT, status TEXT DEFAULT 'OPEN');
        CREATE TABLE shadow_trades(id INTEGER PRIMARY KEY AUTOINCREMENT, lane TEXT, symbol TEXT,
            direction TEXT, entry_ts REAL, entry_price REAL, exit_ts REAL, exit_price REAL, side TEXT,
            move_pct REAL, gross_pnl REAL, costs REAL, net_pnl REAL, hold_min REAL, day TEXT, rules_ver TEXT);
        CREATE TABLE shadow_meta(key TEXT PRIMARY KEY, value TEXT);""")
        c.execute("""INSERT INTO shadow_positions(lane,tier,symbol,direction,entry_ts,entry_price,notional,
                     sl_price,spread_pct,opened_day,status)
                     VALUES('OLD','SWING','MIGUSDT','up',0,100.0,1000.0,97.8,0.05,'2000-01-01','OPEN')""")
        c.commit(); c.close()
        c = sdb(); _ensure_schema(c); c.close()
        pos = open_positions()[0]
        if abs((pos.get("sl_pct") or 0) - 2.2) > 1e-6:
            print(f"[FATAL] migration: sl_pct not recovered ({pos.get('sl_pct')})"); return False
        close_position(pos, 101.0, "TIME")
        c = sdb()
        tr = c.execute("SELECT net_pnl FROM shadow_trades WHERE symbol='MIGUSDT'").fetchone()
        n_open = c.execute("SELECT COUNT(*) FROM shadow_positions WHERE status='OPEN'").fetchone()[0]
        c.close()
        if tr is None or n_open != 0: print("[FATAL] migration: legacy close failed"); return False
        return True
    except Exception as e:
        print(f"[FATAL] migration test failed: {e!r}"); return False
    finally:
        _SDB_PATH, KILL, KILLFILE, TG_ENABLED, _shutdown = prev
        try: os.unlink(tmp.name)
        except Exception: pass

def _numeric_tests():
    """The exact cases that failed historically, as permanent assertions."""
    global _SDB_PATH
    prev_db = _SDB_PATH
    tmp = tempfile.NamedTemporaryFile(prefix="nexus_num_", suffix=".db", delete=False); tmp.close()
    _SDB_PATH = tmp.name          # isolate: explosion/cooldown tests touch decisions
    _c = sdb(); _ensure_schema(_c); _c.close()
    problems = []
    # ladder (incl. the RAREUSDT case: peak 1.94 → floor 1.44)
    for pk, want in [(0.79, None), (0.8, 0.3), (1.94, 1.44), (2.99, 2.49), (3.0, 2.0), (3.5, 2.5)]:
        got = _ladder_floor(pk)
        if want is None:
            if got is not None: problems.append(f"ladder({pk})={got}, want None")
        elif got is None or abs(got - want) > 1e-6:
            problems.append(f"ladder({pk})={got}, want {want}")
    # early cut
    for pk, fav, want in [(0.79, -1.5, True), (0.8, -1.5, False), (0.0, -1.4, False),
                          (0.5, -2.0, True), (1.0, -3.0, False)]:
        if _early_cut(pk, fav) != want: problems.append(f"early_cut({pk},{fav})!={want}")
    # systemic at the recalibrated 0.40
    for v, want in [(-0.40, True), (-0.399, False), (-1.5, True), (0.0, False)]:
        if _is_systemic(v) != want: problems.append(f"is_systemic({v})!={want}")
    # REV v2 three-outcome watch (the state machine, pure)
    now = now_ts()
    def ep(low=100.0, low_ts=None, trig=None):
        return {"low": low, "low_ts": low_ts if low_ts is not None else now,
                "trig_ts": trig if trig is not None else now, "dump": 3.2, "resets": 0}
    if _rev_tick_ep(ep(), 100.75, now) != "enter": problems.append("rev3: +0.75% bounce must ENTER")
    if _rev_tick_ep(ep(), 100.74, now) is not None: problems.append("rev3: sub-bounce must keep watching")
    if _rev_tick_ep(ep(), 100.5, now) is not None: problems.append("rev3: +0.5% below the 0.75% bounce must WATCH")
    e2 = ep(low_ts=now - 11.0)
    if _rev_tick_ep(e2, 100.2, now) != "dead": problems.append("rev2: 10s no-bounce must DEAD")
    e3 = ep()
    r = _rev_tick_ep(e3, 99.5, now)
    if r is not None or e3["low"] != 99.5 or e3["resets"] != 1:
        problems.append("rev2: new low must RESET (low updated, counter incremented)")
    if _rev_tick_ep(ep(trig=now - 121.0), 99.0, now) != "stale":
        problems.append("rev2: >120s episode must go STALE")
    # EXPLOSION v4.6.4 machine: minute projection, refractory, knife-guard
    sym = "PACETESTUSDT"
    t0 = now_ts(); bucket = int(t0 // 60); t_in = bucket * 60 + 5.0
    SS.px_windows[sym] = deque([(bucket * 60 - 5, 100.0), (t_in - 1.0, 102.0)])
    _vol_medians[sym] = 1000.0
    _explosion_accumulate(sym, 3000.0, 102.0, t_in)     # 3000 in 5s → 36x projected
    pb = SS.pendings.get(sym)
    if not pb:
        problems.append("explosion: 36x pace +2% move must ARM a pullback")
    else:
        if pb["direction"] != "up" or abs(pb["level"] - 102.0 * (1 - EXPLOSION_PULLBACK_PCT / 100.0)) > 1e-9:
            problems.append("explosion: wrong direction/level on armed pullback")
        if SS.exp_fired.get(sym) != bucket:
            problems.append("explosion: minute refractory not stamped")
    _explosion_accumulate(sym, 5000.0, 102.5, t_in)     # same bucket → no re-arm
    if len(SS.pendings) != 1: problems.append("explosion: refractory fired twice")
    SS.pendings.pop(sym, None); SS.exp_fired.pop(sym, None); SS.exp_acc.pop(sym, None)
    _explosion_accumulate(sym, 3000.0, 102.0, bucket * 60 + 2.0)   # <3s window
    if SS.pendings.get(sym): problems.append("explosion: fired before the 3s window")
    SS.pendings.pop(sym, None); SS.exp_fired.pop(sym, None); SS.exp_acc.pop(sym, None)
    SS.pendings[sym] = {"symbol": sym, "direction": "up", "level": 100.0,
                        "trigger_px": 101.0, "ts": t0}
    SS.px_windows[sym] = deque([(t0 - 2.0, 100.5), (t0 - 1.0, 100.4), (t0, 100.0)])
    _orig_spawn = spawn_entry
    globals()["spawn_entry"] = lambda coro: coro.close()   # don't run opens in test
    try:
        _check_explosion_fills(None, t0)
        if not SS.pendings.get(sym): problems.append("explosion: knife-guard failed (falling touch filled)")
        SS.px_windows[sym] = deque([(t0 - 1.0, 99.9), (t0, 100.0)])   # rising touch
        _check_explosion_fills(None, t0)
        if SS.pendings.get(sym): problems.append("explosion: rising touch must FILL")
    finally:
        globals()["spawn_entry"] = _orig_spawn
        SS.pendings.pop(sym, None); SS.px_windows.pop(sym, None); _vol_medians.pop(sym, None)
    # flexible cooldown math (v4.8)
    _set_lane_cooldown("CDUSDT", "SGRIND", 10.0)
    if abs(_lane_cd[("CDUSDT", "SGRIND")] - now_ts() - LANE_COOLDOWN_WIN_SEC) > 1.0:
        problems.append("cooldown: win must be 5 min")
    _set_lane_cooldown("CDUSDT", "SGRIND", -45.0)
    if abs(_lane_cd[("CDUSDT", "SGRIND")] - now_ts() - LANE_COOLDOWN_MAX_SEC) > 1.0:
        problems.append("cooldown: full ~$45 loss must be 60 min")
    _set_lane_cooldown("CDUSDT", "SGRIND", -5.0)
    if abs(_lane_cd[("CDUSDT", "SGRIND")] - now_ts() - LANE_COOLDOWN_MIN_SEC) > 1.0:
        problems.append("cooldown: tiny loss must hit the 10 min floor")
    _set_lane_cooldown("CDUSDT", "SGRIND", -22.5)
    if abs(_lane_cd[("CDUSDT", "SGRIND")] - now_ts() - 1800.0) > 2.0:
        problems.append("cooldown: half loss must scale to ~30 min")
    if not _lane_cooldown_ok("CDUSDT", "REV", 0.0):
        problems.append("cooldown: REV must stay cooldown-free")
    if _lane_cooldown_ok("CDUSDT", "SGRIND", _lane_cd[("CDUSDT", "SGRIND")] - 1.0):
        problems.append("cooldown: inside the window must block")
    _lane_cd.pop(("CDUSDT", "SGRIND"), None)
    # SGRIND continuation + SGREV reversal A/B (v4.8.2)
    sym = "SGTESTUSDT"
    now_s = now_ts()
    c1_open_ms = (int(now_s // 900) - 1) * 900 * 1000     # last CLOSED candle
    def _sg_bars(n, step):
        """n green streak candles (open_i = o0 + i*step) + 1 red opposite."""
        o0 = 100.0
        out = []
        for k in range(n):
            o = o0 + k * step; cl = o + step
            out.append((c1_open_ms - (n - k) * 900000, o, cl + 0.1, o - 0.1, cl))
        ro = o0 + n * step                                 # opposite candle open
        out.append((c1_open_ms, ro, ro + 0.1, ro - 1.0, ro - 0.8))
        return out
    def _sg_setup(bars, px):
        _k15[sym] = {"bars": deque(bars), "hi24": max(b[2] for b in bars),
                     "lo24": min(b[3] for b in bars)}
        _sg_last_bar.pop(sym, None); _sgrev_last_bar.pop(sym, None)
        _sgrev_watch.pop(sym, None)
        SS.px_windows[sym] = deque([(now_s, px)])
    rec = []
    orig_sig = _signal
    globals()["_signal"] = lambda *a, **k: rec.append((a[2], a[3]))
    try:
        # A) sgrev arm: 5-candle grind (+5.1%), px mid → SHORT watch at the red low
        barsA = _sg_bars(5, 1.0)
        _sg_setup(barsA, barsA[-1][3] + 0.2)
        _sgrev_scan(None, now_s)
        w = _sgrev_watch.get(sym)
        if not w or w["trade_dir"] != "down" or abs(w["level"] - barsA[-1][3]) > 1e-9:
            problems.append("sgrev: 5-candle grind must arm a SHORT watch at the red low")
        # B) sgrev stale: break before discovery → skipped
        _sg_setup(barsA, barsA[-1][3] - 0.3)
        _sgrev_scan(None, now_s)
        if _sgrev_watch.get(sym): problems.append("sgrev: stale break must be skipped")
        # C) sgrev fire: armed, LIVE px breaks the red low → SHORT
        _sg_setup(barsA, barsA[-1][3] + 0.2)
        _sgrev_scan(None, now_s)
        rec.clear()
        SS.px_windows[sym] = deque([(now_ts(), barsA[-1][3] - 0.1)])
        _sgrev_scan(None, now_ts())
        if rec != [("SGREV", "down")]:
            problems.append(f"sgrev: live low-break must fire SHORT (got {rec})")
        if _sgrev_watch.get(sym): problems.append("sgrev: watch must be consumed on fire")
        # D) sgrev kill: px back above the red HIGH → reversal failed
        _sg_setup(barsA, barsA[-1][3] + 0.2)
        _sgrev_scan(None, now_s)
        SS.px_windows[sym] = deque([(now_ts(), barsA[-1][2] + 0.1)])
        _sgrev_scan(None, now_ts())
        if _sgrev_watch.get(sym): problems.append("sgrev: grind resuming must kill the watch")
        # E) sgrev threshold: 4-candle grind +3.7% → below 4% → no arm
        _sg_setup(_sg_bars(4, 0.9), 103.0)
        _sgrev_scan(None, now_s)
        if _sgrev_watch.get(sym): problems.append("sgrev: <4% grind must not arm")
        # F) sgrev: 8-candle grind (no upper limit) → arms
        _sg_setup(_sg_bars(8, 0.55), 104.5)
        _sgrev_scan(None, now_s)
        if not _sgrev_watch.get(sym): problems.append("sgrev: 8-candle grind must arm (no max)")
        # G) sgrev mirror: 5 red candles (−4.6%) → live high-break fires LONG
        o0 = 200.0
        reds = []
        for k in range(5):
            o = o0 - k * 1.8; cl = o - 1.8
            reds.append((c1_open_ms - (5 - k) * 900000, o, o + 0.1, cl - 0.1, cl))
        go = o0 - 5 * 1.8
        reds.append((c1_open_ms, go, go + 1.0, go - 0.1, go + 0.8))
        _sg_setup(reds, go + 0.2)
        _sgrev_scan(None, now_s)
        rec.clear()
        SS.px_windows[sym] = deque([(now_ts(), reds[-1][2] + 0.1)])
        _sgrev_scan(None, now_ts())
        if rec != [("SGREV", "up")]:
            problems.append(f"sgrev: down-grind high-break must fire LONG (got {rec})")
        # H) sgrind continuation restored: same grind, break ALREADY happened at
        #    discovery → fires WITH the trend immediately...
        _sg_setup(barsA, barsA[-1][3] - 0.1)
        _sg_scan(None, now_s)
        if rec[-1:] != [("SGRIND", "up")]:
            problems.append(f"sgrind: continuation must fire LONG on extension (got {rec[-1:]})")
        _sgrev_scan(None, now_s)   # ...and SGREV skips the same setup as stale
        if _sgrev_watch.get(sym): problems.append("sgrev: must not arm a stale setup")
        # I) sgrind: 7-candle streak → rejected (v4.6.4 nuance: max 6)
        _sg_setup(_sg_bars(7, 1.0), 100.0 + 7 * 1.0)
        rec.clear()
        _sg_scan(None, now_s)
        if rec: problems.append(f"sgrind: 7-candle streak must be rejected (got {rec})")
    finally:
        globals()["_signal"] = orig_sig
        _k15.pop(sym, None); SS.px_windows.pop(sym, None)
        _sg_last_bar.pop(sym, None); _sgrev_last_bar.pop(sym, None)
        _sgrev_watch.pop(sym, None)
    _SDB_PATH = prev_db
    try: os.unlink(tmp.name)
    except Exception: pass
    return problems

def selftest():
    """Behavior + numeric + wiring + REMOVALS. Run: python shadow_trader.py selftest"""
    import inspect
    problems = []
    if not _smoke_test_db(): problems.append("DB smoke test failed")
    if not _migration_test_db(): problems.append("migration test failed")
    problems += _numeric_tests()
    required_defs = [
        "seed_worker", "_seed_one", "_record_tick", "_spawn_seed",
        "k_cache_refresher", "_dd2_scan", "_dd2_classify", "_dipx_scan", "_sg_scan",
        "_sgrev_scan",
        "_burst_scan", "_c2_open",
        "_pf_scan", "_pf_confirm_relong", "_fb_scan", "_sq_scan",
        "_follower_build_prelist", "_follower_scan", "_follower_confirm", "_dayopen_scan",
        "now_measure_worker", "fetch_horizon_price", "funding_ts_worker",
        "_funding_squeeze_arm", "_rev_episode_scan", "_rev_tick_ep", "_rev_resolve",
        "_rev_oi_instrument", "book_manager", "market_recorder",
        "trade_reader", "_record_trade", "_explosion_accumulate", "_vol_median_refresher",
        "_check_explosion_fills", "_explosion_open",
        "_lane_cooldown_ok", "_set_lane_cooldown",
        "_signal", "_emit_signal", "_confirm_and_open", "_final_admission",
        "signal_consumer", "manage_position", "_manage_follower",
        "open_position", "close_position", "_set_pos_flag", "_save_excursions",
        "fetch_candles", "_ladder_floor", "_early_cut", "_is_systemic",
        "fetch_vol_and_high", "fetch_oi_change", "fetch_oi_now", "fetch_book_data",
        "fetch_funding_rate", "rank_candidate", "live_price",
        "_brain_snapshot", "_brain_agrees", "_dip_budget_ok", "_regime_tag",
        "_mark_signal", "_cofire_overlap",
        "poll_loop", "stream_reader", "tg_report_worker", "build_report_text",
        "report", "selftest", "_smoke_test_db", "_migration_test_db", "run", "main",
        "_px_ago", "_extremes", "stream_price", "notional_for", "roundtrip_cost",
        "decide", "_ws_reset",
    ]
    for name in required_defs:
        if name not in globals(): problems.append(f"missing function: {name}")
    # REMOVALS — must NOT exist (v4.7 deletions; presence = wrong build)
    for name in ("ws_force_reader", "_on_liquidation", "_check_whale_exhaustion",
                 "symbol_consecutive_losses", "symbol_daily_loss_usd", "symbol_daily_loss_count",
                 "_rearm_ok", "_over_extended", "_vol_stop", "_brain_blocks",
                 "fetch_5m_avg_range", "_hl_range_pct",
                 "_trade_pace", "_explosion_check", "_is_vertical", "_direction_cap_ok",
                 "_whale_check"):
        if name in globals(): problems.append(f"REMOVED function still present: {name}")
    for attr in ("flow3s", "liq_flow", "whale_state", "q_hist", "vol_stop_cache",
                 "prev_q24", "move_state", "trade_ids", "trade_volume"):
        if hasattr(SS, attr): problems.append(f"REMOVED StreamState attr still present: {attr}")
    if "WHALE_REV" in LANE_PRIO: problems.append("WHALE_REV still in LANE_PRIO")
    required_globals = [
        "SL_PCT", "LADDER_ARM_PCT", "LADDER_GAP", "LADDER_GAP_AT", "LADDER_GAP_BIG",
        "EARLY_CUT_PCT", "REV_DUMP_PCT", "REV_BOUNCE_PCT", "REV_WATCH_SEC",
        "REV_CUTOVER_N", "SYS_BTC_DROP_PCT", "DIP_BUDGET", "DIP_LANES",
        "FS_DUMP_PCT", "FS_BOUNCE_PCT", "FUNDING_EXTREME", "DD2_DROP_PCT",
        "DD2_CANDLES", "SG_MIN_CANDLES", "SG_MAX_CANDLES", "SGREV_MIN_CANDLES",
        "SGREV_MIN_STREAK_PCT", "PF_MIN_RUN_PCT", "PF_RELONG_OI_DROP",
        "BURST_BODY_PCT", "BURST_GAP_MAX_PCT", "BURST_COOLDOWN_SEC",
        "EXPLOSION_VOL_X", "EXPLOSION_PULLBACK_PCT", "VOL_MEDIAN_REFRESH_SEC",
        "LANE_PRIO", "LANE_SIZE_MULT", "RULES_VER", "NEXUS_ROLE", "IS_SCANNER",
        "IS_TRADER", "MAX_CONCURRENT", "RISK_DOLLARS", "SESSION_SIZES",
        "MARKET_DB_PATH", "ENABLE_DEEPDIP_V2", "ENABLE_FAILBREAK", "ENABLE_SQUEEZE",
        "ENABLE_FOLLOWER", "ENABLE_BURST", "ENABLE_EXPLOSION",
        "RECORDER_TOP_N", "REC_BUF_CAP",
        "FLEX_CD_LANES", "LANE_COOLDOWN_WIN_SEC", "LANE_COOLDOWN_MAX_SEC",
    ]
    for name in required_globals:
        if name not in globals(): problems.append(f"missing global: {name}")
    for attr in ("px_windows", "q24", "rev_1s", "rev_eps", "bid", "t1s", "watch",
                 "waitroom", "pendings", "exp_fired", "exp_acc", "confirming",
                 "rec_buf", "rec_bid_buf", "rec_syms", "rec_episodes", "sig_log",
                 "btc_px"):
        if not hasattr(SS, attr): problems.append(f"StreamState missing: {attr}")
    wiring = [
        ("_ladder_floor", "manage_position"), ("_early_cut", "manage_position"),
        ("_manage_follower", "manage_position"), ("FOLLOWER", "manage_position"),
        ("_explosion_accumulate", "stream_reader"), ("prev_q", "stream_reader"),
        ("_check_explosion_fills", "_check_stream_triggers"),
        ("_vol_median_refresher", "run"),
        ("_rev_episode_scan", "_check_stream_triggers"),
        ("_is_systemic", "_rev_episode_scan"), ("_dip_budget_ok", "_rev_episode_scan"),
        ("_funding_squeeze_arm", "_check_stream_triggers"),
        ("_record_trade", "trade_reader"), ("30.0", "trade_reader"),
        ("_emit_signal", "_signal"), ("_final_admission", "_confirm_and_open"),
        ("_final_admission", "_explosion_open"), ("notional_for", "open_position"),
        ("paper", "open_position"), ("episode_id", "open_position"), ("regime", "open_position"),
        ("_set_lane_cooldown", "close_position"), ("_lane_cooldown_ok", "_signal"),
        ("SGRIND", "_final_admission"),
        ("dip_budget_atomic", "open_position"), ("_regime_tag", "close_position"),
        ("entry_ver", "close_position"), ("sl_pct", "close_position"),
        ("_dd2_scan", "poll_loop"), ("_dipx_scan", "poll_loop"), ("_sg_scan", "poll_loop"),
        ("_sgrev_scan", "poll_loop"), ("SGREV", "_final_admission"),
        ("_burst_scan", "poll_loop"), ("ENABLE_BURST", "_burst_scan"),
        ("_pf_scan", "poll_loop"), ("_fb_scan", "poll_loop"), ("_sq_scan", "poll_loop"),
        ("_follower_scan", "poll_loop"), ("_dayopen_scan", "poll_loop"),
        ("signal_consumer", "run"), ("NEXUS_ROLE == \"trader\"", "run"),
        ("market_recorder", "run"), ("book_manager", "run"),
        ("k_cache_refresher", "run"), ("_mark_signal", "_pf_scan"),
        ("fetch_oi_now", "_rev_oi_instrument"), ("fetch_oi_now", "_pf_confirm_relong"),
        ("_brain_agrees", "rank_candidate"), ("LANE_PRIO", "rank_candidate"),
        ("IS_SCANNER", "poll_loop"), ("IS_TRADER", "poll_loop"),
        ("all_open_positions", "poll_loop"), ("paper=1", "build_report_text"),
    ]
    for needle, fn_name in wiring:
        fn = globals().get(fn_name)
        if fn is None: continue
        try: src = inspect.getsource(fn)
        except Exception: problems.append(f"cannot inspect {fn_name}"); continue
        if needle not in src: problems.append(f"WIRING: {fn_name} does not reference {needle}")
    absent = [
        ("WHALE", "_signal"), ("_rearm_ok", "_signal"),
        ("SYMBOL_DAILY_LOSS", "_signal"), ("SYMBOL_DAILY_LOSS", "_final_admission"),
        ("SYMBOL_DAILY_LOSS", "open_position"), ("_rearm_ok", "open_position"),
        ("GUARD", "manage_position"), ("TRAIL", "manage_position"),
        ("_vol_stop", "_confirm_and_open"), ("_over_extended", "_confirm_and_open"),
        ("alertbot_now_opposes", "_signal"), ("_brain_blocks", "_signal"),
    ]
    for needle, fn_name in absent:
        fn = globals().get(fn_name)
        if fn is None: continue
        try: src = inspect.getsource(fn)
        except Exception: continue
        if needle in src: problems.append(f"ABSENCE: {fn_name} still references {needle}")
    if problems:
        print("SELFTEST FAILED:")
        for p in problems: print(f"  ✗ {p}")
        return False
    print(f"SELFTEST PASSED — behavior + numeric + wiring + removals OK (v{RULES_VER}, role {NEXUS_ROLE})")
    return True

# ================================================================ MAIN
def _task_alarm(t):
    if t.cancelled(): return
    e = t.exception()
    if e:
        print(f"[FATAL] core task crashed: {e!r}")
        traceback.print_exception(type(e), e, e.__traceback__)
        _shutdown.set()   # a dead worker stops this process — no half-blind trading

async def run():
    global SESSION_M
    sdb_init()
    if not selftest():
        print("[abort ] selftest failed — not taking the ledger"); sys.exit(1)
    takeover()
    print("=" * 66)
    print(f"NEXUS INERTIA TRADER {RULES_VER} — role={NEXUS_ROLE} · ladder exits · mechanism lanes")
    print(f"  LIVE : ⚡REV v3(2.5%/60s→bid+0.75%) · 🧲FSQZ(2%/1%, half) · 🕳🕳DIP-X(half) · 🐌SGRIND(cont)·🔄SGREV(rev) · 🚀PFADE")
    print(f"  LIVE : 🕳DEEPDIP v2 · 🪤FAILBREAK · 🧨SQUEEZE · 🐦FOLLOWER · 🔆BURST · 💥EXPLOSION(v4.6.4 machine, full)")
    print(f"  v4.8 : sessions flat · no direction caps · no vertical veto · flexible cooldowns (≤1h)")
    print(f"  INSTR: 🫗OI_FLUSH(episodes) · LOG: ⏰FUNDING_TS · 🌅DAYOPEN · DEAD: 🐋WHALE_REV")
    print(f"  exits: stop -{SL_PCT}% HARD · ladder {LADDER_ARM_PCT}/{LADDER_GAP}→{LADDER_GAP_AT}/{LADDER_GAP_BIG} · cut -{EARLY_CUT_PCT}%")
    print(f"  system: dip budget {DIP_BUDGET} · co-fire audit ±{int(CO_FIRE_WINDOW/60)}m · regime tags · "
          f"no benching (deliberate)")
    print(f"  recorder: {MARKET_DB_PATH} (1s, top-{RECORDER_TOP_N}+episodes, {RECORDER_RETAIN_DAYS}d)")
    print(f"  ledger: {SHADOW_DB_PATH} · tg {'on' if TG_ENABLED else 'OFF'} · report --all")
    print("=" * 66)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try: loop.add_signal_handler(sig, _shutdown.set)
        except (NotImplementedError, AttributeError): pass
    async with aiohttp.ClientSession() as session:
        SESSION_M = session
        print(f"[ready] v{RULES_VER} armed — role {NEXUS_ROLE} · selftest passed")
        notify(session, f"🚀 NEXUS {RULES_VER} armed (role {NEXUS_ROLE}, selftest OK)\n"
                        f"ALL 11 lanes LIVE: REV v3·FSQZ·DIP-X·SGRIND·SGREV·PFADE·DDv2·FAILBREAK·SQUEEZE·FOLLOWER·BURST·EXPLOSION\n"
                        f"flat sessions · no direction caps · flexible cds ≤1h · stop -2.2% · ladder 0.8/0.5→3.0/1.0\n"
                        f"ledger {SHADOW_DB_PATH} · pid {os.getpid()}@{_host()}")
        scanner_workers = [
            asyncio.ensure_future(trade_reader()),
            asyncio.ensure_future(_vol_median_refresher(session)),   # EXPLOSION medians
            asyncio.ensure_future(book_manager()),
            asyncio.ensure_future(market_recorder()),
            asyncio.ensure_future(k_cache_refresher(session)),
            asyncio.ensure_future(_follower_build_prelist(session)),
            asyncio.ensure_future(funding_ts_worker(session)),   # reads scanner-side pends
        ]
        trader_workers = [
            # signal_consumer SELF-RETURNS outside the trader role, and
            # asyncio.wait(..., FIRST_COMPLETED) below treats ANY completed
            # worker as shutdown — so in role=both it killed the whole process
            # ~1s after startup and the supervisor restart-looped it forever
            # (the v4.7.1 Render crash loop; the Telegram "armed" message was
            # cancelled mid-send every cycle). It exists only in split mode.
            *([asyncio.ensure_future(signal_consumer())] if NEXUS_ROLE == "trader" else []),
            asyncio.ensure_future(now_measure_worker(session)),
            asyncio.ensure_future(tg_report_worker(session)),
        ]
        # ticker feed + seeding run in EVERY role: a trader without price
        # windows was blind (stream_price None, _btc_hot pinned, _is_vertical
        # fail-open). Detection inside stream_reader is IS_SCANNER-gated.
        shared = [asyncio.ensure_future(poll_loop(session)),      # role-gated inside
                  asyncio.ensure_future(stream_reader()),
                  asyncio.ensure_future(seed_worker(session))]
        tasks = ([t for t, on in zip(scanner_workers, [IS_SCANNER] * len(scanner_workers)) if on] +
                 [t for t, on in zip(trader_workers, [IS_TRADER] * len(trader_workers)) if on] +
                 shared)
        for t in tasks: t.add_done_callback(_task_alarm)
        shut_wait = asyncio.ensure_future(_shutdown.wait())
        try:
            await asyncio.wait([*tasks, shut_wait], return_when=asyncio.FIRST_COMPLETED)
        finally:
            _shutdown.set()
            for t in tasks:
                if not t.done(): t.cancel()
            extras = [t for grp in (_entry_tasks, _bg_tasks, _tg_tasks) for t in list(grp) if not t.done()]
            for t in extras: t.cancel()
            await asyncio.gather(*tasks, shut_wait, *extras, return_exceptions=True)
            _clear_hb(); print("[exit ] heartbeat cleared")
def main():
    if len(sys.argv) > 1 and sys.argv[1] == "selftest": sys.exit(0 if selftest() else 1)
    if len(sys.argv) > 1 and sys.argv[1] == "report":
        report(all_versions=("--all" in sys.argv)); return
    try: asyncio.run(run())
    except KeyboardInterrupt: pass
if __name__ == "__main__":
    main()