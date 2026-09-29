#!/usr/bin/env python3
"""
Nexus v5.1.1 — memory-bounded Binance futures intelligence bot.
Credentials via env ONLY: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID.
Python 3.12+, deps: aiohttp, websockets, numpy (optional, smoke-only).
No scikit-learn. No models. No pickles. No retrains.

v5.1.1 (this build) fixes an outcome-integrity hole in v5.1:
- band_outcomes upserts can no longer overwrite a resolved hit
- feed gaps during a band are recorded as 'incomplete' (hit NULL), not as timeout
- ticker events use exchange event time, not local clock
- REST-seeded klines use bar-open + interval to match the live aggregator
- taker-buy quote volume read from kline column [10]
- alerts refuse to fire on a stale price feed

v5.2.0 (this build) — scanner & pipeline repair:
- push decision uses the best call across ALL bands; a thin 30m prior no longer
  suppresses the whole alert (30m stays the headline of the prediction line)
- trigger relaxation: CAND_REENTRY_SEC 180->90 (env), cooldown scale 0.75 (env),
  mass dump/pump ban 1800->600s (env)
- tracking tolerates a quieter feed than firing: band coverage uses a 120s
  freshness window (env) and a 60s sample-gap instead of 30s/10s
- a band that loses live coverage is rebuilt from 1m klines before it is
  recorded 'incomplete' (resolution_source nexus_kline_reconstruct)
- band-end TRACK edits retry on failure instead of being marked sent up-front
- forecast/risk fall back to bootstrap rows while live buckets are thin
- dead rev30/reversal tracking removed (no trigger ever existed for it)
- LEAN confirm-hold runs as its own task instead of parking an alert worker
- Telegram edits can no longer starve behind sends (3s max edit wait)
- alert queue default 64->128 (env)
"""

from __future__ import annotations
import asyncio, base64, gc, io, json, logging, math, os, re, signal, sqlite3, statistics, time, zipfile
from array import array
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote
from zoneinfo import ZoneInfo

import aiohttp
from aiohttp import web
import websockets

try:
    import orjson as _orjson
    _loads = _orjson.loads
except Exception:
    _loads = json.loads

NEXUS_VERSION = "5.2.0"
NEXUS_BUILD_ID = "nexus-5.2.0-scanner-pipeline"
OUTCOME_SCHEMA_VERSION = "nexus2_hit_only_entry10_v1"
FEATURE_SCHEMA_VERSION = 22
NOTIFICATION_VERSION = "nexus7_v5_2_0_v1"

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("nexus")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
COINMARKETCAP_API_KEY = os.getenv("COINMARKETCAP_API_KEY", "")

VIENNA_TZ = ZoneInfo("Europe/Vienna")
FAPI = "https://fapi.binance.com"
WS_BASES = ["wss://fstream.binance.com/market", "wss://fstream.binance.com"]
CMC_LISTINGS_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
VISION_BASE = "https://data.binance.vision"

PORT = int(os.getenv("PORT", os.getenv("HEALTH_PORT", "8080")))
DATA_DIR = "/data" if os.path.isdir("/data") else "."
DB_PATH = os.getenv("PERFORMANCE_DB_PATH", f"{DATA_DIR}/alerts_performance.db")
STATE_SNAPSHOT_PATH = os.getenv("STATE_SNAPSHOT_PATH", f"{DATA_DIR}/bot_state.json")

GOVERNOR_ENABLED = os.getenv("NEXUS_GOVERNOR_ENABLED", "1").lower() not in {"0", "false", "no"}
GOV_PUSH_GAP_SEC = int(os.getenv("NEXUS_SYM_PUSH_GAP_SEC", "1200"))
GOV_GAP_BYPASS_RANK = 6
GOV_FLIP_MIN_P = 0.65
GOV_SNAPSHOT_DEDUP_SEC = 90

PARABOLIC_ENABLED = os.getenv("NEXUS_PARABOLIC_ENABLED", "1").lower() not in {"0", "false", "no"}
PARABOLIC_EXT_1H = 20.0
PARABOLIC_EXT_24H = 60.0
PARABOLIC_MAX_AGE_D = 7.0
PARABOLIC_MAX_MCAP = 15_000_000
PARABOLIC_P_CAP = 0.60
PARABOLIC_DIV_CAP = 0.75
PARABOLIC_CHASE_SUPPRESS = 60

WASH_ENABLED = os.getenv("NEXUS_WASH_ENABLED", "1").lower() not in {"0", "false", "no"}
WASH_CV_MAX = 0.25
WASH_MIN_TRADES = 50

TIMEFRAME_RULES = [(300, 3.5, "5m", 600), (900, 8.0, "15m", 1200),
                   (3600, 15.0, "1h", 3600), (14400, 22.0, "4h", 10800)]
FLEXIBILITY_PCT = 0.5
TIMEFRAME_TARGET_PCT = {"5m": 3.5, "15m": 8.0, "1h": 15.0, "4h": 22.0}
VOLUME_GATE = 1_000_000
TF_SEC = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400}

BANDS = [("now", "NOW", 0, 5, 0.45), ("30m", "30M", 5, 30, 1.20),
         ("1h", "1H", 30, 60, 1.80), ("4h", "4H", 60, 240, 3.00)]
BAND_SPANS = [(b[0], b[1], b[2], b[3], b[4]) for b in BANDS]
BANDS_ORDER = [b[0] for b in BANDS]
BANDS_NAME = {b[0]: b[1] for b in BANDS}
BAND_DELAY_SEC = 10
TRACK_TICK_SEC = 3.0
BAND_END_EDIT_ONLY = True
FAST_HIT_SEC = 45.0
FEED_STALE_SEC = 30.0
# v5.2.0: tracking tolerates a quieter feed than alert firing — a short ticker
# lull must not poison a whole band as 'incomplete'.
TRACK_FRESH_SEC = float(os.getenv("NEXUS_TRACK_FRESH_SEC", "120"))
TRACK_MAX_SAMPLE_GAP_SEC = 60.0
TRACK_RECOVERY_INTERVAL_SEC = 30.0
TRACK_RECOVERY_LOOKBACK_SEC = 5 * 3600 + 600

BTC_SYMBOL = "BTCUSDT"
ENABLE_BTC_CORR = True
BTC_CORR_MARKET_DRIVEN = 0.60
BTC_CORR_MIN_SAMPLES = 20
BTC_MOVE_SAME_DIR_PCT = 1.5
CORR_SEED_SYMBOLS = int(os.getenv("NEXUS_CORR_SEED_SYMBOLS", "400"))
CORR_SEED_REFRESH_SEC = 3600

MASS_DUMP_MIN_COINS = 10
MASS_DUMP_COOLDOWN_SEC = 1800
MASS_SUPPRESS_SEC = int(os.getenv("NEXUS_MASS_SUPPRESS_SEC", "600"))

LIQUIDATION_MIN_USD = 150_000
LIQUIDATION_PCT_OF_VOL = 0.005

FUNDING_POLL_SEC = 300

NEW_LISTING_MAX_AGE_H = 24
EXTREME_MAX_PER_DAY = 1
EXTREMES_LABEL = "30d"
KLINES_REFRESH_SEC = 12 * 3600
KLINES_LIMIT = 30
REST_DELAY = 0.18

PEAK_MIN_GAIN, PEAK_MIN_DROP, PEAK_COOLDOWN = 30.0, 18.0, 4 * 3600
TOPGAIN_MIN_DAY, TOPGAIN_DROPOFF, TOPGAIN_STILL = 15.0, 4.0, 10.0
LOWCAP_CAP_MAX, LOWCAP_VOL_MIN, LOWCAP_SURGE_X, LOWCAP_SPIKE_PCT, LOWCAP_COOLDOWN = 15_000_000, 250_000, 30.0, 4.0, 3600
CHOP_MAX_PUSHES = 4
CHOP_WINDOW_SEC = 6 * 3600
BTC_BAND_ENABLED = True
BTC_BAND_MOVE_USD = 2000.0
BREAKOUT_WINDOW = {"5m": 3600, "15m": 3600, "1h": 14400, "4h": 86400}

CALL_STRONG = 0.70
CALL_ACTIONABLE = 0.62
CALL_LEAN = 0.53
STRONG_HIST_FLOOR = 0.66

NOTIFY_MIN_P = 0.45
REALERT_MIN_DELTA = 0.06
REALERT_P_EXPIRY_SEC = 86400.0
CAND_REENTRY_SEC = int(os.getenv("NEXUS_CAND_REENTRY_SEC", "90"))
LEAN_BATCH_SEC = 1800
BURST_WINDOW_SEC = 90.0
BURST_MIN_COINS = 5
WAVE_DIGEST_FLUSH_SEC = 75.0
WAVE_DIGEST_MAX_ITEMS = 12
CONFIRM_HOLD_SEC = 45.0
COOLDOWN_MAX_MULT = 1.5
COOLDOWN_SCALE = float(os.getenv("NEXUS_COOLDOWN_SCALE", "0.75"))
FLIP_MIN_P = 0.65
FLIP_FRESH_SEC = 3600.0
ESCALATION_WINDOW_SEC = 600.0
ESCALATION_MIN_GAIN = 0.05
HOT_LIST_CAP = 44
MAX_SILENT_TRACKS = 600
VOL_DISPLAY_CAP = 30.0
FUNDING_EXTREME = 0.005
FUNDING_SQUEEZE = 0.002
THIN_VOL_1M_X = 0.7
THIN_VOL_PACE_X = 0.9

SCALP_VOL_MIN_USD = 500_000
SCALP_VOL_FAST_USD = 200_000
SCALP_VOL_MOVE_PCT = 2.0
SCALP_VOL_BOOST = 0.03
SCALP_VOL_DIR_RATIO = 0.55
SCALP_VOL_COOLDOWN = 300
SCALP_VOL_MAX_AGE_SEC = 7200

DIV_SCALP_MIN_EXH = 55
DIV_HEUR_BASE = 0.34
DIV_HEUR_SLOPE = 0.008
DIV_HEUR_CAP = 0.65

P_DISPLAY_CAP = 0.92
DIV_MIN_WEIGHT = 15.0
DIV_CAP_SMALL = 0.75
DIV_CAP_MATURE = 0.85
DIV_PRIOR = 0.55
HIST_PRIOR_LO, HIST_PRIOR_HI = 0.35, 0.65
RETAINED_ALPHA_DECAY_SEC = 3 * 86400

CHASE_T0 = 35.0
CHASE_SLOPE = 0.30 / (100.0 - CHASE_T0)

RISK_RR_LOW = 2.0
RISK_RR_MED = 1.3
RISK_RR_HIGH = 0.8

EXH_TIER_MATURE = 40
EXH_TAG_MIN = 60
EXH_TIER_CLIMAX = 75
EXTENSION_WINDOW_SEC = 3600
EXH_EXTENSION_Z = 2.0
LIQ_PEAK_FLOOR_USD = 25_000
RS_TOUCH_PCT = 0.5
RS_MAX_SHOWN_PCT = 15.0

MOMENTUM_BRIDGE = True
MOMENTUM_MAX_PTS = 4.0
BAND_MOM_SCALE = {"5m": 1.0, "15m": 0.7, "1h": 0.5, "4h": 0.25}

NON_CRYPTO_BASES = {b.strip().upper() for b in os.getenv("NON_CRYPTO_BASES", "").split(",") if b.strip()} | {
    "TSLA", "NVDA", "AAPL", "AMZN", "MSFT", "COIN", "META", "GOOGL", "MSTR",
    "SPY", "QQQ", "CRCL", "HOOD", "PLTR", "NFLX", "AMD", "INTC", "DIS", "BA",
    "GME", "AMC", "ORCL", "AVGO", "SMCI", "RKLB", "SLV", "GLD", "TLT", "DIA",
}

BUCKET_WINDOW_DAYS = 30
HIST_MIN_EFF, HIST_BASE_MIN_EFF, HIST_SHRINKAGE = 5.0, 20.0, 30.0

SOURCE_WEIGHTS = {"nexus_tick": 1.00, "nexus_1m": 0.72, "nexus_restart_1m": 0.58,
                  "nexus_timeout": 0.55, "legacy_relabel_mfe": 0.00, "backfill": 0.15,
                  "nexus_kline_reconstruct": 0.85}

CLUSTER_WEIGHT_MODE = os.getenv("NEXUS_CLUSTER_WEIGHT_MODE", "sqrt")
REPORT_INTERVAL_SEC = 2 * 3600

HISTORICAL_BOOTSTRAP = os.getenv("HISTORICAL_BOOTSTRAP_ENABLED", "1").lower() not in {"0", "false", "no"}
BOOTSTRAP_DAYS = max(7, min(60, int(os.getenv("HISTORICAL_BOOTSTRAP_DAYS", "30"))))
BOOTSTRAP_WORKERS = max(1, min(12, int(os.getenv("HISTORICAL_BOOTSTRAP_SYMBOL_WORKERS", "6"))))
BOOTSTRAP_SOURCE = "backfill_30d_v4"
BOOTSTRAP_RETRAIN_EVERY = 500
BOOTSTRAP_WARMUP_MIN_SYMBOLS = 120
BOOTSTRAP_RESCAN_SEC = 3600

MEM_SOFT_CAP_MB = float(os.getenv("MEM_SOFT_CAP_MB", "380"))
SYM_CREATE_QV = 250_000
SYM_DEMOTE_QV = 150_000
ALERT_RETENTION_DAYS = int(os.getenv("ALERT_RETENTION_DAYS", "45"))
DIGEST_HOUR, DIGEST_MIN = 11, 0

TG_COOL_LEVELS = (1800.0, 3600.0, 7200.0)
REPLAY_MAX_STALE_SEC = 1800.0
REPLAY_MAX_QUEUE = 8
HEARTBEAT_SILENCE_SEC = int(os.getenv("HEARTBEAT_SILENCE_SEC", str(3 * 3600)))

ALERT_QUEUE_MAX = int(os.getenv("NEXUS_ALERT_QUEUE_MAX", "128"))
ALERT_WORKERS = max(1, min(6, int(os.getenv("NEXUS_ALERT_WORKERS", "3"))))
BOOT_BARS_SYMBOLS = max(50, min(500, int(os.getenv("NEXUS_BOOT_BARS_SYMBOLS", "300"))))

# ---------------------------------------------------------------- SQL constants
BAND_OUTCOMES_CREATE = """CREATE TABLE IF NOT EXISTS band_outcomes(
    alert_id INTEGER NOT NULL, band TEXT NOT NULL, target_pct REAL, stop_pct REAL,
    barrier_win INTEGER, hit INTEGER, outcome TEXT, mfe REAL, mae REAL, mfe_min REAL,
    mae_min REAL, first_min REAL, resolved_ts REAL, resolution_source TEXT,
    label_version TEXT NOT NULL, entry_price REAL, entry_delay_sec REAL,
    best_market_move_pct REAL, final_market_move_pct REAL,
    PRIMARY KEY(alert_id,band,label_version))"""

# v5.1.1: freeze the first resolution. A later write (restart, reconstruction,
# edit cycle) cannot silently flip a resolved outcome.
SQL_OUTCOME_UPSERT = """INSERT INTO band_outcomes(alert_id,band,target_pct,hit,outcome,
    mfe,mae,mfe_min,mae_min,first_min,resolved_ts,resolution_source,label_version,entry_price,
    entry_delay_sec,final_market_move_pct)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(alert_id,band,label_version) DO UPDATE SET
    target_pct=excluded.target_pct, hit=excluded.hit, outcome=excluded.outcome,
    mfe=excluded.mfe, mae=excluded.mae, mfe_min=excluded.mfe_min, mae_min=excluded.mae_min,
    first_min=excluded.first_min, resolved_ts=excluded.resolved_ts,
    resolution_source=excluded.resolution_source, entry_price=excluded.entry_price,
    entry_delay_sec=excluded.entry_delay_sec,
    final_market_move_pct=excluded.final_market_move_pct
    WHERE band_outcomes.hit IS NULL"""

SQL_LEGACY_OUTCOME_INSERT = """INSERT OR IGNORE INTO band_outcomes(alert_id,band,target_pct,hit,
    outcome,mfe,mae,mfe_min,mae_min,first_min,resolved_ts,resolution_source,label_version,
    entry_price,entry_delay_sec) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

SQL_BACKFILL_OUTCOME_INSERT = """INSERT OR IGNORE INTO band_outcomes(alert_id,band,target_pct,hit,
    outcome,mfe,mae,first_min,resolved_ts,resolution_source,label_version,entry_price,
    entry_delay_sec) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"""

# ---------------------------------------------------------------- state
_shutdown = asyncio.Event()
SESSION: Optional[aiohttp.ClientSession] = None
_BOOT_TS = time.time()
_BACKGROUND_TASKS: set[asyncio.Task] = set()
_last_train_done_ts = 0.0
_alert_queue_hwm = 0
_last_fire_alert_error = ""

def now_ts() -> float: return time.time()

def spawn(coro) -> asyncio.Task:
    t = asyncio.create_task(coro)
    _BACKGROUND_TASKS.add(t)
    t.add_done_callback(_BACKGROUND_TASKS.discard)
    return t

def rss_mb() -> float:
    try:
        with open("/proc/self/statm") as f:
            return int(f.read().split()[1]) * 4096 / 1e6
    except Exception:
        return 0.0

def db_size_mb() -> float:
    tot = 0
    for suf in ("", "-wal", "-shm"):
        try: tot += os.path.getsize(DB_PATH + suf)
        except OSError: pass
    return tot / 1e6

_stats = {k: 0 for k in ("ws_messages", "tickers", "alerts_sent", "alerts_suppressed",
    "alerts_batched", "alerts_burst_collapsed", "alerts_realert_blocked",
    "alerts_confirm_cancelled", "alerts_confirm_ok", "symbols_blocked_noncrypto",
    "alert_queue_drops", "telegram_failures", "div_rows",
    "tracking_registered", "tracking_resolved", "tracking_edits", "tracking_errors",
    "backfill_events", "trigger_candidates", "trigger_cooldown_blocks",
    "trigger_mass_blocks", "trigger_pump_blocks", "trigger_btc_blocks",
    "telegram_429", "telegram_429_capped", "alert_delivery_failures",
    "alerts_replayed", "alerts_replay_dropped", "thin_volume_caps", "whale_alerts",
    "flip_blocks", "chop_blocks", "escalation_blocks",
    "chase_discounted", "chase_discount_pts",
    "memory_pressure_events", "symbols_demoted",
    "exh_tagged",
    "shadow_logged", "heartbeats_sent",
    "wash_flags", "parabolic_caps", "parabolic_suppressed",
    "gov_muted", "gov_bypassed", "gov_deduped", "unclear_suppressed",
    "fire_alert_errors", "prior_backfill_fallbacks", "tracking_incomplete")}
_stats["ws_ok"] = {"ticker": False, "force": False, "hot": False}

_last_delivered_alert_ts = _BOOT_TS
_tg_blocked_until = 0.0
_tg_cool_level = 0
_replay_queue: dict[str, tuple] = {}
_cooldown_block_log: Counter = Counter()
_cooldown_block_distinct: set[str] = set()
_last_delivered_p: dict[str, tuple] = {}
_last_push_p: dict[str, tuple] = {}
_sym_push_log: dict[str, deque] = {}
_lean_queue: dict[str, tuple] = {}
_burst_ledger: dict[tuple, deque] = {}
_burst_active_until: dict[tuple, float] = {}
_wave_pending: dict[str, tuple] = {}
_alert_queue: asyncio.Queue = asyncio.Queue(maxsize=ALERT_QUEUE_MAX)
_last_candidate_ts: dict[str, float] = {}
_delivery_retry_after: dict[str, float] = {}
_noncrypto_seen: set[str] = set()
_whale_state: dict[str, deque] = {}
_whale_badge_ts: dict[str, float] = {}

_last_sym_push_ts: dict[str, float] = {}
_last_sym_snapshot: dict[str, tuple] = {}

_rest_block_until = 0.0
def rest_ok() -> bool: return now_ts() >= _rest_block_until
def rest_guard(status: int):
    global _rest_block_until
    if status in (418, 429):
        _rest_block_until = now_ts() + 900
        log.warning("REST breaker: HTTP %s → fapi paused 15 min", status)

async def bounded_gather(factories, limit: int = 4):
    sem = asyncio.Semaphore(limit)
    async def _run(f):
        async with sem:
            try:
                return await f()
            except Exception as e:
                log.debug("bounded_gather task: %s", e)
                return None
    await asyncio.gather(*[_run(f) for f in factories])

# ---------------------------------------------------------------- compact series
class Ring:
    __slots__ = ("ts", "px", "n", "i", "cap")
    def __init__(self, cap: int):
        self.ts = array("d", [0.0]) * cap; self.px = array("d", [0.0]) * cap
        self.cap = cap; self.n = 0; self.i = 0
    def push(self, t, v):
        i = self.i; self.ts[i] = t; self.px[i] = v; i += 1
        self.i = i if i < self.cap else 0
        if self.n < self.cap: self.n += 1
    def at_or_before(self, t):
        n = self.n
        if n == 0: return None
        if self.ts[(self.i - n) % self.cap] > t: return None
        for k in range(n):
            idx = (self.i - 1 - k) % self.cap
            if self.ts[idx] <= t:
                v = self.px[idx]
                if v > 0: return v
        return None
    def minmax(self, lo, hi):
        mn = mx = None; cnt = 0
        for k in range(self.n):
            idx = (self.i - 1 - k) % self.cap
            t = self.ts[idx]
            if t < lo: break
            if t <= hi:
                v = self.px[idx]; cnt += 1
                if mx is None or v > mx: mx = v
                if mn is None or v < mn: mn = v
        return mn, mx, cnt

class BarRing:
    __slots__ = ("ts", "o", "h", "l", "c", "qv", "n", "i", "cap")
    def __init__(self, cap: int):
        for a in ("ts", "o", "h", "l", "c", "qv"):
            setattr(self, a, array("d", [0.0]) * cap)
        self.cap = cap; self.n = 0; self.i = 0
    def push(self, t, o, h, l, c, qv):
        i = self.i
        self.ts[i] = t; self.o[i] = o; self.h[i] = h; self.l[i] = l; self.c[i] = c; self.qv[i] = qv
        i += 1
        self.i = i if i < self.cap else 0
        if self.n < self.cap: self.n += 1
    def close_at_or_before(self, t):
        n = self.n
        if n == 0: return None
        if self.ts[(self.i - n) % self.cap] > t: return None
        for k in range(n):
            idx = (self.i - 1 - k) % self.cap
            if self.ts[idx] <= t:
                v = self.c[idx]
                if v > 0: return v
        return None
    def minmax_close(self, lo, hi):
        mn = mx = None; cnt = 0
        for k in range(self.n):
            idx = (self.i - 1 - k) % self.cap
            t = self.ts[idx]
            if t < lo: break
            if t <= hi:
                v = self.c[idx]; cnt += 1
                if mx is None or v > mx: mx = v
                if mn is None or v < mn: mn = v
        return mn, mx, cnt
    def last_bars(self, k: int):
        k = min(k, self.n); out = []
        for j in range(k):
            idx = (self.i - 1 - j) % self.cap
            out.append((self.ts[idx], self.o[idx], self.h[idx], self.l[idx], self.c[idx], self.qv[idx]))
        out.reverse(); return out

@dataclass(slots=True)
class Sym:
    sym: str
    fine: Ring
    m1: BarRing
    m5: BarRing
    px: float = 0.0
    px_ts: float = 0.0
    bid: float = 0.0
    ask: float = 0.0
    qv24: float = 0.0
    qv_acc: float = 0.0
    hi24: float = 0.0
    lo24: float = 0.0
    day_key: int = 0
    day_open: float = 0.0
    day_high: float = 0.0
    day_low: float = 0.0
    day_peak_pct: float = 0.0
    pdh: float = 0.0
    pdl: float = 0.0
    funding: float = 0.0
    fr_1h: Optional[float] = None
    fr_snap_ts: float = 0.0
    mark: float = 0.0
    indexp: float = 0.0
    basis: float = 0.0
    oi_usd: float = 0.0
    oi_ref: float = 0.0
    oi_ref_ts: float = 0.0
    ls: Optional[float] = None
    hi30: float = 0.0
    lo30: float = 0.0
    onboard: float = 0.0
    corr_cache: tuple = (0.0, None)
    sig_cache: tuple = (0.0, None)
    med_cache: tuple = (0.0, None)
    ret_cache: tuple = (0.0, None)
    leg_up_ts: float = 0.0
    leg_up_px: float = 0.0
    leg_dn_ts: float = 0.0
    leg_dn_px: float = 0.0
    alert_ts: deque = field(default_factory=lambda: deque(maxlen=20))
    last_alert_px: float = 0.0
    last_alert_ts: float = 0.0
    last_alert_dir: str = ""
    liq: Optional[deque] = None
    low_since: float = 0.0

SYMS: dict[str, Sym] = {}
TAKER: dict[str, deque] = {}
ACTIVE_TRACK_SYMS: Counter = Counter()
HOT_LIST: list[str] = []
HOT_VER = 0

def get_sym(name: str, qv: float) -> Optional[Sym]:
    s = SYMS.get(name)
    if s is not None: return s
    if qv < SYM_CREATE_QV: return None
    base = name[:-4] if name.endswith("USDT") else name
    if base.upper() in NON_CRYPTO_BASES:
        if name not in _noncrypto_seen:
            _noncrypto_seen.add(name)
            _stats["symbols_blocked_noncrypto"] += 1
        return None
    s = Sym(name, Ring(900), BarRing(130), BarRing(300))
    SYMS[name] = s
    return s

# ---------------------------------------------------------------- helpers
def config_is_valid() -> bool:
    return bool(TELEGRAM_BOT_TOKEN) and bool(TELEGRAM_CHAT_ID)

def base_of(symbol: str) -> str:
    return symbol[:-4] if symbol.endswith("USDT") else symbol

def make_trade_link(symbol: str) -> str:
    sym_q = quote(symbol.upper(), safe="")
    intent = f"bnc://app.binance.com/trade/trade?at=futures&symbol={sym_q}"
    dp = base64.b64encode(intent.encode()).decode()
    return f"https://app.binance.com/en/futures/{sym_q}?%5Fdp={dp}"

def trade_link_md(symbol: str) -> str:
    return f"[Trade {symbol} on Binance App]({make_trade_link(symbol)})"

def fmt_usd(v: float, signed=False) -> str:
    sign = "+" if signed and v > 0 else "-" if signed and v < 0 else ""
    a = abs(v)
    s = f"{a/1e9:.1f}B" if a >= 1e9 else f"{a/1e6:.1f}M" if a >= 1e6 else f"{a/1e3:.1f}K" if a >= 1e3 else f"{a:,.0f}"
    return f"{sign}${s}"

def to_f(x) -> float:
    try: return float(x)
    except Exception: return 0.0

def _vol_disp(v) -> str:
    if v is None: return "—"
    return ">30x" if v >= VOL_DISPLAY_CAP else f"{v:.1f}x"

def _clip_vol(v) -> Optional[float]:
    if v is None: return None
    return min(v, 30.0)

def fresh_price(s: Optional[Sym], now: float) -> bool:
    return bool(s and math.isfinite(s.px) and s.px > 0 and 0 <= now - s.px_ts <= FEED_STALE_SEC)

_daily_log: dict[str, deque] = {}
def allow_daily(key: str, max_count: int, window: float = 86400) -> bool:
    now = now_ts()
    dq = _daily_log.setdefault(key, deque(maxlen=256))
    cut = now - window
    while dq and dq[0] < cut: dq.popleft()
    if len(dq) >= max_count: return False
    dq.append(now); return True

def score_bucket(score: int):
    for lo, hi, lab in ((0, 39, "0-39"), (40, 59, "40-59"), (60, 79, "60-79"), (80, 100, "80-100")):
        if lo <= score <= hi: return lo, hi, lab
    return (0, 39, "0-39")

def _episode_id(symbol, direction, ts) -> str:
    return f"{symbol}:{direction}:{int(ts // 1800)}"

def _safe_json(obj) -> str:
    return json.dumps(obj, default=lambda x: float(x) if hasattr(x, "item") else str(x),
                      separators=(",", ":"))

def _parse_json(text, default):
    if not text: return default
    try: return json.loads(text)
    except Exception: return default

def _source_weight(resolution_source: str, alert_source: str = "") -> float:
    src = str(alert_source or "")
    if src.startswith("backfill"):
        return SOURCE_WEIGHTS["backfill"] if src == BOOTSTRAP_SOURCE else 0.0
    if resolution_source in SOURCE_WEIGHTS: return SOURCE_WEIGHTS[resolution_source]
    if src.startswith("legacy"): return SOURCE_WEIGHTS["legacy_relabel_mfe"]
    return 1.0

async def _shutdown_or_sleep(sec: float):
    try:
        await asyncio.wait_for(_shutdown.wait(), timeout=sec)
    except asyncio.TimeoutError:
        pass

# ---------------------------------------------------------------- telegram
_tg_lock = asyncio.Lock()
_tg_next_ok = 0.0
_tg_alert_waiters = 0
_TG_MIN_INTERVAL = 1.05
TG_EDIT_MAX_WAIT_SEC = 3.0

def _tg_retry_after(body: str) -> float:
    try:
        d = json.loads(body or "{}")
        return max(0.0, float((d.get("parameters") or {}).get("retry_after") or 0.0))
    except Exception:
        return 0.0

async def _tg_request(url: str, payload: dict, *, priority: bool, retry_429: bool = False):
    global _tg_next_ok, _tg_blocked_until, _tg_cool_level, _tg_alert_waiters
    if priority: _tg_alert_waiters += 1
    try:
        waited = 0.0
        while True:
            # v5.2.0: an edit may proceed after TG_EDIT_MAX_WAIT_SEC even while
            # sends are waiting — TRACK lines must not starve behind alerts.
            if not priority and _tg_alert_waiters > 0 and waited < TG_EDIT_MAX_WAIT_SEC:
                await asyncio.sleep(0.05); waited += 0.05; continue
            wait = max(0.0, _tg_next_ok - time.time())
            if wait > 0:
                await asyncio.sleep(wait)
                continue
            async with _tg_lock:
                if not priority and _tg_alert_waiters > 0: continue
                if _tg_next_ok > time.time(): continue
                try:
                    async with SESSION.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
                        status, body = r.status, await r.text()
                except Exception:
                    _tg_next_ok = max(_tg_next_ok, time.time() + _TG_MIN_INTERVAL)
                    raise
                _tg_next_ok = max(_tg_next_ok, time.time() + _TG_MIN_INTERVAL)
                if status != 429:
                    if status == 200: _tg_cool_level = 0
                    return status, body
                _stats["telegram_429"] += 1
                ra = _tg_retry_after(body)
                raw = (ra + 0.35) if ra > 0 else 2.0
                cap = TG_COOL_LEVELS[min(_tg_cool_level, len(TG_COOL_LEVELS) - 1)]
                back = min(raw, cap)
                if raw > cap:
                    _stats["telegram_429_capped"] += 1
                    log.warning("Telegram 429: raw retry_after %.0fs CAPPED to %.0fs (level %d)",
                                ra, back, _tg_cool_level)
                else:
                    log.warning("Telegram 429: backoff %.1fs", back)
                _tg_cool_level = min(_tg_cool_level + 1, len(TG_COOL_LEVELS) - 1)
                _tg_next_ok = max(_tg_next_ok, time.time() + back)
                _tg_blocked_until = _tg_next_ok
                return status, body
    finally:
        if priority: _tg_alert_waiters = max(0, _tg_alert_waiters - 1)

def _strip_md(text: str) -> str:
    urls: list[str] = []
    def _stash(m: re.Match) -> str:
        urls.append(m.group(0)); return f"\x00{len(urls) - 1}\x00"
    def _mdlink(m: re.Match) -> str:
        urls.append(m.group(2)); return f"{m.group(1)} \x00{len(urls) - 1}\x00"
    t = re.sub(r"\[([^\]]+)\]\((https?://[^)]+)\)", _mdlink, text)
    t = re.sub(r"https?://[^\s\x00)]+", _stash, t)
    t = re.sub(r"[*_`]", "", t)
    return re.sub(r"\x00(\d+)\x00", lambda m: urls[int(m.group(1))], t)

async def _replay_worker():
    while not _shutdown.is_set():
        await _shutdown_or_sleep(2.0)
        if _shutdown.is_set() or not _replay_queue: continue
        if now_ts() < _tg_blocked_until or now_ts() < _tg_next_ok: continue
        for rk in list(_replay_queue.keys()):
            ts_enq, text = _replay_queue[rk]
            if now_ts() - ts_enq > REPLAY_MAX_STALE_SEC:
                _replay_queue.pop(rk, None); _stats["alerts_replay_dropped"] += 1
                continue
            mid = await send_msg(text)
            if mid is not None:
                _replay_queue.pop(rk, None); _stats["alerts_replayed"] += 1
                break
            break
        await asyncio.sleep(1.05)

async def send_msg(text: str) -> Optional[int]:
    if not (config_is_valid() and SESSION): return None
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown",
               "disable_web_page_preview": True}
    try:
        status, body = await _tg_request(url, payload, priority=True, retry_429=False)
        if status == 200:
            mid = json.loads(body).get("result", {}).get("message_id")
            mid = int(mid) if mid is not None else None
            if mid is not None:
                global _last_delivered_alert_ts
                _last_delivered_alert_ts = now_ts()
            return mid
        if status == 429:
            if len(_replay_queue) < REPLAY_MAX_QUEUE:
                _replay_queue[f"msg|{hash(text) & 0xFFFFFF}"] = (now_ts(), text)
            else:
                _stats["alerts_replay_dropped"] += 1
            return None
        if status == 400 and "parse entities" in body.lower():
            s2, b2 = await _tg_request(url, {"chat_id": TELEGRAM_CHAT_ID, "text": _strip_md(text),
                                             "disable_web_page_preview": True}, priority=True, retry_429=False)
            if s2 == 200:
                mid = json.loads(b2).get("result", {}).get("message_id")
                mid = int(mid) if mid is not None else None
                if mid is not None:
                    _last_delivered_alert_ts = now_ts()
                return mid
        log.error("TG send %s: %s", status, body[:200])
        _stats["telegram_failures"] += 1
        return None
    except Exception as e:
        _stats["telegram_failures"] += 1
        log.error("TG send exception: %s", e)
        return None

async def deferred_startup_notice(text: str):
    if not config_is_valid(): return
    while not _shutdown.is_set():
        if now_ts() >= _tg_blocked_until:
            if await send_msg(text) is not None: return
            if now_ts() >= _tg_blocked_until: return
        await _shutdown_or_sleep(15)

async def edit_msg(message_id: int, text: str) -> bool:
    if not (config_is_valid() and SESSION) or not message_id: return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "message_id": int(message_id), "text": text,
               "parse_mode": "Markdown", "disable_web_page_preview": True}
    try:
        status, body = await _tg_request(url, payload, priority=False, retry_429=False)
        if status == 200 or "message is not modified" in body: return True
        if status == 429: return False
        if status == 400 and "parse entities" in body.lower():
            p = dict(payload); p["text"] = _strip_md(text); p.pop("parse_mode", None)
            s2, b2 = await _tg_request(url, p, priority=False, retry_429=False)
            return s2 == 200 or "message is not modified" in b2
        _stats["telegram_failures"] += 1
        return False
    except Exception:
        _stats["telegram_failures"] += 1
        return False
# ---------------------------------------------------------------- sqlite
def _db() -> sqlite3.Connection:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.execute("PRAGMA busy_timeout=15000")
    return c

def _db_retry(fn, *args, tries: int = 4, base: float = 0.4, **kwargs):
    last: Exception = RuntimeError("unreachable")
    for k in range(tries):
        try: return fn(*args, **kwargs)
        except sqlite3.OperationalError as e:
            last = e; time.sleep(base * (2 ** k))
    raise last

def _ensure_cols(c, table, cols):
    have = {r[1] for r in c.execute(f"PRAGMA table_info({table})")}
    for n, t in cols.items():
        if n not in have:
            c.execute(f"ALTER TABLE {table} ADD COLUMN {n} {t}")

def _pk_cols(c, table):
    rows = c.execute(f"PRAGMA table_info({table})").fetchall()
    return [r[1] for r in sorted((r for r in rows if int(r[5] or 0) > 0), key=lambda r: int(r[5]))]

def db_selftest():
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute(BAND_OUTCOMES_CREATE)
        conn.execute(SQL_OUTCOME_UPSERT,
                     (1, "now", 0.45, 1, "target", 0.7, -0.1, None, None, None,
                      0.0, "selftest", OUTCOME_SCHEMA_VERSION, 1.0, 300, None))
        conn.execute(SQL_LEGACY_OUTCOME_INSERT,
                     (2, "now", 0.45, 1, "target", 1.0, 0.0, None, None, None,
                      0.0, "selftest", OUTCOME_SCHEMA_VERSION, 1.0, 300))
        conn.execute(SQL_BACKFILL_OUTCOME_INSERT,
                     (3, "now", 0.45, 1, "target", 0.7, -0.1, None,
                      0.0, "selftest", OUTCOME_SCHEMA_VERSION, 1.0, 300))
        conn.execute("SELECT alert_id,band,target_pct,hit,outcome,mfe,mae,mfe_min,mae_min,"
                     "first_min,resolved_ts,resolution_source,label_version,entry_price,"
                     "entry_delay_sec,final_market_move_pct FROM band_outcomes")
        log.info("DB self-test passed")
    except sqlite3.Error as e:
        raise RuntimeError(f"SQL self-test failed: {e}")
    finally:
        conn.close()

_ALERT_COLS = {
    "symbol": "TEXT", "category": "TEXT", "direction": "TEXT", "timeframe": "TEXT",
    "archetype": "TEXT", "cluster_id": "INTEGER", "score": "INTEGER", "score_final": "INTEGER",
    "alert_price": "REAL", "detected_price": "REAL", "ts": "REAL", "detected_ts": "REAL",
    "delivered_ts": "REAL", "telegram_message_id": "INTEGER", "features": "TEXT",
    "feature_schema": "INTEGER", "source": "TEXT", "calls": "TEXT", "base_message": "TEXT",
    "low_vol": "INTEGER DEFAULT 0", "outcome_schema": "TEXT", "episode_id": "TEXT",
    "telegram_suppressed": "INTEGER DEFAULT 0", "quality_gate_reason": "TEXT",
    "entry_price_10s": "REAL", "notification_version": "TEXT", "config_version": "TEXT",
    "ret_5m": "REAL", "ret_15m": "REAL", "ret_30m": "REAL", "ret_60m": "REAL",
    "ret_240m": "REAL", "ret_1440m": "REAL", "hit_scalp": "INTEGER", "mfe_scalp": "REAL",
    "pk_scalp": "REAL", "hit_medium": "INTEGER", "mfe_medium": "REAL", "pk_medium": "REAL",
    "hit_sleep": "INTEGER", "mfe_sleep": "REAL", "pk_sleep": "REAL", "hit_hold": "INTEGER",
    "mfe_hold": "REAL", "pk_hold": "REAL"}

def db_init():
    c = _db()
    try:
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("""CREATE TABLE IF NOT EXISTS alerts(
            id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, category TEXT, direction TEXT,
            timeframe TEXT, archetype TEXT, cluster_id INTEGER, score INTEGER, score_final INTEGER,
            alert_price REAL, detected_price REAL, ts REAL, detected_ts REAL, delivered_ts REAL,
            telegram_message_id INTEGER, features TEXT, feature_schema INTEGER, source TEXT,
            calls TEXT, base_message TEXT, low_vol INTEGER DEFAULT 0, outcome_schema TEXT,
            episode_id TEXT, telegram_suppressed INTEGER DEFAULT 0, quality_gate_reason TEXT,
            entry_price_10s REAL, notification_version TEXT, config_version TEXT,
            ret_5m REAL, ret_15m REAL, ret_30m REAL, ret_60m REAL, ret_240m REAL, ret_1440m REAL,
            hit_scalp INTEGER, mfe_scalp REAL, pk_scalp REAL, hit_medium INTEGER, mfe_medium REAL,
            pk_medium REAL, hit_sleep INTEGER, mfe_sleep REAL, pk_sleep REAL, hit_hold INTEGER,
            mfe_hold REAL, pk_hold REAL)""")
        _ensure_cols(c, "alerts", _ALERT_COLS)
        have = {r[1] for r in c.execute("PRAGMA table_info(band_outcomes)")}
        if not have:
            c.execute(BAND_OUTCOMES_CREATE)
        else:
            canon = {"alert_id": "INTEGER", "band": "TEXT", "target_pct": "REAL", "stop_pct": "REAL",
                     "barrier_win": "INTEGER", "hit": "INTEGER", "outcome": "TEXT", "mfe": "REAL",
                     "mae": "REAL", "mfe_min": "REAL", "mae_min": "REAL", "first_min": "REAL",
                     "resolved_ts": "REAL", "resolution_source": "TEXT", "label_version": "TEXT",
                     "entry_price": "REAL", "entry_delay_sec": "REAL",
                     "best_market_move_pct": "REAL", "final_market_move_pct": "REAL"}
            if _pk_cols(c, "band_outcomes") == ["alert_id", "band", "label_version"]:
                _ensure_cols(c, "band_outcomes", canon)
                c.execute("UPDATE band_outcomes SET label_version='legacy_pre_nexus2' "
                          "WHERE label_version IS NULL OR label_version=''")
            else:
                log.info("Rebuilding legacy band_outcomes (PK=%s)", _pk_cols(c, "band_outcomes"))
                c.execute("DROP TABLE IF EXISTS band_outcomes__new")
                c.execute("""CREATE TABLE band_outcomes__new(
                    alert_id INTEGER NOT NULL, band TEXT NOT NULL, target_pct REAL, stop_pct REAL,
                    barrier_win INTEGER, hit INTEGER, outcome TEXT, mfe REAL, mae REAL, mfe_min REAL,
                    mae_min REAL, first_min REAL, resolved_ts REAL, resolution_source TEXT,
                    label_version TEXT NOT NULL, entry_price REAL, entry_delay_sec REAL,
                    best_market_move_pct REAL, final_market_move_pct REAL,
                    PRIMARY KEY(alert_id,band,label_version))""")
                old = {r[1] for r in c.execute("PRAGMA table_info(band_outcomes)")}
                def ex(col, d="NULL"): return col if col in old else d
                hit_e = ex("hit", ex("barrier_win"))
                out_e = ("outcome" if "outcome" in old else
                         f"CASE WHEN {hit_e}=1 THEN 'target' WHEN {hit_e}=0 THEN 'timeout' ELSE NULL END")
                lab = ("COALESCE(NULLIF(label_version,''),'legacy_pre_nexus2')" if "label_version" in old
                       else "'legacy_pre_nexus2'")
                cols = ["alert_id","band","target_pct","hit","outcome","mfe","mae","mfe_min","mae_min",
                        "first_min","resolved_ts","resolution_source","label_version","entry_price",
                        "entry_delay_sec","best_market_move_pct","final_market_move_pct","stop_pct","barrier_win"]
                sel = [ex("alert_id","0"), ex("band","''"), ex("target_pct","0.0"), hit_e, out_e,
                       ex("mfe"), ex("mae"), ex("mfe_min"), ex("mae_min"), ex("first_min"),
                       ex("resolved_ts"), ex("resolution_source","'legacy'"), lab, ex("entry_price"),
                       ex("entry_delay_sec"), ex("best_market_move_pct"), ex("final_market_move_pct"),
                       ex("stop_pct"), ex("barrier_win")]
                c.execute(f"INSERT OR IGNORE INTO band_outcomes__new({','.join(cols)}) "
                          f"SELECT {','.join(sel)} FROM band_outcomes")
                c.execute("DROP TABLE band_outcomes")
                c.execute("ALTER TABLE band_outcomes__new RENAME TO band_outcomes")
        c.execute("""CREATE TABLE IF NOT EXISTS alert_predictions(
            alert_id INTEGER NOT NULL, band TEXT NOT NULL, p_hist REAL, p_raw REAL, p_cal REAL,
            p_final REAL, alpha REAL, display_call TEXT, actionable INTEGER, model_version TEXT,
            PRIMARY KEY(alert_id,band))""")
        _ensure_cols(c, "alert_predictions", {"alert_id": "INTEGER", "band": "TEXT", "p_hist": "REAL",
                     "p_raw": "REAL", "p_cal": "REAL", "p_final": "REAL", "alpha": "REAL",
                     "display_call": "TEXT", "actionable": "INTEGER", "model_version": "TEXT"})
        c.execute("""CREATE TABLE IF NOT EXISTS shadow_candidates(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL, direction TEXT NOT NULL, timeframe TEXT NOT NULL,
            ts REAL NOT NULL, block_reason TEXT NOT NULL, price REAL, entry_price REAL,
            mfe_now REAL DEFAULT 0, mfe_30m REAL DEFAULT 0, mfe_1h REAL DEFAULT 0, mfe_4h REAL DEFAULT 0,
            hit_now INTEGER, hit_30m INTEGER, hit_1h INTEGER, hit_4h INTEGER, resolved_ts REAL)""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_shadow_ts ON shadow_candidates(ts)")
        c.execute("CREATE TABLE IF NOT EXISTS nexus_meta(key TEXT PRIMARY KEY, value TEXT)")
        c.execute("""CREATE TABLE IF NOT EXISTS historical_bootstrap_state(
            symbol TEXT PRIMARY KEY, version TEXT NOT NULL, start_day TEXT NOT NULL,
            end_day TEXT NOT NULL, rows_loaded INTEGER DEFAULT 0, events_inserted INTEGER DEFAULT 0,
            completed_ts REAL NOT NULL)""")
        c.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_backfill_event_dedupe
                     ON alerts(source,symbol,timeframe,detected_ts) WHERE source LIKE 'backfill_%'""")
        try:
            c.execute("""DELETE FROM band_outcomes WHERE alert_id IN
                         (SELECT id FROM alerts WHERE source LIKE 'backfill%'
                          AND source <> ?)""", (BOOTSTRAP_SOURCE,))
            deleted = c.execute("DELETE FROM alerts WHERE source LIKE 'backfill%' AND source <> ?",
                                (BOOTSTRAP_SOURCE,)).rowcount
            c.execute("DELETE FROM historical_bootstrap_state WHERE version <> ?", (BOOTSTRAP_SOURCE,))
            if deleted:
                log.info("Purged %d superseded backfill alerts", deleted)
        except Exception as e:
            log.error("backfill purge: %s", e)
        c.execute("INSERT OR REPLACE INTO nexus_meta VALUES('schema_version',?)", (NEXUS_VERSION,))
        c.execute("INSERT OR REPLACE INTO nexus_meta VALUES('outcome_schema',?)", (OUTCOME_SCHEMA_VERSION,))
        c.execute("CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts(ts)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_alerts_bucket ON alerts(category,timeframe,score,archetype)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_alerts_episode ON alerts(episode_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_outcomes_band ON band_outcomes(band,label_version,hit)")
        c.commit()
        db_selftest()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    log.info("DB ready: %s", DB_PATH)

def db_insert_alert(symbol, category, direction, timeframe, score, price, detected_ts, delivered_ts,
                    message_id, features, archetype, calls, low_vol, base_message, source="live_nexus") -> int:
    c = _db()
    cur = c.execute("""INSERT INTO alerts(symbol,category,direction,timeframe,score,score_final,
        alert_price,detected_price,ts,detected_ts,delivered_ts,telegram_message_id,features,
        feature_schema,archetype,calls,base_message,low_vol,source,cluster_id,outcome_schema,
        episode_id,telegram_suppressed,quality_gate_reason,notification_version,config_version)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (symbol, category, direction, timeframe, int(score), int(score), float(price), float(price),
         float(delivered_ts), float(detected_ts), float(delivered_ts),
         int(message_id) if message_id else None, _safe_json(features), FEATURE_SCHEMA_VERSION,
         archetype, _safe_json(calls), base_message, int(low_vol or 0), source,
         int(detected_ts // 1800), OUTCOME_SCHEMA_VERSION, _episode_id(symbol, direction, detected_ts),
         0 if message_id else 1, None, NOTIFICATION_VERSION, NEXUS_BUILD_ID))
    aid = int(cur.lastrowid)
    for band, cl in (calls or {}).items():
        c.execute("""INSERT OR REPLACE INTO alert_predictions
            (alert_id,band,p_hist,p_raw,p_cal,p_final,alpha,display_call,actionable,model_version)
            VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (aid, band, cl.get("p_hist"), cl.get("pre_chase"), cl.get("p_cal"),
             cl.get("p_final"), cl.get("alpha"), cl.get("display_call"),
             1 if cl.get("actionable") else 0, NEXUS_BUILD_ID))
    c.commit(); c.close(); return aid

def db_set_entry_ts(alert_id, price, entry_ts):
    c = _db()
    c.execute("UPDATE alerts SET entry_price_10s=?,alert_price=?,ts=COALESCE(ts,?) WHERE id=?",
              (float(price), float(price), float(entry_ts), int(alert_id)))
    c.commit(); c.close()

def db_set_outcome(alert_id, band, hit, outcome, mfe, mae, mfe_min, mae_min, first_min,
                   resolution_source, entry_price, final_mv, exclude: bool = False,
                   target: Optional[float] = None):
    if target is None:
        target = next((t for b, _n, _d0, _d1, t in BANDS if b == band), 2.5)
    c = _db()
    c.execute(SQL_OUTCOME_UPSERT,
              (int(alert_id), band, target, (None if exclude else int(hit)), outcome,
               float(mfe), float(mae), mfe_min, mae_min, first_min, time.time(),
               resolution_source, OUTCOME_SCHEMA_VERSION, float(entry_price),
               BAND_DELAY_SEC, final_mv))
    c.commit(); c.close()

def db_set_return(alert_id, minutes, value):
    if minutes not in {5, 15, 30, 60, 240, 1440}: return
    c = _db(); c.execute(f"UPDATE alerts SET ret_{minutes}m=? WHERE id=?", (float(value), int(alert_id)))
    c.commit(); c.close()

def db_existing_outcomes(alert_id: int) -> dict:
    c = _db(); c.row_factory = sqlite3.Row
    rows = c.execute("SELECT * FROM band_outcomes WHERE alert_id=? AND label_version=?",
                     (int(alert_id), OUTCOME_SCHEMA_VERSION)).fetchall()
    c.close()
    return {r["band"]: r for r in rows}

def db_pending_tracks(limit=250):
    cut = time.time() - TRACK_RECOVERY_LOOKBACK_SEC
    c = _db(); c.row_factory = sqlite3.Row
    rows = c.execute("""SELECT a.id, a.symbol, a.direction, a.timeframe, a.detected_ts, a.ts,
        a.detected_price, a.entry_price_10s, a.telegram_message_id, a.calls, a.base_message,
        a.delivered_ts
        FROM alerts a WHERE a.ts>=? AND a.source LIKE 'live_nexus%'
        AND a.delivered_ts IS NOT NULL
        AND ((a.timeframe='rev30' AND (SELECT COUNT(*) FROM band_outcomes b
                 WHERE b.alert_id=a.id AND b.label_version=?)<1)
          OR (a.timeframe<>'rev30' AND (SELECT COUNT(*) FROM band_outcomes b
                 WHERE b.alert_id=a.id AND b.label_version=?)<4))
        ORDER BY a.ts ASC LIMIT ?""", (cut, OUTCOME_SCHEMA_VERSION, OUTCOME_SCHEMA_VERSION,
                                       int(limit))).fetchall()
    c.close(); return rows

def db_outcome_count() -> int:
    c = _db()
    n = c.execute("SELECT COUNT(*) FROM band_outcomes WHERE label_version=?",
                  (OUTCOME_SCHEMA_VERSION,)).fetchone()[0]
    c.close(); return int(n)

def db_chase_audit(min_age_min: int = 120, window_days: int = 7) -> Optional[dict]:
    c = _db(); c.row_factory = sqlite3.Row
    try:
        rows = c.execute("""
            SELECT ap.band, ap.p_raw, ap.p_final, bo.hit
            FROM alert_predictions ap
            JOIN band_outcomes bo ON bo.alert_id = ap.alert_id AND bo.band = ap.band
                                 AND bo.label_version = ?
            JOIN alerts a ON a.id = ap.alert_id
            WHERE ap.p_raw IS NOT NULL AND ap.p_final IS NOT NULL
              AND bo.hit IS NOT NULL
              AND a.detected_ts BETWEEN ? AND ?
              AND a.source NOT LIKE 'backfill%'
        """, (OUTCOME_SCHEMA_VERSION,
              now_ts() - window_days * 86400,
              now_ts() - min_age_min * 60)).fetchall()
    finally:
        c.close()
    if not rows:
        return None
    agg: dict[str, dict] = {}
    for r in rows:
        d = agg.setdefault(r["band"], {"n": 0, "hits": 0, "sr": 0.0, "sf": 0.0, "br": 0.0, "bf": 0.0})
        hit = float(r["hit"])
        p_raw = min(max(float(r["p_raw"]), 0.0), 1.0)
        p_fin = min(max(float(r["p_final"]), 0.0), 1.0)
        d["n"] += 1; d["hits"] += int(r["hit"])
        d["sr"] += p_raw; d["sf"] += p_fin
        d["br"] += (p_raw - hit) ** 2; d["bf"] += (p_fin - hit) ** 2
    for b, d in agg.items():
        n = d["n"]
        d["rate"] = d["hits"] / n
        d["mean_raw"] = d["sr"] / n
        d["mean_final"] = d["sf"] / n
        d["brier_raw"] = d["br"] / n
        d["brier_final"] = d["bf"] / n
        d["brier_gain"] = d["brier_raw"] - d["brier_final"]
        for k in ("hits", "sr", "sf", "br", "bf"):
            del d[k]
    return agg

async def chase_audit_task():
    while not _shutdown.is_set():
        try:
            a = await asyncio.to_thread(db_chase_audit, 120)
            if a:
                for band, d in a.items():
                    log.info("chase-audit %s: n=%d rate=%.3f raw=%.3f final=%.3f "
                             "brier_raw=%.4f final=%.4f gain=%+.4f",
                             band, d["n"], d["rate"], d["mean_raw"], d["mean_final"],
                             d["brier_raw"], d["brier_final"], d["brier_gain"])
        except Exception as e:
            log.error("chase audit: %s", e)
        await _shutdown_or_sleep(6 * 3600)

def migrate_legacy_batch(limit=2000) -> int:
    c = _db(); c.row_factory = sqlite3.Row
    rows = c.execute("""SELECT a.* FROM alerts a WHERE (a.mfe_scalp IS NOT NULL OR a.mfe_medium IS NOT NULL
        OR a.mfe_sleep IS NOT NULL OR a.mfe_hold IS NOT NULL) AND NOT EXISTS
        (SELECT 1 FROM band_outcomes bo WHERE bo.alert_id=a.id AND bo.label_version=?)
        ORDER BY a.id ASC LIMIT ?""", (OUTCOME_SCHEMA_VERSION, int(limit))).fetchall()
    mapping = {"now": "mfe_scalp", "30m": "mfe_medium", "1h": "mfe_sleep", "4h": "mfe_hold"}
    ins = 0
    for r in rows:
        for band, mc in mapping.items():
            mfe = r[mc] if mc in r.keys() else None
            if mfe is None: continue
            target = next(t for b, _n, _d0, _d1, t in BANDS if b == band)
            hit = 1 if float(mfe) >= target else 0
            c.execute(SQL_LEGACY_OUTCOME_INSERT,
                (r["id"], band, target, hit, "target" if hit else "timeout", float(mfe), 0.0,
                 None, None, None, r["ts"] or time.time(),
                 "legacy_relabel_mfe", OUTCOME_SCHEMA_VERSION,
                 r["alert_price"] or r["detected_price"] or 0.0, 300))
            ins += 1
    c.commit(); c.close(); return ins

# ---------------------------------------------------------------- market math
def price_at(s: Sym, t: float) -> Optional[float]:
    v = s.fine.at_or_before(t)
    if v is not None: return v
    v = s.m1.close_at_or_before(t)
    if v is not None: return v
    return s.m5.close_at_or_before(t)

def pct_change(s: Sym, now: float, win: float) -> Optional[float]:
    if s.px <= 0: return None
    base = price_at(s, now - win)
    if not base or base <= 0: return None
    return (s.px / base - 1.0) * 100.0

def breakout_dir(s: Sym, now: float, win: float) -> Optional[str]:
    lo, hi = now - win, now - 60
    if win <= 7800: mn, mx, cnt = s.m1.minmax_close(lo, hi)
    else: mn, mx, cnt = s.m5.minmax_close(lo, hi)
    if mn is None or cnt < 2: return None
    if s.px > mx: return "up"
    if s.px < mn: return "down"
    return None

def returns_m5(s: Sym, now: float) -> dict[int, float]:
    cut = now - 86400; out = {}; prev = None
    for t, _o, _h, _l, c, _q in s.m5.last_bars(288):
        if t < cut: continue
        if prev is not None and c > 0 and prev > 0:
            out[int(t // 300)] = math.log(c / prev)
        prev = c
    return out

_RET_TTL = 300.0
def returns_m5_cached(s: Sym, now: float) -> dict[int, float]:
    ts, v = s.ret_cache
    if v is not None and now - ts < _RET_TTL: return v
    v = returns_m5(s, now); s.ret_cache = (now, v)
    return v

def _pearson(xs, ys) -> Optional[float]:
    n = len(xs); mx = sum(xs) / n; my = sum(ys) / n
    cov = vx = vy = 0.0
    for x, y in zip(xs, ys):
        dx, dy = x - mx, y - my
        cov += dx * dy; vx += dx * dx; vy += dy * dy
    return cov / math.sqrt(vx * vy) if vx > 0 and vy > 0 else None

def rolling_btc_corr(s: Sym, now: float) -> Optional[float]:
    if s.sym == BTC_SYMBOL: return None
    ts, c = s.corr_cache
    if now - ts < 300: return c
    b = SYMS.get(BTC_SYMBOL)
    if not b:
        s.corr_cache = (now_ts(), None); return None
    rs, rb = returns_m5_cached(s, now), returns_m5_cached(b, now)
    common = set(rs) & set(rb)
    r = _pearson([rs[k] for k in sorted(common)], [rb[k] for k in sorted(common)]) \
        if len(common) >= BTC_CORR_MIN_SAMPLES else None
    s.corr_cache = (now_ts(), r)
    return r

def m5_sigma(s: Sym, now: float) -> Optional[float]:
    ts, v = s.sig_cache
    if now - ts < 300: return v
    rets = list(returns_m5_cached(s, now).values())[-72:]
    v = statistics.pstdev(rets) if len(rets) >= 12 else None
    s.sig_cache = (now, v)
    return v

def median_minute_qv(s: Sym, now: float) -> Optional[float]:
    ts, v = s.med_cache
    if now - ts < 60: return v
    vals = [b[5] for b in s.m1.last_bars(30) if b[5] > 0]
    v = statistics.median(vals) if len(vals) >= 5 else None
    s.med_cache = (now, v)
    return v

def liq_sides(s: Sym, win: float):
    if not s.liq: return 0.0, 0.0
    cut = int((now_ts() - win) // 10); L = S = 0.0
    for e in reversed(s.liq):
        if e[0] < cut: break
        L += e[1]; S += e[2]
    return L, S

def tk_imb(name: str, win: float) -> Optional[float]:
    dq = TAKER.get(name)
    if not dq: return None
    b0 = int((now_ts() - win) // 5); sg = ab = 0.0
    for e in reversed(dq):
        if e[0] < b0: break
        sg += e[1]; ab += e[2]
    return max(-1.0, min(1.0, sg / ab)) if ab > 0 else None

def oi_change_pct(s: Sym) -> Optional[float]:
    if s.oi_ref > 0 and s.oi_usd > 0:
        return (s.oi_usd / s.oi_ref - 1.0) * 100.0
    return None

def range_pos(mn, mx, px) -> Optional[float]:
    if mn is None or mx is None or mx <= mn: return None
    return min(1.0, max(0.0, (px - mn) / (mx - mn)))

def btc_regime() -> str:
    b = SYMS.get(BTC_SYMBOL)
    ch = pct_change(b, now_ts(), 86400) if b else None
    if ch is None: return "chop"
    return "bull" if ch > 3 else "bear" if ch < -3 else "chop"

def regime_align(direction: str) -> float:
    r = btc_regime()
    if r == "chop": return 0.0
    return 1.0 if (r == "bull" and direction == "up") or (r == "bear" and direction == "down") else -1.0

def is_btc_driven(s: Sym, direction: str, now: float, win: float) -> bool:
    corr = rolling_btc_corr(s, now)
    if corr is None or corr < BTC_CORR_MARKET_DRIVEN: return False
    b = SYMS.get(BTC_SYMBOL)
    bp = pct_change(b, now, win) if b else None
    if bp is None: return False
    return (direction == "up" and bp >= BTC_MOVE_SAME_DIR_PCT) or \
           (direction == "down" and bp <= -BTC_MOVE_SAME_DIR_PCT)

def spread_pct(s: Sym) -> Optional[float]:
    if s.bid > 0 and s.ask >= s.bid:
        return (s.ask - s.bid) / ((s.ask + s.bid) / 2.0) * 100.0
    return None

def classify_archetype(raw: dict) -> str:
    vals = {"vol": max(raw.get("vol_5m_pace_x") or 0, raw.get("vol_1m_x") or 0) / 8,
            "oi": abs(raw.get("oi_pct") or 0) / 6,
            "funding": abs(raw.get("fr") or 0) / 0.005,
            "taker": abs(raw.get("taker_imb") or 0)}
    k = max(vals, key=vals.get)
    return "balanced" if vals[k] < 0.4 else f"{k}_driven"

def _liq_tier(spread, qv24) -> Optional[str]:
    if spread is None: return None
    if spread < 0.05 and (qv24 or 0) >= 10_000_000: return "GOOD"
    if spread < 0.15 and (qv24 or 0) >= 2_000_000: return "OK"
    return "THIN"

# ---------------------------------------------------------------- R/S levels
def nearest_levels(s: Sym, direction: str, now: float):
    px = s.px
    if px <= 0: return None
    cands: list[tuple[float, str]] = []
    if s.pdh > 0: cands.append((s.pdh, "PDH"))
    if s.pdl > 0: cands.append((s.pdl, "PDL"))
    if s.hi24 > 0: cands.append((s.hi24, "24hH"))
    if s.lo24 > 0: cands.append((s.lo24, "24hL"))
    if s.hi30 > 0: cands.append((s.hi30, "30dH"))
    if s.lo30 > 0: cands.append((s.lo30, "30dL"))
    bars = s.m5.last_bars(60)
    for k in range(3, max(3, len(bars) - 3)):
        h, l = bars[k][2], bars[k][3]
        try:
            if h > max(bars[k-1][2], bars[k-2][2], bars[k-3][2],
                       bars[k+1][2], bars[k+2][2], bars[k+3][2]):
                cands.append((h, "swing"))
            if l < min(bars[k-1][3], bars[k-2][3], bars[k-3][3],
                       bars[k+1][3], bars[k+2][3], bars[k+3][3]):
                cands.append((l, "swing"))
        except Exception:
            continue
    res = [(p, t) for p, t in cands if p > px * 1.001]
    sup = [(p, t) for p, t in cands if p < px * 0.999]
    r = min(res, key=lambda x: x[0]) if res else None
    sp = max(sup, key=lambda x: x[0]) if sup else None
    return (r, sp)

def rs_line(s: Sym, direction: str, now: float) -> str:
    lv = nearest_levels(s, direction, now)
    if not lv or (lv[0] is None and lv[1] is None): return ""
    r, sp = lv
    parts = []
    if r and (r[0] / s.px - 1.0) * 100.0 <= RS_MAX_SHOWN_PCT:
        parts.append(f"R {r[0]:.6g} ({r[1]}, {(r[0]/s.px-1)*100:+.1f}%)")
    if sp and (1.0 - sp[0] / s.px) * 100.0 <= RS_MAX_SHOWN_PCT:
        parts.append(f"S {sp[0]:.6g} ({sp[1]}, {(sp[0]/s.px-1)*100:+.1f}%)")
    if not parts: return ""
    return "📏 " + " · ".join(parts)

# ---------------------------------------------------------------- exhaustion engine
def ext_move_pct(s: Sym, now: float, win: float) -> Optional[float]:
    base = price_at(s, now - win)
    if not base or base <= 0 or s.px <= 0: return None
    return (s.px / base - 1.0) * 100.0

def leg_decay(s: Sym, direction: str) -> Optional[float]:
    bars = s.m5.last_bars(12)
    if len(bars) < 6: return None
    sign = 1.0 if direction == "up" else -1.0
    legs = [(b[4] / b[1] - 1.0) * 100.0 * sign for b in bars if b[1] > 0]
    if not legs or max(legs) <= 0: return None
    return legs[-1] / max(legs)

def climax_bar(s: Sym, direction: str) -> bool:
    bars = s.m1.last_bars(31)
    if len(bars) < 31: return False
    _t, o, h, l, c, _q = bars[-1]
    rng = h - l
    med = statistics.median([b[2] - b[3] for b in bars[:-1]])
    if med <= 0 or o <= 0 or rng <= 0: return False
    pos = (c - l) / rng
    return rng >= 3.0 * med and (pos >= 0.55 if direction == "down" else pos <= 0.45)

def liq_decel(s: Sym, direction: str, now: float) -> Optional[float]:
    if not s.liq: return None
    b0 = int(now // 10); side = 1 if direction == "down" else 2
    def wsum(a, b):
        return sum(e[side] for e in reversed(s.liq) if b0 - b < e[0] <= b0 - a)
    recent = wsum(0, 12)
    peak = max(wsum(k, k + 12) for k in range(12, 78, 12))
    if peak < LIQ_PEAK_FLOOR_USD: return None
    return recent / peak if peak > 0 else None

def exhaustion_score(s: Sym, now: float, direction: str, pct: float, raw: dict):
    pts = 0
    reasons: list[str] = []
    sign = 1.0 if direction == "up" else -1.0
    sig = raw.get("_sigma")
    ext1 = ext_move_pct(s, now, EXTENSION_WINDOW_SEC)
    ext1_dir = ext1 * sign if ext1 is not None else None
    ext24 = ext_move_pct(s, now, 86400)
    ext24_dir = ext24 * sign if ext24 is not None else None
    z1 = z24 = None
    if sig and sig > 0:
        per_bar_pct = sig * 100.0
        if ext1_dir is not None and per_bar_pct > 0:
            z1 = ext1_dir / per_bar_pct * math.sqrt(300.0 / 3600.0)
        if ext24_dir is not None and per_bar_pct > 0:
            z24 = ext24_dir / per_bar_pct * math.sqrt(300.0 / 86400.0)
    if ext1_dir is not None:
        if ext1_dir >= 15: pts += 25
        elif ext1_dir >= 8: pts += 15
        elif ext1_dir >= 4: pts += 8
    if z1 is not None and z1 >= EXH_EXTENSION_Z:
        pts += 15; reasons.append(f"ext z{z1:.1f}")
    elif ext1_dir is not None and ext1_dir >= 4:
        pts += 5
    if z24 is not None and z24 >= EXH_EXTENSION_Z * 1.5:
        pts += 10; reasons.append("24h run extended")
    if ext1_dir is not None and abs(pct) > 0 and ext1_dir > 0:
        share = abs(pct) / ext1_dir
        if share <= 0.25: pts += 12; reasons.append("tail-end of move")
        elif share <= 0.40: pts += 6
    for rp in (raw.get("shape_1h"), raw.get("shape_4h")):
        if rp is None: continue
        if (direction == "down" and rp <= 0.10) or (direction == "up" and rp >= 0.90):
            pts += 18; reasons.append("at range extreme"); break
        if (direction == "down" and rp <= 0.20) or (direction == "up" and rp >= 0.80):
            pts += 9; break
    age_min = None
    if direction == "up" and s.leg_up_ts > 0 and s.leg_up_px > 0:
        if (s.px / s.leg_up_px - 1.0) * 100.0 > 0.5:
            age_min = (now - s.leg_up_ts) / 60.0
    elif direction == "down" and s.leg_dn_ts > 0 and s.leg_dn_px > 0:
        if (1.0 - s.px / s.leg_dn_px) * 100.0 > 0.5:
            age_min = (now - s.leg_dn_ts) / 60.0
    if age_min is not None:
        if age_min >= 60: pts += 10; reasons.append(f"move {age_min:.0f}m old")
        elif age_min >= 30: pts += 5
    dec = leg_decay(s, direction)
    if dec is not None:
        if dec <= 0.4: pts += 12; reasons.append("legs fading")
        elif dec <= 0.6: pts += 6
    ld = liq_decel(s, direction, now)
    if ld is not None and ld <= 0.35:
        pts += 12; reasons.append("liq cascade drying")
    if climax_bar(s, direction):
        pts += 10; reasons.append("climax bar")
    tk = raw.get("taker_imb")
    aligned = None
    if tk is not None:
        aligned = tk * sign
        if aligned < -0.10:
            pts += 8; reasons.append("taker flow against move")
    prev_age = prev_z = prev_same = None
    if s.last_alert_ts > 0 and s.last_alert_px > 0:
        prev_age = (now - s.last_alert_ts) / 60.0
        prev_same = 1.0 if s.last_alert_dir == direction else 0.0
        pm = (s.px / s.last_alert_px - 1.0) * 100.0 * sign
        if sig and sig > 0:
            prev_z = pm / (sig * 100.0)
        if prev_age <= 20 and prev_same == 1.0 and (prev_z is None or prev_z < 0.5):
            pts += 8; reasons.append("re-alert churn")
    score = min(100, pts)
    raw.update({"ext_1h_z": z1, "ext_24h_z": z24, "impulse_age_min": age_min,
                "momentum_accel": dec, "participation_aligned": aligned,
                "prev_alert_age_min": prev_age, "prev_alert_move_z": prev_z,
                "prev_alert_same_dir": prev_same})
    return score, (ext1_dir if ext1 is not None else None), dec, ld, reasons

def exh_tier_word(e) -> str:
    if e is None: return "—"
    if e >= EXH_TIER_CLIMAX: return "CLIMAX"
    if e >= EXH_TAG_MIN: return "EXHAUSTING"
    if e >= EXH_TIER_MATURE: return "MATURE"
    return "FRESH"

# ---------------------------------------------------------------- chase risk
def chase_tier_word(c: int) -> str:
    if c >= 70: return "EXTREME"
    if c >= 50: return "HIGH"
    if c >= 30: return "MODERATE"
    return "LOW"

def chase_score(s: Sym, now: float, direction: str, pct: float, raw: dict,
                label: str, exh: float):
    pts = 0.0
    reasons: list[str] = []
    tgt = TIMEFRAME_TARGET_PCT.get(label, 4.0)
    pts += exh * 0.4
    ratio = abs(pct) / max(tgt, 0.1)
    if ratio >= 3.0:
        pts += 20; reasons.append(f"{abs(pct):.1f}% already")
    elif ratio >= 2.0:
        pts += 14; reasons.append(f"{abs(pct):.1f}% already")
    elif ratio >= 1.5:
        pts += 8; reasons.append(f"{abs(pct):.1f}% already")
    lv = nearest_levels(s, direction, now)
    if lv:
        probe = lv[0] if direction == "up" else lv[1]
        if probe:
            dist = abs(probe[0] / s.px - 1.0) * 100.0
            tag = "R" if direction == "up" else "S"
            if dist < tgt * 0.5:
                pts += 20; reasons.append(f"{tag} {dist:.1f}% caps {label} target")
            elif dist < tgt:
                pts += 10; reasons.append(f"{tag} {dist:.1f}% < {tgt:.0f}% target")
    recent = sum(1 for t in s.alert_ts if now - t < 3600)
    if recent >= 3:
        pts += 10; reasons.append(f"{recent} alerts/h")
    elif recent == 2:
        pts += 5
    if s.last_alert_dir and s.last_alert_dir != direction and now - s.last_alert_ts < 1800:
        pts += 10; reasons.append("recent flip")
    fr = raw.get("fr")
    if fr is not None:
        if direction == "up":
            if fr >= FUNDING_EXTREME:
                pts += 10; reasons.append("crowded longs funding")
            elif fr <= -FUNDING_EXTREME:
                pts += 10; reasons.append("squeeze fuel late")
        else:
            if fr <= -FUNDING_EXTREME:
                pts += 10; reasons.append("crowded shorts funding")
            elif fr >= FUNDING_EXTREME:
                pts += 10; reasons.append("squeeze fuel late")
    v1 = raw.get("vol_1m_x"); v5 = raw.get("vol_5m_pace_x")
    thin = (v1 is not None and v5 is not None
            and v1 < THIN_VOL_1M_X and v5 < THIN_VOL_PACE_X)
    if thin:
        pts += 15; reasons.append("thin volume")
    spread = raw.get("spread")
    if spread is not None and spread > 0.15:
        pts += 5; reasons.append("wide spread")
    rp = raw.get("shape_1h")
    if rp is not None:
        if (direction == "up" and rp >= 0.95) or (direction == "down" and rp <= 0.05):
            pts += 10; reasons.append("at range extreme")
    return min(100, round(pts)), reasons, thin

def chase_penalty_frac(chase: int) -> float:
    if chase <= CHASE_T0: return 0.0
    return min(0.30, (chase - CHASE_T0) * CHASE_SLOPE)

# ---------------------------------------------------------------- momentum bridge
def momentum_bonus(s: Sym, direction: str, now: float, raw: dict):
    if not MOMENTUM_BRIDGE: return 0.0, []
    sign = 1.0 if direction == "up" else -1.0
    pts = 0.0
    reasons: list[str] = []
    if breakout_dir(s, now, 3600) == direction:
        pts += 4; reasons.append("breakout")
    v = max(raw.get("vol_1m_x") or 0, raw.get("vol_5m_pace_x") or 0)
    if v >= 3.0:
        pts += 4
    oi = raw.get("oi_pct")
    if oi is not None and oi * sign > 5.0:
        pts += 4
    tk = raw.get("taker_imb")
    if tk is not None and tk * sign > 0.2:
        pts += 3
    L, S = liq_sides(s, 300)
    liq_aligned = (S if direction == "up" else L)
    if liq_aligned >= 25_000:
        pts += 2
    if pts <= 0: return 0.0, []
    pts = min(MOMENTUM_MAX_PTS, pts)
    return pts, reasons

# ---------------------------------------------------------------- features / score
def _dow_flags(ts: float) -> dict:
    names = ["tue", "wed", "thu", "fri", "sat", "sun"]
    out = {f"dow_{x}": 0.0 for x in names}
    wd = datetime.fromtimestamp(ts, tz=timezone.utc).weekday()
    if wd >= 1: out[f"dow_{names[wd - 1]}"] = 1.0
    return out

def build_raw(s: Sym, now: float) -> dict:
    med = median_minute_qv(s, now)
    bars = s.m1.last_bars(10)
    qv1 = bars[-1][5] if bars else None
    s5 = sum(b[5] for b in bars[-5:]); s10 = sum(b[5] for b in bars)
    vol1 = qv1 / med if (med and qv1 is not None) else None
    pace = (s5 / 5) / med if (med and s5 > 0) else None
    ppace = ((s10 - s5) / 5) / med if (med and s10 > s5) else None
    accel = pace / ppace if (pace and ppace) else None
    b5 = s.m5.last_bars(48)
    hi4 = max((b[2] for b in b5), default=None); lo4 = min((b[3] for b in b5), default=None)
    h1 = s.m1.last_bars(60)
    hi1 = max((b[2] for b in h1), default=None); lo1 = min((b[3] for b in h1), default=None)
    mn15, mx15, _ = s.fine.minmax(now - 900, now)
    L, S = liq_sides(s, 900)
    fr = s.funding if s.funding is not None else None
    return {"vol_1m_x": _clip_vol(vol1), "vol_5m_pace_x": _clip_vol(pace),
            "volume_accel": accel,
            "oi_pct": oi_change_pct(s), "taker_imb": tk_imb(s.sym, 120), "taker_1m": tk_imb(s.sym, 60),
            "fr": fr, "fr_delta": (fr - s.fr_1h) if (fr is not None and s.fr_1h is not None) else None,
            "basis": s.basis if s.basis else None, "btc_corr": None, "spread": None,
            "quote_vol_24h": s.qv24 or None, "low_vol": 1 if (s.qv24 or 0) < 5_000_000 else 0,
            "shape_15m": range_pos(mn15, mx15, s.px), "shape_1h": range_pos(lo1, hi1, s.px),
            "shape_4h": range_pos(lo4, hi4, s.px),
            "liq_15m": (L + S) if (L + S) > 0 else None}

def build_feature_vector(pct, label, raw, liq, liq_thr, aligned, direction, ts):
    tgt = TIMEFRAME_TARGET_PCT.get(label, abs(pct) or 1.0)
    v1, v5 = raw.get("vol_1m_x"), raw.get("vol_5m_pace_x")
    bestv = v5 if v5 is not None else v1
    impulse = min(2.5, abs(pct) / max(tgt, 0.01))
    sig = raw.get("vol_sigma") or raw.get("_sigma")
    fr = raw.get("fr")
    fv = {"_schema": FEATURE_SCHEMA_VERSION,
          "impulse_raw": impulse, "impulse_score": min(1.5, abs(pct) / max(tgt, 0.01)),
          "volume": bestv, "volume_1m": v1, "volume_5m_pace": v5, "volume_accel": raw.get("volume_accel"),
          "oi_signed": raw.get("oi_pct"),
          "oi_abs": abs(raw["oi_pct"]) if raw.get("oi_pct") is not None else None,
          "taker": raw.get("taker_imb"), "taker_1m": raw.get("taker_1m"),
          "liq_ratio": min(3.0, liq / max(liq_thr, 1.0)) if liq is not None else None,
          "breakout": 1.0 if aligned else 0.0,
          "funding_signed": fr / 0.005 if fr is not None else None,
          "funding_abs": abs(fr) / 0.005 if fr is not None else None,
          "funding_delta": (raw["fr_delta"] / 0.005) if raw.get("fr_delta") is not None else None,
          "basis": raw.get("basis"), "btc_corr": raw.get("btc_corr"),
          "regime_align": regime_align(direction),
          "vacuum": min(5.0, impulse / max(bestv, 0.5)) if bestv is not None else None,
          "impulse_z": (min(10.0, (abs(pct) / 100.0) /
                        (sig * math.sqrt(TF_SEC.get(label, 300) / 300.0))) if sig else None),
          "spread": raw.get("spread"),
          "quote_vol_log": math.log10(raw["quote_vol_24h"]) if raw.get("quote_vol_24h") else None,
          "low_vol_flag": float(raw.get("low_vol", 0)),
          "dir_down": 1.0 if direction == "down" else 0.0,
          "tf_15m": 1.0 if label == "15m" else 0.0, "tf_1h": 1.0 if label == "1h" else 0.0,
          "tf_4h": 1.0 if label == "4h" else 0.0,
          "shape_15m": raw.get("shape_15m"), "shape_1h": raw.get("shape_1h"),
          "shape_4h": raw.get("shape_4h"),
          "extension_pct": raw.get("extension_pct"), "leg_decay": raw.get("leg_decay"),
          "liq_decel": raw.get("liq_decel"), "climax_bar": raw.get("climax_bar"),
          "vol_sigma": sig, "exh_score": raw.get("exh_score", 0.0),
          "ext_1h_z": raw.get("ext_1h_z"), "ext_24h_z": raw.get("ext_24h_z"),
          "impulse_age_min": raw.get("impulse_age_min"), "momentum_accel": raw.get("momentum_accel"),
          "participation_aligned": raw.get("participation_aligned"),
          "prev_alert_age_min": raw.get("prev_alert_age_min"),
          "prev_alert_move_z": raw.get("prev_alert_move_z"),
          "prev_alert_same_dir": raw.get("prev_alert_same_dir")}
    fv.update(_dow_flags(ts))
    return fv

def compute_signal_score(pct, label, raw, liq, aligned, direction):
    target = TIMEFRAME_TARGET_PCT.get(label, 4.0)
    impulse = min(25.0, max(0.0, (abs(pct) - 1.0) / max(target - 1.0, 0.1) * 25.0))
    v = max(raw.get("vol_1m_x") or 0, raw.get("vol_5m_pace_x") or 0)
    vol = min(15.0, max(0.0, (v - 1.0) / 7.0 * 15.0))
    oi = raw.get("oi_pct"); oi_pts = min(15.0, max(0.0, oi or 0) / 8.0 * 15.0)
    tak = raw.get("taker_imb")
    at = (tak if direction == "up" else -tak) if tak is not None else 0.0
    tak_pts = min(15.0, max(0.0, at) * 15.0)
    liq_pts = min(10.0, (liq / 500_000.0) * 10.0) if liq else 0.0
    brk = 10.0 if aligned else 0.0
    fr = abs(raw.get("fr") or 0)
    fund = 3.0 if fr < 0.0005 else 10.0 if fr <= 0.002 else max(-5.0, 10 - (fr - 0.002) / 0.003 * 15)
    reg = 6.0 * regime_align(direction)
    return round(max(0, min(100, impulse + vol + oi_pts + tak_pts + liq_pts + brk + fund + reg)))

# ---------------------------------------------------------------- history probability
_hist_cache: dict[tuple, tuple] = {}
_hist_cache_gen = 0

def _cluster_factor(n: int) -> float:
    if CLUSTER_WEIGHT_MODE == "off" or n <= 1: return 1.0
    if CLUSTER_WEIGHT_MODE == "full": return 1.0 / float(n)
    return 1.0 / math.sqrt(float(n))

def invalidate_prior_caches():
    global _hist_cache_gen
    _hist_cache.clear()
    try: _fc_cache.clear()
    except NameError: pass
    _hist_cache_gen += 1
    log.info("priors: caches invalidated (gen %d) after bootstrap pass", _hist_cache_gen)

def _hist_rows(band, cat, tf, include_backfill: bool = False):
    now = now_ts(); key = (band, cat, tf, bool(include_backfill), _hist_cache_gen)
    c = _hist_cache.get(key)
    if c and now - c[0] < 300: return c[1]
    conn = None
    try:
        conn = _db()
        backfill_clause = "" if include_backfill else "AND a.source NOT LIKE 'backfill%'"
        rows = conn.execute(f"""SELECT bo.hit,a.archetype,a.score,bo.resolution_source,a.source,
            a.episode_id ep
            FROM band_outcomes bo JOIN alerts a ON a.id=bo.alert_id
            WHERE bo.band=? AND bo.label_version=? AND bo.hit IS NOT NULL AND a.category=?
            AND a.timeframe=? AND a.ts>=?
            {backfill_clause}
            AND (a.source NOT LIKE 'live_nexus%' OR a.delivered_ts IS NOT NULL OR a.telegram_suppressed=1)""",
            (band, OUTCOME_SCHEMA_VERSION, cat, tf, now - BUCKET_WINDOW_DAYS * 86400)).fetchall()
    except Exception as e:
        log.error("history read: %s", e); return []
    finally:
        if conn is not None:
            try: conn.close()
            except Exception: pass
    _hist_cache[key] = (now, rows)
    if len(_hist_cache) > 128: _hist_cache.clear()
    return rows

def _history_probability_from_rows(rows, score, arch, band):
    if not rows: return None, 0.0
    lo, hi, _ = score_bucket(score)
    ep_n: Counter = Counter(r[5] for r in rows if r[5])
    def agg(rs):
        tw = hw = 0.0
        for hit, _a, _sc, rs_, src, ep in rs:
            w = _source_weight(rs_, src)
            if w <= 0: continue
            if ep: w *= _cluster_factor(ep_n[ep])
            tw += w
            if hit == 1: hw += w
        return (hw / tw if tw > 0 else None), tw
    p_all, w_all = agg(rows)
    if band.endswith("div") or band == "rev30":
        prior = DIV_PRIOR
    else:
        prior = min(HIST_PRIOR_HI, max(HIST_PRIOR_LO, p_all)) if p_all is not None else 0.55
    tiers = (agg([r for r in rows if r[1] == arch and lo <= r[2] <= hi]),
             agg([r for r in rows if lo <= r[2] <= hi]),
             agg([r for r in rows if r[1] == arch]))
    for p, w in tiers:
        if p is not None and w >= HIST_MIN_EFF:
            return (prior * HIST_SHRINKAGE + p * w) / (HIST_SHRINKAGE + w), w
    if p_all is not None and w_all >= HIST_BASE_MIN_EFF:
        return (prior * HIST_SHRINKAGE + p_all * w_all) / (HIST_SHRINKAGE + w_all), w_all
    return None, 0.0

def history_probability(cat, tf, score, arch, band):
    live_rows = _hist_rows(band, cat, tf, include_backfill=False)
    p, w = _history_probability_from_rows(live_rows, score, arch, band)
    if p is not None:
        return p, w
    if HISTORICAL_BOOTSTRAP:
        seeded_rows = _hist_rows(band, cat, tf, include_backfill=True)
        p2, w2 = _history_probability_from_rows(seeded_rows, score, arch, band)
        if p2 is not None:
            _stats["prior_backfill_fallbacks"] += 1
            return p2, w2
    return None, 0.0

# ---------------------------------------------------------------- reach & risk
_fc_cache: dict[tuple, tuple] = {}

def _bucket_rows(band, cat, tf, include_backfill: bool = False):
    now = now_ts(); key = (band, cat, tf, include_backfill, "fc")
    c = _fc_cache.get(key)
    if not (c and now - c[0] < 600):
        conn = None
        try:
            conn = _db()
            backfill_clause = "" if include_backfill else "AND a.source NOT LIKE 'backfill%'"
            rows = conn.execute(f"""SELECT bo.mfe, bo.mae, bo.hit FROM band_outcomes bo
                JOIN alerts a ON a.id=bo.alert_id
                WHERE bo.band=? AND bo.label_version=? AND bo.hit IS NOT NULL
                AND a.category=? AND a.timeframe=? AND a.ts>=?
                {backfill_clause}""",
                (band, OUTCOME_SCHEMA_VERSION, cat, tf, now - BUCKET_WINDOW_DAYS * 86400)).fetchall()
            c = (now, [(float(r[0]), float(r[1] or 0.0), int(r[2] or 0)) for r in rows])
        except Exception as e:
            log.debug("reach/risk read %s/%s/%s: %s", band, cat, tf, e)
            c = (now, [])
        finally:
            if conn is not None:
                try: conn.close()
                except Exception: pass
        _fc_cache[key] = c
        if len(_fc_cache) > 64: _fc_cache.clear()
    return c[1]

def _pctl(vals: list, q: float):
    if not vals: return None
    s = sorted(vals)
    idx = min(len(s) - 1, max(0, int(len(s) * q)))
    return s[idx]

def forecast_for(band: str, cat: str, tf: str) -> Optional[float]:
    rows = _bucket_rows(band, cat, tf)
    if len(rows) < 15 and HISTORICAL_BOOTSTRAP:
        # v5.2.0: warm-up fallback — bootstrap rows keep the prediction line
        # alive until 15 live outcomes exist in the bucket.
        rows = _bucket_rows(band, cat, tf, include_backfill=True)
    if len(rows) < 15: return None
    hits = [m for m, _a, h in rows if h == 1 and m > 0]
    src = hits if len(hits) >= 10 else [m for m, _a, _h in rows if m > 0]
    if not src: return None
    m = _pctl(src, 0.70)
    if m is None: return None
    base_band = band[:-3] if band.endswith("div") else band
    tf_of_band = {"now": "5m", "30m": "15m", "1h": "1h", "4h": "4h"}.get(base_band, "5m")
    cap = TIMEFRAME_TARGET_PCT.get(tf_of_band, 5.0) * 1.5
    return round(min(max(m, 0.05), cap), 2)

def risk_for(band: str, cat: str, tf: str) -> Optional[float]:
    rows = _bucket_rows(band, cat, tf)
    if len(rows) < 15 and HISTORICAL_BOOTSTRAP:
        rows = _bucket_rows(band, cat, tf, include_backfill=True)
    if len(rows) < 15: return None
    adv = [abs(a) for _m, a, _h in rows if a < 0]
    if len(adv) < 10: return None
    r = _pctl(adv, 0.50)
    return round(min(r, 20.0), 2) if r else None

# ---------------------------------------------------------------- scorecard / audits
def verdict_scorecard(days=7):
    cut = time.time() - days * 86400
    sc = {b[0]: {"lh": 0, "lm": 0, "sh": 0, "sm": 0} for b in BANDS}
    shadow = {b[0]: [0, 0] for b in BANDS}
    try:
        c = _db()
        for band, dc, hit, direction in c.execute("""SELECT ap.band, ap.display_call, bo.hit, a.direction
            FROM alert_predictions ap JOIN band_outcomes bo ON bo.alert_id=ap.alert_id AND bo.band=ap.band
            AND bo.label_version=? JOIN alerts a ON a.id=ap.alert_id
            WHERE a.ts>=? AND bo.hit IS NOT NULL""", (OUTCOME_SCHEMA_VERSION, cut)):
            d = sc.get(band)
            if d is None: continue
            if not dc: continue
            k = "l" if direction == "up" else "s"
            if dc.startswith("STRONG") or dc in ("LONG", "SHORT") \
               or dc.startswith("LEAN"):
                d[k + ("h" if hit else "m")] += 1
        for band, hit in c.execute("""SELECT b.band, b.hit FROM (
                SELECT 'now' band, hit_now hit FROM shadow_candidates WHERE resolved_ts IS NOT NULL AND ts>=?
                UNION ALL SELECT '30m', hit_30m FROM shadow_candidates WHERE resolved_ts IS NOT NULL AND ts>=?
                UNION ALL SELECT '1h', hit_1h FROM shadow_candidates WHERE resolved_ts IS NOT NULL AND ts>=?
                UNION ALL SELECT '4h', hit_4h FROM shadow_candidates WHERE resolved_ts IS NOT NULL AND ts>=?) b""",
                (cut, cut, cut, cut)):
            if band in shadow: shadow[band][1 if hit else 0] += 1
        c.close()
    except Exception as e:
        log.error("scorecard: %s", e)
    return sc, shadow

def brier_audit(days=7):
    cut = time.time() - days * 86400; out = {}
    try:
        c = _db()
        for b, name, *_ in BANDS:
            rows = c.execute("""SELECT ap.p_final, bo.hit FROM alert_predictions ap
                JOIN band_outcomes bo ON bo.alert_id=ap.alert_id AND bo.band=ap.band AND bo.label_version=?
                JOIN alerts a ON a.id=ap.alert_id
                WHERE ap.band=? AND ap.p_final IS NOT NULL AND bo.hit IS NOT NULL AND a.ts>=?""",
                (OUTCOME_SCHEMA_VERSION, b, cut)).fetchall()
            if rows:
                out[name] = (round(sum((p - h) ** 2 for p, h in rows) / len(rows), 3), len(rows))
            rows = c.execute("""SELECT ap.p_final, bo.hit FROM alert_predictions ap
                JOIN band_outcomes bo ON bo.alert_id=ap.alert_id AND bo.band = ap.band || 'div' AND bo.label_version=?
                JOIN alerts a ON a.id=ap.alert_id
                WHERE ap.band=? AND ap.p_final IS NOT NULL AND bo.hit IS NOT NULL AND a.ts>=?""",
                (OUTCOME_SCHEMA_VERSION, b, cut)).fetchall()
            if rows:
                out[name + "div"] = (round(sum((p - h) ** 2 for p, h in rows) / len(rows), 3), len(rows))
        c.close()
    except Exception as e:
        log.error("brier audit: %s", e)
    return out

def edge_audit(days=7, p_band: str = "30m") -> dict:
    cut = time.time() - days * 86400
    out: dict[str, Any] = {"sel": {}, "disc": [], "n_del": 0, "n_sup": 0}
    try:
        c = _db(); c.row_factory = sqlite3.Row
        for band, _bn, *_ in BANDS:
            rows = c.execute("""SELECT CASE WHEN a.telegram_suppressed=0 THEN 'd' ELSE 's' END pool,
                    CAST(ap.p_final*20 AS INT)/20.0 pb, COUNT(*) n, AVG(bo.hit) hr
                FROM alerts a
                JOIN alert_predictions ap ON ap.alert_id=a.id AND ap.band=?
                JOIN band_outcomes bo ON bo.alert_id=a.id AND bo.band=? AND bo.label_version=?
                 AND bo.hit IS NOT NULL
                WHERE a.source LIKE 'live_nexus%' AND a.ts>=? AND a.timeframe<>'rev30'
                  AND ap.p_final IS NOT NULL
                GROUP BY pool, pb""", (band, band, OUTCOME_SCHEMA_VERSION, cut)).fetchall()
            strat: dict[float, dict] = {}
            for r in rows:
                b = strat.setdefault(float(r["pb"]), {})
                b[r["pool"]] = (int(r["n"]), float(r["hr"]))
                if r["pool"] == "d": out["n_del"] += int(r["n"])
                else: out["n_sup"] += int(r["n"])
            out["sel"][band] = {f"{p:.2f}": v for p, v in sorted(strat.items())}
        rows = c.execute("""SELECT CAST(ap.p_final*20 AS INT)/20.0 pb, COUNT(*) n, AVG(bo.hit) hr
            FROM alert_predictions ap
            JOIN band_outcomes bo ON bo.alert_id=ap.alert_id AND bo.band=ap.band
              AND bo.label_version=? AND bo.hit IS NOT NULL
            JOIN alerts a ON a.id=ap.alert_id
            WHERE ap.band=? AND ap.p_final IS NOT NULL AND a.source LIKE 'live_nexus%' AND a.ts>=?
            GROUP BY pb ORDER BY pb""", (OUTCOME_SCHEMA_VERSION, p_band, cut)).fetchall()
        out["disc"] = [{"p": float(r["pb"]), "n": int(r["n"]), "hr": float(r["hr"])} for r in rows]
        c.close()
    except Exception as e:
        log.error("edge audit: %s", e)
    return out

def missed_opportunity_audit(days=7):
    cut = time.time() - days * 86400
    out: dict[str, Any] = {"candidates": 0, "hits": {b[0]: [0, 0] for b in BANDS},
                           "mfe3": 0, "mfe5": 0, "mfe10": 0}
    try:
        c = _db(); c.row_factory = sqlite3.Row
        rows = c.execute("""SELECT a.id,
               MAX(CASE WHEN ap.display_call IS NULL THEN 0
                        WHEN ap.display_call IN ('NO EDGE','—','UNCLEAR','AVOID') THEN 0 ELSE 1 END) called
            FROM alerts a JOIN alert_predictions ap ON ap.alert_id=a.id
            WHERE a.ts>=? AND a.source LIKE 'live_nexus%'
            GROUP BY a.id""", (cut,)).fetchall()
        uncalled = [r["id"] for r in rows if not r["called"]]
        out["candidates"] = len(uncalled)
        for i in range(0, len(uncalled), 400):
            chunk = uncalled[i:i + 400]
            qmarks = ",".join("?" * len(chunk))
            for band, hit, mfe in c.execute(
                    f"""SELECT band, hit, mfe FROM band_outcomes
                        WHERE alert_id IN ({qmarks}) AND label_version=? AND hit IS NOT NULL""",
                    (*chunk, OUTCOME_SCHEMA_VERSION)):
                if band in out["hits"]:
                    out["hits"][band][0] += int(hit or 0); out["hits"][band][1] += 1
                m = mfe or 0.0
                if m > 3: out["mfe3"] += 1
                if m > 5: out["mfe5"] += 1
                if m > 10: out["mfe10"] += 1
        c.close()
    except Exception as e:
        log.error("missed-opportunity audit: %s", e)
    return out

# ---------------------------------------------------------------- tracking
@dataclass(slots=True)
class BandLive:
    mfe: float = 0.0
    mae: float = 0.0
    hit: Optional[int] = -1          # -1 pending, 0 miss, 1 hit, None = incomplete
    outcome: Optional[str] = None
    tgt_ts: Optional[float] = None
    first_min: Optional[float] = None
    mfe_min: Optional[float] = None
    mae_min: Optional[float] = None
    final_mv: Optional[float] = None
    edit_sent: bool = False
    coverage_ok: bool = True          # v5.1.1
    last_sample_ts: Optional[float] = None
    samples: int = 0

@dataclass(slots=True)
class TrackState:
    alert_id: int
    symbol: str
    direction: str
    tf: str
    detected_ts: float
    detected_price: float
    message_id: Optional[int] = None
    entry_ts: Optional[float] = None
    entry_price: Optional[float] = None
    calls: dict = field(default_factory=dict)
    bands: dict = field(default_factory=lambda: {b[0]: BandLive() for b in BANDS})
    band_dirs: dict = field(default_factory=dict)
    div_done: set = field(default_factory=set)
    done: set = field(default_factory=set)
    last_edit: float = 0.0
    exh: float = 0.0
    exh_rev_sent: bool = False
    peak_mv: float = 0.0
    is_call: bool = True
    text0: str = ""
    link: str = ""
    forecast: float = 0.0

_active_tracks: dict[int, TrackState] = {}
_pending_alert_keys: set[str] = set()
_ret_pending: dict[int, dict] = {}

def _calls_make_call(calls: dict) -> bool:
    for cl in (calls or {}).values():
        dc = cl.get("display_call") or ""
        if dc and dc not in ("NO EDGE", "—", "UNCLEAR", "AVOID"):
            return True
    return False

def register_track(st: TrackState):
    _active_tracks[st.alert_id] = st
    ACTIVE_TRACK_SYMS[st.symbol] += 1
    _ret_pending[st.alert_id] = {"det_ts": st.detected_ts, "det_px": st.detected_price,
                                 "sym": st.symbol, "pending": {5, 15, 30, 60, 240, 1440}}
    _stats["tracking_registered"] += 1
    spawn(_track_loop(st))

def track_line(st: TrackState) -> str:
    parts = []
    for band, bname, *_ in BANDS:
        bl = st.bands[band]
        if bl.hit == 1:
            fast = bl.tgt_ts is not None and bl.tgt_ts <= FAST_HIT_SEC
            icon = "⚡✅" if fast else "✅"
            parts.append(f"{bname} {icon} {bl.mfe:+.2f}%")
        elif bl.hit == 0:
            mv = bl.final_mv if bl.final_mv is not None else bl.mae
            parts.append(f"{bname} {'◌' if mv > 0 else '❌'} {mv:+.2f}%")
        elif bl.hit is None:
            parts.append(f"{bname} ⚠️ incomplete")
        else:
            parts.append(f"{bname} ⏳")
    return "TRACK · " + " · ".join(parts)

def forecast_grade_line(st: TrackState) -> str:
    fc = st.forecast
    if not fc: return ""
    bl = st.bands.get("30m")
    if bl is None: return ""
    if bl.hit is None:
        return f"typical reach: {fc:+.1f}%/30m → incomplete data"
    actual = bl.final_mv if bl.final_mv is not None else (bl.mfe if bl.hit >= 0 else None)
    if actual is None: return ""
    if bl.hit == 1:
        ratio = actual / max(fc, 0.05)
        tail = "🚀" if ratio >= 1.5 else "✓"
        return f"typical reach: {fc:+.1f}%/30m → actual {actual:+.1f}% {tail}"
    if actual > 0:
        return f"typical reach: {fc:+.1f}%/30m → actual {actual:+.1f}% · short of reach"
    return f"typical reach: {fc:+.1f}%/30m → actual {actual:+.1f}% 💀"

async def _edit_track(st: TrackState, final: bool = False) -> bool:
    if not st.message_id: return False
    txt = st.text0 + "\n\n" + track_line(st) + "\n" + st.link
    if final:
        fcl = forecast_grade_line(st)
        if fcl: txt += "\n" + fcl
    ok = await edit_msg(st.message_id, txt)
    if ok:
        _stats["tracking_edits"] += 1
    return ok

async def _track_loop(st: TrackState):
    try:
        if st.entry_price is None:
            await asyncio.sleep(BAND_DELAY_SEC)
            s = SYMS.get(st.symbol)
            now = now_ts()
            if not fresh_price(s, now):
                # Can't record an entry from a stale feed; mark all bands incomplete.
                await _mark_all_incomplete(st, "no_fresh_entry")
                return
            st.entry_ts = now; st.entry_price = s.px
            try:
                await asyncio.to_thread(_db_retry, db_set_entry_ts, st.alert_id, s.px, st.entry_ts)
            except Exception as e:
                log.error("track %s entry write: %s", st.alert_id, e)
        target = {b[0]: b[4] for b in BANDS}
        span = {b[0]: (b[2] * 60.0, b[3] * 60.0) for b in BANDS}
        while not _shutdown.is_set():
            if len(st.done) + len(st.div_done) >= len(BANDS): break
            try:
                now = now_ts()
                s = SYMS.get(st.symbol)
                # v5.2.0: tracking tolerates a quieter feed than alert firing —
                # a short ticker lull must not poison a whole band as 'incomplete'.
                px = s.px if (s and math.isfinite(s.px) and s.px > 0
                              and 0 <= now - s.px_ts <= TRACK_FRESH_SEC) else None
                raw_mv = None
                if px and st.entry_price:
                    raw_mv = (px / st.entry_price - 1.0) * 100.0
                if raw_mv is not None and px:
                    st.peak_mv = max(st.peak_mv, raw_mv * (-1.0 if st.direction == "down" else 1.0))
                for band in BANDS_ORDER:
                    if band in st.done or band in st.div_done: continue
                    d0, d1 = span[band]
                    el = now - (st.entry_ts or now)
                    bl = st.bands[band]
                    if el < d0: continue
                    bdir = st.band_dirs.get(band, st.direction)
                    # v5.1.1: coverage tracking — a feed gap must not silently become a timeout.
                    if px is None:
                        bl.coverage_ok = False
                    mv = raw_mv * (1.0 if bdir == "up" else -1.0) if raw_mv is not None else None
                    if mv is not None:
                        bl.last_sample_ts = now
                        bl.samples += 1
                        if el < d1:
                            if mv > bl.mfe:
                                bl.mfe = mv; bl.mfe_min = el / 60.0
                            if mv < bl.mae:
                                bl.mae = mv; bl.mae_min = el / 60.0
                            if bl.tgt_ts is None and bl.mfe >= target[band]:
                                bl.tgt_ts = el
                    if el >= d1:
                        complete = (bl.coverage_ok and bl.samples > 0
                                    and bl.last_sample_ts is not None
                                    and (now - bl.last_sample_ts) <= TRACK_MAX_SAMPLE_GAP_SEC)
                        rec_source = "nexus_tick"
                        if bl.mfe >= target[band]:
                            bl.hit = 1; bl.outcome = "target"
                        elif complete:
                            bl.hit = 0; bl.outcome = "timeout"
                            bl.final_mv = mv
                        else:
                            # v5.2.0: last chance — rebuild this band from 1m klines
                            # before recording it as unknown.
                            rec = await _reconstruct_band(st, band, d0, d1, target[band], bdir)
                            if rec is not None:
                                bl.hit, bl.outcome = rec
                                rec_source = "nexus_kline_reconstruct"
                            else:
                                bl.hit = None; bl.outcome = "incomplete"
                                bl.final_mv = None
                        bl.first_min = (bl.tgt_ts / 60.0) if bl.tgt_ts is not None else None
                        st.done.add(band)
                        store_band = band + "div" if bdir != st.direction else band
                        if bdir != st.direction: _stats["div_rows"] += 1
                        if bl.hit is None: _stats["tracking_incomplete"] += 1
                        await asyncio.to_thread(
                            _db_retry, db_set_outcome, st.alert_id, store_band,
                            bl.hit if bl.hit is not None else 0,
                            bl.outcome, bl.mfe, bl.mae, bl.mfe_min, bl.mae_min, bl.first_min,
                            rec_source, st.entry_price or 0.0, bl.final_mv,
                            exclude=(bl.hit is None), target=target[band])
                        _stats["tracking_resolved"] += 1
                        if BAND_END_EDIT_ONLY and st.message_id and not bl.edit_sent:
                            # v5.2.0: only mark sent when the edit actually went out.
                            if await _edit_track(st):
                                bl.edit_sent = True
            except Exception as e:
                _stats["tracking_errors"] += 1
                log.error("track %s tick: %s", st.alert_id, e)
            await asyncio.sleep(TRACK_TICK_SEC)
        if st.message_id:
            if not await _edit_track(st, final=True):
                await asyncio.sleep(10)
                await _edit_track(st, final=True)
    except asyncio.CancelledError:
        raise
    finally:
        _active_tracks.pop(st.alert_id, None)
        n = ACTIVE_TRACK_SYMS.get(st.symbol, 0) - 1
        if n > 0: ACTIVE_TRACK_SYMS[st.symbol] = n
        else: ACTIVE_TRACK_SYMS.pop(st.symbol, None)

async def _reconstruct_band(st: TrackState, band: str, d0: float, d1: float,
                            tgt: float, bdir: str):
    """v5.2.0: rebuild one band from 1m klines when live coverage was lost.

    Bars are indexed by open time against the band window (no positional skew).
    Edge bars may bleed up to 59s outside the window — far tighter than
    recording the band as unknown. Returns (hit, outcome) or None."""
    if not rest_ok() or not st.entry_price or not st.entry_ts:
        return None
    start = st.entry_ts + d0 * 60.0
    end = st.entry_ts + d1 * 60.0
    if now_ts() < end - 60:
        return None
    try:
        async with SESSION.get(f"{FAPI}/fapi/v1/klines",
                               params={"symbol": st.symbol, "interval": "1m",
                                       "startTime": int(start * 1000),
                                       "limit": min(300, int(d1 - d0) + 2)},
                               timeout=aiohttp.ClientTimeout(total=12)) as r:
            rest_guard(r.status)
            if r.status != 200:
                return None
            kl = await r.json()
    except Exception:
        return None
    if not isinstance(kl, list):
        return None
    sign = 1.0 if bdir == "up" else -1.0
    entry = float(st.entry_price)
    if entry <= 0:
        return None
    mfe = 0.0; mae = 0.0; mfe_min = mae_min = None
    final_close = None
    for k in kl:
        o = int(k[0]) / 1000.0
        if o + 60.0 <= start or o >= end:
            continue
        h, l, c = to_f(k[2]), to_f(k[3]), to_f(k[4])
        if h <= 0 or l <= 0:
            continue
        fav = ((h - entry) if sign > 0 else (entry - l)) / entry * 100.0
        adv = ((l - entry) if sign > 0 else (entry - h)) / entry * 100.0
        if fav > mfe:
            mfe = fav; mfe_min = max(0.0, (o - start) / 60.0)
        if adv < mae:
            mae = adv; mae_min = max(0.0, (o - start) / 60.0)
        if c > 0:
            final_close = c
    if final_close is None:
        return None
    hit = 1 if mfe >= tgt else 0
    bl = st.bands[band]
    bl.mfe, bl.mae = mfe, mae
    bl.mfe_min, bl.mae_min = mfe_min, mae_min
    bl.final_mv = (final_close / entry - 1.0) * 100.0 * sign
    bl.hit = hit
    bl.outcome = "target" if hit else "timeout"
    return bl.hit, bl.outcome

async def _mark_all_incomplete(st: TrackState, reason: str):
    """v5.1.1: cannot observe → record as unknown, never as a miss."""
    for band in BANDS_ORDER:
        bl = st.bands[band]
        bl.hit = None
        bl.outcome = reason
        st.done.add(band)
        await asyncio.to_thread(
            _db_retry, db_set_outcome, st.alert_id, band,
            0, reason, 0.0, 0.0, None, None, None,
            "nexus_tick", st.entry_price or st.detected_price or 0.0, None,
            True, target=next((t for b, _n, _d0, _d1, t in BANDS if b == band), 1.0))
        _stats["tracking_incomplete"] += 1
    try:
        await _edit_track(st, final=True)
    except Exception:
        pass

async def recovery_worker():
    await asyncio.sleep(20)
    while not _shutdown.is_set():
        try:
            rows = await asyncio.to_thread(db_pending_tracks)
            for r in rows:
                aid = r["id"]
                if aid in _active_tracks: continue
                det_ts = r["detected_ts"] or r["ts"] or 0.0
                age = now_ts() - det_ts
                if r["timeframe"] == "rev30":
                    continue
                if age > 240 * 60 + 120:
                    entry_px = r["entry_price_10s"] or r["detected_price"] or 0.0
                    direction = r["direction"] or "up"
                    reconstructed = 0
                    if entry_px > 0 and rest_ok():
                        try:
                            async with SESSION.get(f"{FAPI}/fapi/v1/klines",
                                    params={"symbol": r["symbol"], "interval": "5m",
                                            "startTime": int((det_ts + BAND_DELAY_SEC) * 1000),
                                            "limit": 52},
                                    timeout=aiohttp.ClientTimeout(total=12)) as rsp:
                                rest_guard(rsp.status)
                                kl = await rsp.json() if rsp.status == 200 else None
                            if isinstance(kl, list) and len(kl) >= 4:
                                sign = 1.0 if direction == "up" else -1.0
                                calls = _parse_json(r["calls"], {})
                                for band, _bn, d0, d1, tgt in BANDS:
                                    j0 = int(d0 * 60 / 300)
                                    j1 = int(d1 * 60 / 300)
                                    if j1 > len(kl) or j0 >= len(kl):
                                        continue
                                    mfe = 0.0; mae = 0.0
                                    mfe_min = mae_min = None
                                    for jj in range(j0, min(j1, len(kl))):
                                        h_, l_ = to_f(kl[jj][2]), to_f(kl[jj][3])
                                        fav = ((h_ - entry_px) if sign > 0 else (entry_px - l_)) / entry_px * 100.0
                                        adv = ((l_ - entry_px) if sign > 0 else (entry_px - h_)) / entry_px * 100.0
                                        if fav > mfe: mfe = fav; mfe_min = (jj + 1) * 5.0
                                        if adv < mae: mae = adv; mae_min = (jj + 1) * 5.0
                                    hit = 1 if mfe >= tgt else 0
                                    close_final = to_f(kl[min(j1, len(kl)) - 1][4])
                                    final_mv = (close_final - entry_px) / entry_px * 100.0 * sign
                                    store_band = band
                                    bdir = (calls.get(band) or {}).get("dir")
                                    if bdir and bdir != direction:
                                        store_band = band + "div"
                                        final_mv = abs(final_mv) if hit else final_mv
                                    await asyncio.to_thread(
                                        _db_retry, db_set_outcome, aid, store_band, hit,
                                        "target" if hit else "timeout", mfe, mae, mfe_min, mae_min, None,
                                        "nexus_kline_reconstruct", entry_px, final_mv)
                                    reconstructed += 1
                        except Exception as e:
                            log.error("recovery reconstruct %s: %s", aid, e)
                    if not reconstructed:
                        for b, _n, _d0, _d1, _t in BANDS:
                            await asyncio.to_thread(
                                _db_retry, db_set_outcome, aid, b, None, "unresolved",
                                0.0, 0.0, None, None, None, "nexus_restart_1m",
                                entry_px, None, exclude=True)
                    continue
                if not r["entry_price_10s"]:
                    for b, _n, _d0, _d1, _t in BANDS:
                        await asyncio.to_thread(
                            _db_retry, db_set_outcome, aid, b, 0, "no_entry",
                            0.0, 0.0, None, None, None, "nexus_restart_1m",
                            r["detected_price"] or 0.0, None, True)
                    continue
                sym = r["symbol"] or ""
                stored_calls = _parse_json(r["calls"], {})
                st = TrackState(aid, sym, r["direction"] or "up", r["timeframe"] or "5m",
                                det_ts, r["detected_price"] or 0.0,
                                entry_ts=det_ts + BAND_DELAY_SEC, entry_price=r["entry_price_10s"],
                                message_id=r["telegram_message_id"], calls=stored_calls)
                st.is_call = _calls_make_call(stored_calls)
                st.text0 = r["base_message"] or f"*{sym}*"
                st.link = trade_link_md(sym)
                st.forecast = float(stored_calls.get("30m", {}).get("forecast") or 0.0)
                for band in BANDS_ORDER:
                    d = (stored_calls.get(band) or {}).get("dir")
                    if d: st.band_dirs[band] = d
                try:
                    prior_rows = await asyncio.to_thread(db_existing_outcomes, aid)
                except Exception:
                    prior_rows = {}
                for bname, br in prior_rows.items():
                    base = bname[:-3] if bname.endswith("div") else bname
                    if bname.endswith("div"):
                        st.div_done.add(base)
                        continue
                    bl = st.bands.get(bname)
                    if bl is None or br["hit"] is None and br["outcome"] not in ("unresolved", "no_entry", "incomplete"):
                        # unresolved rows are treated as pending so the band can be re-observed
                        pass
                    if bl is None: continue
                    if br["hit"] is None and br["outcome"] in ("unresolved", "no_entry", "incomplete"):
                        # leave band pending
                        continue
                    if br["hit"] is None: continue
                    bl.hit = int(br["hit"]); bl.outcome = br["outcome"]
                    bl.mfe = br["mfe"] or 0.0; bl.mae = br["mae"] or 0.0
                    bl.first_min = br["first_min"]
                    bl.final_mv = br["final_market_move_pct"]
                    bl.edit_sent = True
                    st.done.add(bname)
                register_track(st)
        except Exception as e:
            log.error("recovery: %s", e)
        await _shutdown_or_sleep(TRACK_RECOVERY_INTERVAL_SEC)

async def returns_sweeper():
    while not _shutdown.is_set():
        try:
            now = now_ts()
            for aid in list(_ret_pending):
                e = _ret_pending[aid]
                s = SYMS.get(e["sym"]); px = s.px if s and s.px > 0 else None
                for m in list(e["pending"]):
                    if now - e["det_ts"] >= m * 60:
                        if now - e["det_ts"] <= m * 60 + 900 and px and e["det_px"]:
                            await asyncio.to_thread(_db_retry, db_set_return, aid, m,
                                                    (px / e["det_px"] - 1.0) * 100.0)
                        e["pending"].discard(m)
                if not e["pending"]:
                    _ret_pending.pop(aid, None)
        except Exception as e:
            log.error("returns: %s", e)
        await _shutdown_or_sleep(60)

# ---------------------------------------------------------------- shadow log
_shadow_last: dict[str, float] = {}

def _shadow_insert(sym, direction, tf, price, ts, reason):
    c = _db()
    c.execute("""INSERT INTO shadow_candidates(symbol,direction,timeframe,ts,block_reason,price)
                 VALUES(?,?,?,?,?,?)""", (sym, direction, tf, float(ts), reason, float(price)))
    c.commit(); c.close()

async def log_shadow(s: Sym, label, direction, price, now, reason):
    key = f"{s.sym}|{label}|{direction}"
    last = _shadow_last.get(key, 0.0)
    if now - last < 1800: return
    _shadow_last[key] = now
    if len(_shadow_last) > 4000:
        for k in list(_shadow_last.keys())[:2000]:
            _shadow_last.pop(k, None)
    _stats["shadow_logged"] += 1
    await asyncio.to_thread(_shadow_insert, s.sym, direction, label, price, now, reason)

def _shadow_due_rows(cut: float):
    c = _db()
    rows = c.execute("""SELECT id,symbol,direction,ts,entry_price FROM shadow_candidates
        WHERE resolved_ts IS NULL AND ts>=?""", (cut,)).fetchall()
    c.close(); return rows

def _shadow_set_entry(sid: int, px: float):
    c = _db()
    c.execute("UPDATE shadow_candidates SET entry_price=? WHERE id=?", (float(px), sid))
    c.commit(); c.close()

async def shadow_resolver():
    while not _shutdown.is_set():
        try:
            now = now_ts()
            rows = await asyncio.to_thread(_shadow_due_rows, now - 260 * 60)
            for sid, sym, direction, ts, entry in rows:
                s = SYMS.get(sym); px = s.px if (s and s.px > 0) else None
                if px is None: continue
                if not entry:
                    if now - ts >= BAND_DELAY_SEC:
                        await asyncio.to_thread(_shadow_set_entry, sid, px)
                    continue
                sign = 1.0 if direction == "up" else -1.0
                mvv = (px / entry - 1.0) * 100.0 * sign
                def _upd():
                    c = _db(); c.row_factory = sqlite3.Row
                    r = c.execute("SELECT * FROM shadow_candidates WHERE id=?", (sid,)).fetchone()
                    if r is None: c.close(); return
                    el = now - ts; done = True
                    for bname, _bn, d0, d1, tgt in BAND_SPANS:
                        col_m, col_h = f"mfe_{bname}", f"hit_{bname}"
                        cur_mfe = r[col_m] or 0.0
                        mfe = max(cur_mfe, mvv) if el >= d0 * 60 else cur_mfe
                        hit = r[col_h]
                        if hit is None:
                            done = False
                            if mfe >= tgt: hit = 1
                            elif el >= d1 * 60: hit = 0
                        c.execute(f"UPDATE shadow_candidates SET {col_m}=?, {col_h}=? WHERE id=?",
                                  (mfe, hit, sid))
                    if done:
                        c.execute("UPDATE shadow_candidates SET resolved_ts=? WHERE id=?", (now, sid))
                    c.commit(); c.close()
                await asyncio.to_thread(_upd)
        except Exception as e:
            log.error("shadow resolver: %s", e)
        await _shutdown_or_sleep(30)

# ---------------------------------------------------------------- decision vocabulary
TIER_RANK = {"STRONG LONG": 6, "STRONG SHORT": 6, "LONG": 5, "SHORT": 5,
             "LEAN LONG": 4, "LEAN SHORT": 4,
             "UNCLEAR": 2, "AVOID": 1, "NO EDGE": 1, "—": 0}

def _verdict_icon(dc: str):
    if dc in ("—", "", "NO EDGE"): return ("○" if dc == "NO EDGE" else "⚪")
    if dc.startswith("STRONG"): return "🟩" if "LONG" in dc else "🟥"
    if dc in ("LONG", "SHORT"): return "🟢" if dc == "LONG" else "🔴"
    if dc.startswith("LEAN"): return "🔵" if "LONG" in dc else "🟠"
    if dc == "UNCLEAR": return "🟨"
    if dc == "AVOID": return "⬜️"
    return "⚪"

def _decision_word(p: Optional[float], direction: str, strong_floor_ok: bool) -> str:
    if p is None: return "UNCLEAR"
    word = "LONG" if direction == "up" else "SHORT"
    if p >= CALL_STRONG and strong_floor_ok: return f"STRONG {word}"
    if p >= CALL_STRONG: return word
    if p >= CALL_ACTIONABLE: return word
    if p >= CALL_LEAN: return f"LEAN {word}"
    if p >= NOTIFY_MIN_P: return "UNCLEAR"
    return "AVOID"

def _ml_disp(c: dict) -> str:
    h = c.get("p_hist")
    p = c.get("p_final")
    if h is None and p is None: return "—"
    hs = f"H{h*100:.0f}" if h is not None else "H—"
    ps = f"CAL{p*100:.0f}%" if p is not None else "CAL—"
    return f"{hs} → {ps}"

def _ladder_rows(calls: dict) -> list:
    rows = []
    for band, bname, *_ in BANDS:
        c = calls.get(band) or {}
        dc = c.get("display_call") or "UNCLEAR"
        icon = _verdict_icon(dc)
        cal = _ml_disp(c)
        if c.get("div"):
            cont_p = c.get("cont_p")
            ctx = f" · cont {cont_p*100:.0f}%" if cont_p is not None else ""
            rows.append(f"{icon} {bname} · {dc} · {cal}{ctx}")
        else:
            rows.append(f"{icon} {bname} · {dc} · {cal}")
    return rows

def _risk_tier(rr: float) -> str:
    if rr >= RISK_RR_LOW: return "LOW"
    if rr >= RISK_RR_MED: return "MED"
    if rr >= RISK_RR_HIGH: return "HIGH"
    return "EXTREME"

def render_alert(s: Sym, label, direction, pct, now, raw, calls,
                 exh, chase, thin_vol, whale: bool, whale_usd: float,
                 whale_side: Optional[str], flip_note: str = "",
                 parabolic: bool = False, wash_note: str = "") -> str:
    dir30 = (calls.get("30m") or {}).get("dir") or direction
    icon = "🟢" if dir30 == "up" else "🔴"
    if whale:
        icon = "🐋" if whale_side == "buy" else "🩸"
    if parabolic:
        icon = "🪫" if icon in ("🟢", "🔴") else icon
    head = f"{icon} {s.sym} {pct:+.2f}% ({label})" + (" · VOL" if whale else "")
    brk = breakout_dir(s, now, 3600)
    tag = {"up": "1h high breakout", "down": "1h low breakdown"}.get(
        brk, "momentum" if label in ("5m", "15m") else "range move")
    lines = [head, f"💲 ${s.px:.6g} · 🧭 {tag}", ""]
    lines.extend(_ladder_rows(calls))
    lines.append("")
    c30 = calls.get("30m") or {}
    fc30 = c30.get("forecast")
    p30 = c30.get("p_final")
    word = "UP" if dir30 == "up" else "DOWN"
    sign = "+" if dir30 == "up" else "−"
    mag = f"{sign}{abs(fc30):.1f}%/30m" if fc30 is not None else "—/30m"
    ptxt = f"P{p30*100:.0f}%" if p30 is not None else "P—"
    lines.append(f"prediction: {word} {mag} · {ptxt}")
    lines.append("")
    if whale:
        wword = "BUY" if whale_side == "buy" else "SELL"
        against = (whale_side == "buy" and direction == "down") or \
                  (whale_side == "sell" and direction == "up")
        wtxt = f"🐋 WHALE {wword}: {fmt_usd(whale_usd)}"
        if against:
            wtxt += " — AGAINST the move · " + \
                    ("absorption warning" if whale_side == "buy" else "distribution warning")
        else:
            wtxt += " — with the move"
        lines.append(wtxt)
    rtxt = rs_line(s, dir30, now)
    if rtxt: lines.append(rtxt)
    if s.oi_usd > 0 and s.oi_ref > 0:
        d = s.oi_usd - s.oi_ref
        pc_o = (s.oi_usd / s.oi_ref - 1.0) * 100.0
        p30m = pct_change(s, now, 1800)
        fr_ = raw.get("fr")
        if abs(d) < max(50_000, s.oi_ref * 0.001):
            exp = "flat"
        elif d > 0:
            if direction == "up" and fr_ is not None and fr_ <= -FUNDING_SQUEEZE \
               and p30m is not None and p30m < -0.3:
                exp = f"squeeze forming — shorts trapped (30m px {p30m:+.1f}%)"
            elif direction == "down" and fr_ is not None and fr_ >= FUNDING_SQUEEZE \
                 and p30m is not None and p30m > 0.3:
                exp = f"squeeze forming — longs trapped (30m px {p30m:+.1f}%)"
            elif p30m is not None and p30m >= 0:
                exp = f"bullish expansion (30m px {p30m:+.1f}%)"
            elif p30m is not None:
                exp = f"bearish expansion (30m px {p30m:+.1f}%)"
            else:
                exp = "expansion"
        else:
            exp = "deleveraging"
        lines.append(f"🐋 OI {fmt_usd(s.oi_usd)} · {pc_o:+.1f}%/30m ({fmt_usd(d, signed=True)}) · {exp}")
    ctx = []
    corr = raw.get("btc_corr")
    if corr is not None: ctx.append(f"BTC corr {corr:+.2f}")
    sp = raw.get("spread")
    if sp is not None: ctx.append(f"Spread {sp:.3f}%")
    if s.ls and s.ls > 0: ctx.append(f"L/S {s.ls:.2f}")
    L, S = liq_sides(s, 300)
    if max(L, S) > 50_000:
        side, amt = ("Long", L) if L >= S else ("Short", S)
        ctx.append(f"💥 {side} liq {fmt_usd(amt)} (5m)")
    if ctx:
        lines.append(" · ".join(ctx))
    v1, v5_ = raw.get("vol_1m_x"), raw.get("vol_5m_pace_x")
    vol_s = f"📊 Vol 1m {_vol_disp(v1)} · 5m pace {_vol_disp(v5_)}"
    if raw.get("low_vol"): vol_s += " · low-vol"
    if thin_vol: vol_s += " · THIN"
    lines.append(vol_s)
    tk = raw.get("taker_imb")
    seg = "⚖️ Taker —" if tk is None else (
        f"⚖️ Taker {tk:+.2f} " + ("buy-side" if tk > 0.05 else "sell-side" if tk < -0.05 else "balanced"))
    lines.append(seg)
    fr, bp = raw.get("fr"), raw.get("basis")
    seg = "🧪 Funding —" if fr is None else (
        f"🧪 Funding {fr*100:.3f}%" + (" 🔥" if abs(fr) >= FUNDING_EXTREME else ""))
    if bp is not None:
        seg += f" · Basis {bp*100:+.2f}%"
    lines.append(seg)
    if parabolic:
        lines.append("🪫 PARABOLIC TAPE — P capped; manipulation-prone")
    if wash_note:
        lines.append(wash_note)
    if chase >= 30 or (exh is not None and exh >= EXH_TIER_MATURE):
        lines.append(f"⚠️ chase {chase_tier_word(chase)} {chase} · exh {exh_tier_word(exh)} {exh if exh is not None else '—'}")
    if flip_note:
        lines.append(flip_note)
    lines.append("")
    c30b = calls.get("30m") or {}
    fc30 = c30b.get("forecast")
    dir30b = c30b.get("dir") or direction
    cat = raw.get("_cat") or ("pump" if dir30b == "up" else "dump")
    bucket = "30mdiv" if c30b.get("div") else "30m"
    reward = fc30 if fc30 is not None else next((t for b, _n, _d0, _d1, t in BANDS if b == "30m"), 1.20)
    risk = risk_for(bucket, cat, label)
    rr = (reward / risk) if (risk is not None and risk > 0) else None
    tier = _risk_tier(rr) if rr is not None else None
    if parabolic and tier != "EXTREME":
        tier = "EXTREME"
    if tier:
        lines.append(f"🛡 Risk: {tier} (reward +{reward:.1f}% · risk -{(risk or 0):.1f}%)")
    return "\n".join(lines)

# ---------------------------------------------------------------- alert pipeline
def cooldown_for(s: Sym, label: str, base: float) -> float:
    now = now_ts()
    recent = sum(1 for t in s.alert_ts if now - t < 600)
    return base * COOLDOWN_SCALE * min(COOLDOWN_MAX_MULT, 1.0 + 0.5 * recent)

async def fetch_real_volume(s: Sym) -> dict:
    out = {"vol_1m_x": None, "vol_5m_pace_x": None, "volume_accel": None,
           "qv_1m_usd": None, "qv_1m_buy": None}
    if not rest_ok(): return out
    try:
        async with SESSION.get(f"{FAPI}/fapi/v1/klines",
                               params={"symbol": s.sym, "interval": "1m", "limit": 40},
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            rest_guard(r.status)
            if r.status != 200: return out
            kl = await r.json()
        if not isinstance(kl, list) or len(kl) < 16: return out
        qvs = [to_f(k[7]) for k in kl]
        closed = qvs[:-1]
        if closed:
            out["qv_1m_usd"] = closed[-1]
            # v5.1.1: column [10] is taker-buy quote volume directly.
            out["qv_1m_buy"] = to_f(kl[-2][10]) if len(kl[-2]) > 10 else to_f(kl[-2][9]) * to_f(kl[-2][4])
        hist = [q for q in closed[:-10] if q > 0]
        med = statistics.median(hist) if len(hist) >= 5 else None
        if not med or med <= 0: return out
        s5 = sum(closed[-5:]) / 5.0
        p5 = sum(closed[-10:-5]) / 5.0
        out["vol_1m_x"] = _clip_vol(closed[-1] / med)
        out["vol_5m_pace_x"] = _clip_vol(s5 / med)
        if p5 > 0: out["volume_accel"] = (s5 / med) / (p5 / med)
    except Exception:
        pass
    return out

async def seed_volume_bars(s: Sym):
    if not rest_ok() or s.m1.n >= 30: return
    try:
        async with SESSION.get(f"{FAPI}/fapi/v1/klines",
                               params={"symbol": s.sym, "interval": "1m", "limit": 120},
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            rest_guard(r.status)
            if r.status != 200: return
            kl = await r.json()
        if not isinstance(kl, list) or not kl: return
        last_local = s.m1.ts[(s.m1.i - 1) % s.m1.cap] if s.m1.n else 0.0
        horizon = now_ts() - 60.0
        for k in kl:
            # v5.1.1: canonicalise to bar-open + interval so seeded bars match
            # the aggregator's close-of-minute convention.
            t = int(k[0]) / 1000.0 + 60.0
            if last_local < t <= horizon:
                s.m1.push(t, to_f(k[1]), to_f(k[2]), to_f(k[3]), to_f(k[4]), to_f(k[7]))
    except Exception:
        pass

def whale_check(name: str, qv_1m_usd, qv_1m_buy) -> tuple[bool, float, Optional[str]]:
    now = now_ts()
    st = _whale_state.get(name)
    if st:
        cut_b = int((now - 60) // 10)
        while st and st[0][0] < cut_b:
            st.popleft()
        gross60 = sum(b + sl for _t, b, sl in st)
        buy60 = sum(b for _t, b, sl in st)
        b10 = sl10 = 0.0
        if st and st[-1][0] >= int((now - 10) // 10):
            b10, sl10 = st[-1][1], st[-1][2]
        gross10 = b10 + sl10
        if gross10 >= SCALP_VOL_FAST_USD and \
           abs(b10 - sl10) >= SCALP_VOL_DIR_RATIO * gross10:
            return True, gross10, ("buy" if b10 >= sl10 else "sell")
        if gross60 >= SCALP_VOL_MIN_USD and \
           abs(buy60 - (gross60 - buy60)) >= SCALP_VOL_DIR_RATIO * gross60:
            return True, gross60, ("buy" if buy60 >= (gross60 - buy60) else "sell")
    if qv_1m_usd and qv_1m_usd >= SCALP_VOL_MIN_USD:
        if qv_1m_buy is None:
            return True, qv_1m_usd, None
        ratio = qv_1m_buy / qv_1m_usd if qv_1m_usd > 0 else 0.0
        if ratio >= SCALP_VOL_DIR_RATIO:
            return True, qv_1m_usd, "buy"
        if (1.0 - ratio) >= SCALP_VOL_DIR_RATIO:
            return True, qv_1m_usd, "sell"
    return False, 0.0, None

async def wash_check(s: Sym) -> tuple[bool, str]:
    if not WASH_ENABLED: return False, ""
    if not rest_ok(): return False, ""
    try:
        async with SESSION.get(f"{FAPI}/fapi/v1/aggTrades",
                               params={"symbol": s.sym, "limit": 500},
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            rest_guard(r.status)
            if r.status != 200: return False, ""
            trades = await r.json()
        if not isinstance(trades, list) or len(trades) < WASH_MIN_TRADES:
            return False, ""
        notionals = [to_f(t.get("q")) * to_f(t.get("p")) for t in trades]
        notionals = [x for x in notionals if x > 0]
        if len(notionals) < WASH_MIN_TRADES: return False, ""
        mean = sum(notionals) / len(notionals)
        if mean <= 0: return False, ""
        var = sum((x - mean) ** 2 for x in notionals) / len(notionals)
        cv = math.sqrt(var) / mean
        if cv < WASH_CV_MAX:
            return True, f"🧪 WASH-SUSPECT: trade-size CV {cv:.2f} < {WASH_CV_MAX} ({len(notionals)} trades)"
        return False, ""
    except Exception:
        return False, ""

def _parabolic_active(s: Sym, now: float, ext1_dir: Optional[float],
                      ext24_dir: Optional[float]) -> bool:
    if not PARABOLIC_ENABLED: return False
    recent = s.onboard > 0 and (now - s.onboard) <= PARABOLIC_MAX_AGE_D * 86400
    lowcap = False
    mc = _market_cap.get(base_of(s.sym))
    if mc is not None and mc <= PARABOLIC_MAX_MCAP: lowcap = True
    ext_hot = ((ext1_dir is not None and abs(ext1_dir) >= PARABOLIC_EXT_1H) or
               (ext24_dir is not None and abs(ext24_dir) >= PARABOLIC_EXT_24H))
    return (recent or lowcap) and ext_hot

async def _confirm_hold(symbol: str, direction: str, det_px: float) -> bool:
    deadline = now_ts() + CONFIRM_HOLD_SEC
    while now_ts() < deadline and not _shutdown.is_set():
        await asyncio.sleep(5)
        s = SYMS.get(symbol)
        if not fresh_price(s, now_ts()):
            _stats["alerts_confirm_cancelled"] += 1
            return False
        sign = 1.0 if direction == "up" else -1.0
        adverse = (s.px / det_px - 1.0) * 100.0 * sign
        if adverse < -0.30:
            _stats["alerts_confirm_cancelled"] += 1
            return False
    if _shutdown.is_set(): return False
    _stats["alerts_confirm_ok"] += 1
    return True

def _burst_should_collapse(direction: str, label: str, now: float) -> bool:
    key = (direction, label)
    until = _burst_active_until.get(key, 0.0)
    if now < until:
        return True
    dq = _burst_ledger.get(key)
    if dq is None:
        dq = _burst_ledger[key] = deque(maxlen=64)
    dq.append(now)
    while dq and dq[0] < now - BURST_WINDOW_SEC:
        dq.popleft()
    if len(dq) >= BURST_MIN_COINS:
        _burst_active_until[key] = now + BURST_WINDOW_SEC
        return True
    return False

def _expire_delivered_p(now: float):
    stale = [k for k, v in _last_delivered_p.items() if now - v[1] > REALERT_P_EXPIRY_SEC]
    for k in stale:
        _last_delivered_p.pop(k, None)
    stale_push = [k for k, v in _last_push_p.items() if now - v[0] > 6 * 3600]
    for k in stale_push:
        _last_push_p.pop(k, None)
    stale_gov = [k for k, v in _last_sym_push_ts.items() if now - v > 2 * GOV_PUSH_GAP_SEC]
    for k in stale_gov:
        _last_sym_push_ts.pop(k, None)
    stale_snap = [k for k, (t, *_r) in _last_sym_snapshot.items() if now - t > GOV_SNAPSHOT_DEDUP_SEC * 4]
    for k in stale_snap:
        _last_sym_snapshot.pop(k, None)

def _chop_ok(name: str, now: float) -> bool:
    dq = _sym_push_log.get(name)
    if not dq: return True
    return sum(1 for t in dq if now - t < CHOP_WINDOW_SEC) < CHOP_MAX_PUSHES

def _track_budget_ok(pushed: bool) -> bool:
    return pushed or len(_active_tracks) < MAX_SILENT_TRACKS

async def _store_and_track(s: Sym, name: str, cat: str, direction: str, label: str,
                           score: int, det_px: float, det_ts: float, fv: dict,
                           archetype: str, calls: dict, low_vol: int, body: str,
                           exh: float, message_id: Optional[int],
                           pushed: bool = False, arm_gate: bool = False,
                           cd_key: str = "", gate_p: Optional[float] = None,
                           gate_rank: int = 0, gate_raw: Optional[float] = None):
    aid = await asyncio.to_thread(
        _db_retry, db_insert_alert, name, cat, direction, label, score, det_px, det_ts,
        now_ts(), message_id, fv, archetype, calls, low_vol, body)
    s.alert_ts.append(det_ts)
    s.last_alert_px = det_px; s.last_alert_ts = det_ts; s.last_alert_dir = direction
    now = now_ts()
    if arm_gate and gate_p is not None:
        _last_delivered_p[f"{name}|{direction}"] = (gate_p, now, gate_rank)
    if pushed:
        _last_alerted[cd_key] = now
        _last_push_p[f"{name}|{direction}"] = (now, gate_raw or gate_p or 0.0, label)
        _sym_push_log.setdefault(name, deque(maxlen=8)).append(now)
        if GOVERNOR_ENABLED:
            _last_sym_push_ts[name] = now
            _last_sym_snapshot[name] = (now, det_px, s.oi_usd, label, direction)
    if not _track_budget_ok(pushed):
        return aid
    fc30 = (calls or {}).get("30m", {}).get("forecast") or 0.0
    st = TrackState(aid, name, direction, label,
                    det_ts, det_px, message_id=message_id, calls=calls, exh=float(exh),
                    is_call=_calls_make_call(calls), text0=body,
                    link=trade_link_md(name) if message_id else "",
                    forecast=float(fc30))
    for band in BANDS_ORDER:
        d = (calls.get(band) or {}).get("dir")
        if d: st.band_dirs[band] = d
    register_track(st)
    return aid

async def _alert_worker():
    while not _shutdown.is_set():
        item = None
        try:
            item = await asyncio.wait_for(_alert_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
        if item is None: continue
        s, label, direction, pct, now = item
        try:
            await _fire_alert_impl(s, label, direction, pct, now)
        except Exception as e:
            log.error("alert worker %s: %s", s.sym, e)
        finally:
            _alert_queue.task_done()

async def fire_alert(s: Sym, label: str, direction: str, pct: float, now: float):
    global _alert_queue_hwm
    key = f"{s.sym}|{label}|{direction}"
    _last_candidate_ts[key] = now_ts()
    q = _alert_queue
    if q.qsize() >= ALERT_QUEUE_MAX:
        try:
            dropped = q.get_nowait()
            q.task_done()
            _stats["alert_queue_drops"] += 1
            try:
                ds, dl, dd, _dp, _dn = dropped
                log.warning("alert queue full — dropped %s %s %s (signal lost)",
                            ds.sym, dl, dd)
            except Exception:
                pass
        except Exception:
            pass
    try:
        q.put_nowait((s, label, direction, pct, now))
        _alert_queue_hwm = max(_alert_queue_hwm, q.qsize())
    except asyncio.QueueFull:
        _stats["alert_queue_drops"] += 1

async def _fire_alert_impl(s: Sym, label: str, direction: str, pct: float, now: float):
    global _last_fire_alert_error
    name = s.sym
    key = f"{name}|{label}|{direction}"
    if key in _pending_alert_keys: return
    rb = _delivery_retry_after.get(key)
    if rb and now < rb: return
    _pending_alert_keys.add(key)
    hold_key = False
    try:
        now = now_ts()
        # v5.1.1: refuse to fire from a stale feed.
        if not fresh_price(s, now):
            return
        det_px = s.px; det_ts = now
        await seed_volume_bars(s)
        raw = build_raw(s, now)
        raw["btc_corr"] = rolling_btc_corr(s, now)
        raw["_sigma"] = m5_sigma(s, now)
        raw["spread"] = spread_pct(s)
        raw.update(await fetch_real_volume(s))
        raw["_cat"] = "pump" if direction == "up" else "dump"
        liq = raw.get("liq_15m") or 0.0
        liq_thr = max(LIQUIDATION_MIN_USD, (s.qv24 or 0.0) * LIQUIDATION_PCT_OF_VOL)
        exh, ext_dir, dec, ld, exh_reasons = exhaustion_score(s, now, direction, pct, raw)
        raw.update({"extension_pct": ext_dir, "leg_decay": dec, "liq_decel": ld,
                    "climax_bar": 1.0 if climax_bar(s, direction) else 0.0,
                    "vol_sigma": raw.get("_sigma"), "exh_score": float(exh)})
        chase, chase_reasons, thin_vol = chase_score(s, now, direction, pct, raw, label, exh)
        if thin_vol: _stats["thin_volume_caps"] += 1
        if exh >= EXH_TAG_MIN: _stats["exh_tagged"] += 1
        cat = raw["_cat"]
        fv = build_feature_vector(pct, label, raw, liq, liq_thr, False, direction, now)

        aligned = breakout_dir(s, now, BREAKOUT_WINDOW[label]) == direction
        score = compute_signal_score(pct, label, raw, liq, aligned, direction)
        archetype = classify_archetype(raw)
        mom_pts, mom_reasons = momentum_bonus(s, direction, now, raw)

        ext1_signed = ext_dir
        if direction == "down" and ext1_signed is not None:
            ext1_signed = abs(ext1_signed) * -1.0
        ext24 = ext_move_pct(s, now, 86400)
        ext24_signed = ext24 * (1.0 if direction == "up" else -1.0) if ext24 is not None else None
        parabolic = _parabolic_active(s, now, ext1_signed, ext24_signed)
        if parabolic:
            mom_pts = 0.0

        whale_now, whale_usd, whale_side = whale_check(name, raw.get("qv_1m_usd"), raw.get("qv_1m_buy"))
        whale = (label == "5m" and abs(pct) >= SCALP_VOL_MOVE_PCT and whale_now
                 and now - _whale_badge_ts.get(name, 0.0) >= SCALP_VOL_COOLDOWN)
        if whale:
            _whale_badge_ts[name] = now
            _stats["whale_alerts"] += 1
        pen = chase_penalty_frac(chase)
        if pen > 0:
            _stats["chase_discounted"] += 1
            _stats["chase_discount_pts"] += int(pen * 100)
        opp_dir = "down" if direction == "up" else "up"

        calls = {}
        for band, bname, *_ in BANDS:
            p_hist, _eff = await asyncio.to_thread(
                history_probability, cat, label, score, archetype, band)
            fc = await asyncio.to_thread(forecast_for, band, cat, label)
            p_rf = p_hist
            if mom_pts > 0:
                band_mom = mom_pts * BAND_MOM_SCALE.get(label, 0.5)
                p_rf = min(0.95, (p_rf or 0.0) + band_mom / 100.0)
            else:
                band_mom = 0.0
            vol_boost = False
            if whale and band == "now":
                p_rf = min(0.95, (p_rf or 0.0) + SCALP_VOL_BOOST)
                vol_boost = True
            p_final = p_rf * (1.0 - pen) if p_rf is not None else None
            if p_final is not None:
                p_final = min(p_final, P_DISPLAY_CAP)
                if parabolic and p_final > PARABOLIC_P_CAP:
                    p_final = PARABOLIC_P_CAP
                    _stats["parabolic_caps"] += 1
            strong_floor_ok = p_hist is not None and p_hist >= STRONG_HIST_FLOOR
            disp = _decision_word(p_final, direction, strong_floor_ok)
            calls[band] = {"p_hist": p_hist, "pre_chase": p_rf,
                           "p_final": p_final, "alpha": 0.0,
                           "display_call": disp, "dir": direction, "div": False,
                           "chase": chase, "forecast": fc,
                           "momentum_pts": round(band_mom, 1), "vol_boost": vol_boost,
                           "actionable": bool(p_final is not None and p_final >= CALL_ACTIONABLE)}

        for band, bname, *_ in BANDS:
            c = calls[band]
            if c["p_final"] is None: continue
            if band == "now" and exh < DIV_SCALP_MIN_EXH: continue
            div_band = band + "div"
            p_dh, w_dh = await asyncio.to_thread(
                history_probability, cat, label, score, archetype, div_band)
            p_div = None; src_cap = DIV_HEUR_CAP
            if w_dh < DIV_MIN_WEIGHT:
                continue
            if p_dh is not None:
                p_div = p_dh
                src_cap = DIV_CAP_SMALL if w_dh < 30 else DIV_CAP_MATURE
            elif exh >= 45:
                p_div = min(DIV_HEUR_CAP, max(0.30, DIV_HEUR_BASE + (exh - 40) * DIV_HEUR_SLOPE))
            else:
                continue
            p_div = min(p_div, src_cap, P_DISPLAY_CAP)
            if parabolic and p_div > PARABOLIC_DIV_CAP:
                p_div = PARABOLIC_DIV_CAP
                _stats["parabolic_caps"] += 1
            if p_div > c["p_final"] and p_div >= CALL_LEAN:
                opp_word = _decision_word(p_div, opp_dir, True)
                if opp_word.startswith("STRONG") or opp_word in ("LONG", "SHORT"):
                    pass
                else:
                    opp_word = f"LEAN {('LONG' if opp_dir=='up' else 'SHORT')}"
                dfc = await asyncio.to_thread(forecast_for, div_band, cat, label)
                if dfc is None: dfc = c.get("forecast")
                calls[band] = {"p_hist": p_dh, "pre_chase": None,
                               "p_final": p_div, "alpha": 0.0,
                               "display_call": opp_word, "dir": opp_dir, "div": True,
                               "cont_p": c["p_final"],
                               "chase": chase, "forecast": dfc,
                               "momentum_pts": 0.0, "vol_boost": False,
                               "actionable": bool(p_div >= CALL_ACTIONABLE)}
                _stats["div_rows"] += 1

        # v5.2.0: the push decision uses the best call across ALL bands. The 30m
        # row stays the headline of the prediction line, but a thin 30m prior
        # (UNCLEAR/AVOID) no longer suppresses an alert another band called.
        c30 = calls["30m"]
        best_band = "30m"
        best_dc = c30["display_call"] or "UNCLEAR"
        best_rank = TIER_RANK.get(best_dc, 0)
        best_p = c30["p_final"]
        best_raw = c30["pre_chase"]
        for band in BANDS_ORDER:
            if band == best_band: continue
            c = calls[band]
            dc = c.get("display_call") or "UNCLEAR"
            r = TIER_RANK.get(dc, 0)
            if r > best_rank or (r == best_rank and (c.get("p_final") or 0.0) > (best_p or 0.0)):
                best_band, best_dc, best_rank = band, dc, r
                best_p = c.get("p_final")
                best_raw = c.get("pre_chase")
        max_any_p = max((c["p_final"] for c in calls.values() if c["p_final"] is not None), default=None)

        best_is_real_call = best_dc not in ("UNCLEAR", "AVOID", "NO EDGE", "—")
        if not best_is_real_call:
            _stats["unclear_suppressed"] += 1
            if _track_budget_ok(False):
                await _store_and_track(s, name, cat, direction, label, score, det_px, det_ts,
                                       fv, archetype, calls, raw.get("low_vol", 0),
                                       f"*{name}*", exh, None)
            _last_candidate_ts[key] = now
            return

        if parabolic and chase >= PARABOLIC_CHASE_SUPPRESS:
            _stats["parabolic_suppressed"] += 1
            await log_shadow(s, label, direction, det_px, now, "parabolic_chase")
            _last_candidate_ts[key] = now
            return

        if max_any_p is None or max_any_p < NOTIFY_MIN_P:
            _stats["alerts_suppressed"] += 1
            if _track_budget_ok(False):
                await _store_and_track(s, name, cat, direction, label, score, det_px, det_ts,
                                       fv, archetype, calls, raw.get("low_vol", 0),
                                       f"*{name}*", exh, None)
            _last_candidate_ts[key] = now
            return

        body = render_alert(s, label, direction, pct, now, raw, calls,
                            exh, chase, thin_vol, whale, whale_usd, whale_side,
                            parabolic=parabolic)
        link = trade_link_md(name)

        if GOVERNOR_ENABLED and not whale:
            gov_gap = _last_sym_push_ts.get(name, 0.0)
            in_gap = (now - gov_gap) < GOV_PUSH_GAP_SEC
            is_flip_call = bool((calls.get(best_band) or {}).get("div"))
            bypass = best_rank >= GOV_GAP_BYPASS_RANK
            flip_pass = is_flip_call and (best_p or 0.0) >= GOV_FLIP_MIN_P
            snap = _last_sym_snapshot.get(name)
            same_snap = bool(snap and (now - snap[0]) < GOV_SNAPSHOT_DEDUP_SEC
                             and abs(snap[1] - det_px) / max(det_px, 1e-12) < 1e-6
                             and abs(snap[2] - s.oi_usd) < 1.0
                             and snap[3] != label and snap[4] == direction)
            if same_snap:
                _stats["gov_deduped"] += 1
                if _track_budget_ok(False):
                    await _store_and_track(s, name, cat, direction, label, score, det_px, det_ts,
                                           fv, archetype, calls, raw.get("low_vol", 0), body,
                                           exh, None)
                _last_candidate_ts[key] = now
                return
            if in_gap and not bypass and not flip_pass:
                _stats["gov_muted"] += 1
                if _track_budget_ok(False):
                    await _store_and_track(s, name, cat, direction, label, score, det_px, det_ts,
                                           fv, archetype, calls, raw.get("low_vol", 0), body,
                                           exh, None)
                _last_candidate_ts[key] = now
                return
            if in_gap:
                _stats["gov_bypassed"] += 1

        wash_note = ""
        if WASH_ENABLED and label == "5m" and direction == "up":
            mc = _market_cap.get(base_of(name))
            if mc is not None and mc <= LOWCAP_CAP_MAX:
                suspicious, wash_note = await wash_check(s)
                if suspicious:
                    _stats["wash_flags"] += 1
                    await log_shadow(s, label, direction, det_px, now, "wash_suspect")

        _expire_delivered_p(now)
        rk = f"{name}|{direction}"
        entry = _last_delivered_p.get(rk)
        is_div_call = bool((calls.get(best_band) or {}).get("div"))
        if entry is not None and best_rank >= 3 and best_p is not None and not is_div_call:
            last_p, last_t, last_rank = entry
            if best_rank <= last_rank and best_p - last_p < REALERT_MIN_DELTA:
                _stats["alerts_realert_blocked"] += 1
                _cooldown_block_log[name] += 1
                _cooldown_block_distinct.add(name)
                if _track_budget_ok(False):
                    await _store_and_track(s, name, cat, direction, label, score, det_px, det_ts,
                                           fv, archetype, calls, raw.get("low_vol", 0), body,
                                           exh, None)
                _last_candidate_ts[key] = now
                return

        flip_note = ""
        opp = "down" if direction == "up" else "up"
        opp_entry = _last_push_p.get(f"{name}|{opp}")
        is_flip = bool(opp_entry and now - opp_entry[0] <= FLIP_FRESH_SEC)
        if is_flip and best_p is not None and best_p < FLIP_MIN_P:
            _stats["flip_blocks"] += 1
            if _track_budget_ok(False):
                await _store_and_track(s, name, cat, direction, label, score, det_px, det_ts,
                                       fv, archetype, calls, raw.get("low_vol", 0), body,
                                       exh, None)
            _last_candidate_ts[key] = now
            return
        if is_flip:
            hhmm = datetime.fromtimestamp(opp_entry[0], tz=VIENNA_TZ).strftime("%H:%M")
            flip_note = f"↩ flips {('LONG' if direction=='up' else 'SHORT')} {hhmm} — bar raised (P≥65)"

        lp = _last_push_p.get(rk)
        if lp and now - lp[0] <= ESCALATION_WINDOW_SEC and lp[2] != label:
            if best_raw is not None and best_raw < lp[1] + ESCALATION_MIN_GAIN:
                _stats["escalation_blocks"] += 1
                if _track_budget_ok(False):
                    await _store_and_track(s, name, cat, direction, label, score, det_px, det_ts,
                                           fv, archetype, calls, raw.get("low_vol", 0), body,
                                           exh, None)
                _last_candidate_ts[key] = now
                return

        if not _chop_ok(name, now):
            _stats["chop_blocks"] += 1
            if _track_budget_ok(False):
                await _store_and_track(s, name, cat, direction, label, score, det_px, det_ts,
                                       fv, archetype, calls, raw.get("low_vol", 0), body,
                                       exh, None)
            _last_candidate_ts[key] = now
            return

        if _burst_should_collapse(direction, label, now):
            _stats["alerts_burst_collapsed"] += 1
            wk = f"{name}|{direction}|{label}"
            prev = _wave_pending.get(wk)
            if prev is None or (max_any_p or 0) > prev[5]:
                _wave_pending[wk] = (now, direction, label, pct, best_dc, max_any_p or 0.0)
            if _track_budget_ok(False):
                await _store_and_track(s, name, cat, direction, label, score, det_px, det_ts,
                                       fv, archetype, calls, raw.get("low_vol", 0), body,
                                       exh, None)
            _last_candidate_ts[key] = now
            return

        if not whale and best_dc.startswith("LEAN"):
            # v5.2.0: the 45s confirm-hold runs in its own task so it cannot park
            # an alert worker while other signals queue up.
            hold_key = True
            spawn(_confirm_and_deliver({
                "key": key, "s": s, "name": name, "cat": cat, "direction": direction,
                "label": label, "score": score, "det_px": det_px, "det_ts": det_ts,
                "fv": fv, "archetype": archetype, "calls": calls,
                "low_vol": raw.get("low_vol", 0), "body": body, "exh": exh,
                "best_p": best_p, "best_rank": best_rank, "best_dc": best_dc}))
            return

        # Final freshness gate immediately before delivery.
        if not fresh_price(s, now_ts()):
            return
        body = render_alert(s, label, direction, pct, now, raw, calls,
                            exh, chase, thin_vol, whale, whale_usd, whale_side,
                            flip_note=flip_note, parabolic=parabolic, wash_note=wash_note)
        full = body + "\n" + link
        mid = await send_msg(full)
        if mid is None:
            _stats["alert_delivery_failures"] += 1
            _delivery_retry_after[key] = now_ts() + 300
            return
        _stats["alerts_sent"] += 1
        await _store_and_track(s, name, cat, direction, label, score, det_px, det_ts,
                               fv, archetype, calls, raw.get("low_vol", 0), body,
                               exh, mid, pushed=True, arm_gate=True, cd_key=key,
                               gate_p=best_p, gate_rank=best_rank, gate_raw=best_raw)
    except Exception as e:
        _stats["fire_alert_errors"] += 1
        _last_fire_alert_error = f"{type(e).__name__}: {e}"[:300]
        log.exception("fire_alert %s failed", name)
    finally:
        if not hold_key:
            _pending_alert_keys.discard(key)

# ---------------------------------------------------------------- batchers
async def _confirm_and_deliver(ctx: dict):
    # v5.2.0: LEAN confirm-hold + batching off the worker path.
    key = ctx["key"]
    try:
        intact = await _confirm_hold(ctx["name"], ctx["direction"], ctx["det_px"])
        now = now_ts()
        if not intact:
            if _track_budget_ok(False):
                await _store_and_track(ctx["s"], ctx["name"], ctx["cat"], ctx["direction"],
                                       ctx["label"], ctx["score"], ctx["det_px"], ctx["det_ts"],
                                       ctx["fv"], ctx["archetype"], ctx["calls"],
                                       ctx["low_vol"], ctx["body"], ctx["exh"], None)
            _last_candidate_ts[key] = now
            return
        _stats["alerts_batched"] += 1
        prev = _lean_queue.get(ctx["name"])
        if prev is None or (ctx["best_p"] or 0.0) > prev[2]:
            _lean_queue[ctx["name"]] = (now_ts(), ctx["direction"], ctx["best_p"] or 0.0,
                                        ctx["label"], ctx["best_dc"])
        if _track_budget_ok(False):
            await _store_and_track(ctx["s"], ctx["name"], ctx["cat"], ctx["direction"],
                                   ctx["label"], ctx["score"], ctx["det_px"], ctx["det_ts"],
                                   ctx["fv"], ctx["archetype"], ctx["calls"],
                                   ctx["low_vol"], ctx["body"], ctx["exh"], None,
                                   arm_gate=True, gate_p=ctx["best_p"], gate_rank=ctx["best_rank"])
        _last_candidate_ts[key] = now
    except Exception as e:
        log.error("lean confirm %s: %s", ctx.get("name"), e)
    finally:
        _pending_alert_keys.discard(key)


async def lean_batch_worker():
    while not _shutdown.is_set():
        await _shutdown_or_sleep(60)
        if _shutdown.is_set(): continue
        now = now_ts()
        if _lean_queue:
            oldest = min(t for t, _d, _p, _l, _c in _lean_queue.values())
            if now - oldest >= LEAN_BATCH_SEC or len(_lean_queue) >= 12:
                items = sorted(_lean_queue.items(), key=lambda kv: -kv[1][2])[:12]
                _lean_queue.clear()
                lines = [f"📋 LEAN watch — {len(items)} candidates"]
                for name, (_ts, direction, p, label, dc) in items:
                    d_icon = _verdict_icon(dc)
                    lines.append(f"{d_icon} {name} P{p*100:.0f}% · {label} · {trade_link_md(name)}")
                await send_msg("\n".join(lines))
                await asyncio.sleep(1.05)

async def wave_digest_worker():
    while not _shutdown.is_set():
        await _shutdown_or_sleep(20)
        if _shutdown.is_set() or not _wave_pending: continue
        now = now_ts()
        oldest = min(v[0] for v in _wave_pending.values())
        if now - oldest < WAVE_DIGEST_FLUSH_SEC and len(_wave_pending) < WAVE_DIGEST_MAX_ITEMS:
            continue
        items = list(_wave_pending.values())
        _wave_pending.clear()
        groups: dict[tuple, list] = {}
        for _ts, d, lbl, pct, dc, p in items:
            groups.setdefault((d, lbl), []).append((pct, dc, p))
        lines = [f"🌊 Wave digest — {len(items)} correlated candidates"]
        for (d, lbl), arr in sorted(groups.items()):
            arr.sort(key=lambda x: -x[2])
            seg = " · ".join(f"{p*100:.0f}%" for _pct, _dc, p in arr[:10])
            more = f" +{len(arr)-10} more" if len(arr) > 10 else ""
            arrow = "🟢" if d == "up" else "🔴"
            lines.append(f"{arrow} {lbl} {d.upper()} · {len(arr)} coins: {seg}{more}")
        await send_msg("\n".join(lines))

# ---------------------------------------------------------------- 2h report
_report_prev: dict[str, int] = {}

def _two_hour_report_rows(cut: float):
    c = _db(); c.row_factory = sqlite3.Row
    sent = c.execute("""SELECT COUNT(*) n FROM alerts
        WHERE delivered_ts>=? AND telegram_suppressed=0 AND source LIKE 'live_nexus%'""",
        (cut,)).fetchone()["n"]
    silent = c.execute("""SELECT COUNT(*) n FROM alerts
        WHERE delivered_ts>=? AND telegram_suppressed=1 AND source LIKE 'live_nexus%'""",
        (cut,)).fetchone()["n"]
    claimed: dict[str, list] = {}
    for r in c.execute("""SELECT ap.band b, ap.p_final p FROM alert_predictions ap
        JOIN alerts a ON a.id=ap.alert_id
        WHERE a.delivered_ts>=? AND a.telegram_suppressed=0
          AND a.source LIKE 'live_nexus%' AND ap.p_final IS NOT NULL""", (cut,)):
        d = claimed.setdefault(r["b"], [0.0, 0]); d[0] += r["p"]; d[1] += 1
    realized: dict[str, list] = {}
    fade_realized: dict[str, list] = {}
    right = [0, 0]
    cohort = {"fresh": [0, 0], "late": [0, 0]}
    mom_b = {"0-3": [0, 0], "4-7": [0, 0], "8+": [0, 0]}
    rows2 = c.execute("""SELECT CASE WHEN bo.band LIKE '%div'
                        THEN substr(bo.band, 1, length(bo.band)-3) ELSE bo.band END b,
               bo.band bb, bo.hit h, bo.final_market_move_pct fm, a.calls cj
            FROM band_outcomes bo JOIN alerts a ON a.id=bo.alert_id
            WHERE bo.resolved_ts>=? AND bo.hit IS NOT NULL AND bo.band<>'rev30'
              AND a.source LIKE 'live_nexus%'""", (cut,)).fetchall()
    c.close()
    for r in rows2:
        b = r["b"]; is_div = str(r["bb"]).endswith("div")
        d = (fade_realized if is_div else realized).setdefault(b, [0, 0])
        d[0] += int(r["h"] or 0); d[1] += 1
        if b == "now" and not is_div:
            right[1] += 1
            if r["h"] == 1 or (r["fm"] is not None and r["fm"] > 0): right[0] += 1
            try: cj = json.loads(r["cj"] or "{}")
            except Exception: cj = {}
            c30 = cj.get("now", {})
            ch = c30.get("chase")
            if ch is not None:
                k = "late" if ch >= 50 else "fresh"
                cohort[k][0] += int(r["h"] or 0); cohort[k][1] += 1
            mp = c30.get("momentum_pts") or 0
            mk = "8+" if mp >= 8 else "4-7" if mp >= 4 else "0-3"
            mom_b[mk][0] += int(r["h"] or 0); mom_b[mk][1] += 1
    return int(sent), int(silent), claimed, realized, right, cohort, mom_b, fade_realized

async def two_hour_report_worker():
    await _shutdown_or_sleep(600)
    while not _shutdown.is_set():
        nowv = datetime.now(VIENNA_TZ)
        nxt = nowv.replace(minute=0, second=0, microsecond=0) + timedelta(hours=2 - nowv.hour % 2)
        await _shutdown_or_sleep(max(60.0, (nxt - nowv).total_seconds()))
        if _shutdown.is_set(): return
        try:
            end = now_ts(); start = end - REPORT_INTERVAL_SEC
            sent, silent, claimed, realized, right, cohort, mom_b, fade_realized = \
                await asyncio.to_thread(_two_hour_report_rows, start)
            t0 = datetime.fromtimestamp(start, tz=VIENNA_TZ)
            t1 = datetime.fromtimestamp(end, tz=VIENNA_TZ)
            lines = [f"⏱ NEXUS v5 · {t0:%H:%M}–{t1:%H:%M}", ""]
            lines.append("🎯 *Hit rates* (realized / claimed)")
            lines.append("```")
            for band, bname, *_ in BANDS:
                d = realized.get(band, [0, 0, 0.0, 0]); cl = claimed.get(band, [0.0, 0])
                n = d[1]
                r_pct = (d[0] / n * 100.0) if n else 0.0
                c_pct = (cl[0] / cl[1] * 100.0) if cl[1] else 0.0
                gap = r_pct - c_pct
                mark = "▲" if gap >= 3 else "▼" if gap <= -3 else "≈"
                lines.append(f"{bname:<5} {d[0]:>3}/{n:<4} {r_pct:>4.0f}%  claim {c_pct:>3.0f}%  {mark}{abs(gap):.0f}")
            lines.append("```")
            fade_parts = []
            for band, bname, *_ in BANDS:
                d = fade_realized.get(band)
                if d and d[1]:
                    fade_parts.append(f"{bname} {d[0]}/{d[1]} ({d[0]/d[1]*100:.0f}%)")
            if fade_parts:
                lines.append("fade outcomes: " + " · ".join(fade_parts))
            segs = []
            if right[1]:
                segs.append(f"NOW right-side {right[0]}/{right[1]} ({right[0]/right[1]*100:.0f}%)")
            if cohort["fresh"][1] or cohort["late"][1]:
                fr, fn = cohort["fresh"]; lt, ln = cohort["late"]
                cp = []
                if fn: cp.append(f"fresh {fr/fn*100:.0f}% ({fn})")
                if ln: cp.append(f"late {lt/ln*100:.0f}% ({ln})")
                segs.append("chase: " + " · ".join(cp))
            if mom_b["8+"][1] or mom_b["0-3"][1]:
                mp = []
                for mk in ("0-3", "4-7", "8+"):
                    h, n = mom_b[mk]
                    if n: mp.append(f"{mk} {h/n*100:.0f}%")
                segs.append("momentum: " + " · ".join(mp))
            if _stats["chase_discounted"]:
                segs.append(f"chase-discount {int((_stats['chase_discount_pts'] / _stats['chase_discounted']))}pts avg ({_stats['chase_discounted']})")
            if segs:
                lines.append("")
                lines.append("🔍 " + "  ·  ".join(segs))
            keys = ("trigger_candidates", "trigger_cooldown_blocks", "trigger_pump_blocks",
                    "trigger_btc_blocks", "alerts_realert_blocked", "flip_blocks",
                    "chop_blocks", "escalation_blocks",
                    "thin_volume_caps", "alert_queue_drops", "telegram_429", "whale_alerts",
                    "div_rows", "gov_muted", "gov_bypassed", "gov_deduped",
                    "parabolic_caps", "parabolic_suppressed", "wash_flags",
                    "unclear_suppressed", "tracking_incomplete")
            deltas = []
            for k in keys:
                d = _stats[k] - _report_prev.get(k, 0)
                if d: deltas.append(f"{k.replace('trigger_','').replace('alerts_','').replace('_blocks','').replace('_',' ')} {d}")
            _report_prev.update({k: _stats[k] for k in keys})
            if deltas:
                lines.append("")
                lines.append("🚦 Δ2h: " + " · ".join(deltas))
            lines.append("")
            lines.append(f"💬 q {_alert_queue.qsize()}/{_alert_queue_hwm} · RSS {rss_mb():.0f}MB · "
                         f"{len(SYMS)} syms · {len(_active_tracks)} tracks · priors-only")
            await send_msg("\n".join(lines))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("2h report: %s", e)

# ---------------------------------------------------------------- scanner
_last_alerted: dict[str, float] = {}
_mass_suppress_until = 0.0
_mass_pump_until = 0.0

async def scanner_worker():
    log.info("Scanner warming up 90s…")
    await _shutdown_or_sleep(90)
    while not _shutdown.is_set():
        t0 = now_ts()
        try:
            for s in list(SYMS.values()):
                now = now_ts()
                if not fresh_price(s, now) or (s.qv24 or 0) < VOLUME_GATE: continue
                up_move = (s.px / s.leg_up_px - 1.0) * 100.0 if s.leg_up_px > 0 else 0.0
                dn_move = (1.0 - s.px / s.leg_dn_px) * 100.0 if s.leg_dn_px > 0 else 0.0
                if s.leg_up_px <= 0 or up_move < 0.3:
                    s.leg_up_ts = now; s.leg_up_px = s.px
                if s.leg_dn_px <= 0 or dn_move < 0.3:
                    s.leg_dn_ts = now; s.leg_dn_px = s.px
                p5 = pct_change(s, now, 300)
                if p5 is None: continue
                moves = {"5m": p5, "15m": pct_change(s, now, 900),
                         "1h": pct_change(s, now, 3600), "4h": pct_change(s, now, 14400)}
                for win, thr, label, cd in TIMEFRAME_RULES:
                    p = moves.get(label)
                    if p is None: continue
                    if abs(p) < thr - FLEXIBILITY_PCT: continue
                    _stats["trigger_candidates"] += 1
                    direction = "up" if p > 0 else "down"
                    cd_key = f"{s.sym}|{label}|{direction}"
                    if now - _last_alerted.get(cd_key, 0.0) < cooldown_for(s, label, cd):
                        _stats["trigger_cooldown_blocks"] += 1
                        _cooldown_block_log[s.sym] += 1
                        _cooldown_block_distinct.add(s.sym)
                        continue
                    last_cand = _last_candidate_ts.get(cd_key)
                    if last_cand and now - last_cand < CAND_REENTRY_SEC:
                        _stats["trigger_cooldown_blocks"] += 1
                        continue
                    if direction == "down" and now < _mass_suppress_until:
                        _stats["trigger_mass_blocks"] += 1
                        await log_shadow(s, label, direction, s.px, now, "mass_dump"); continue
                    if direction == "up" and now < _mass_pump_until:
                        _stats["trigger_pump_blocks"] += 1
                        await log_shadow(s, label, direction, s.px, now, "mass_pump"); continue
                    if ENABLE_BTC_CORR and is_btc_driven(s, direction, now, win):
                        _stats["trigger_btc_blocks"] += 1
                        await log_shadow(s, label, direction, s.px, now, "btc_filter"); continue
                    await fire_alert(s, label, direction, p, now)
        except Exception as e:
            log.error("scanner: %s", e)
        await asyncio.sleep(max(0.0, 3.0 - (now_ts() - t0)))

# ---------------------------------------------------------------- websockets
def on_ticker_item(d: dict):
    name = d.get("s") or ""
    if not name.endswith("USDT"): return
    px = to_f(d.get("c")); qv = to_f(d.get("q"))
    if px <= 0: return
    s = get_sym(name, qv)
    if not s: return
    # v5.1.1: use exchange event time with strict validation.
    received = now_ts()
    evt = to_f(d.get("E")) / 1000.0
    if not math.isfinite(px) or not math.isfinite(evt) or evt <= 0:
        return
    if evt > received + 5 or evt <= s.px_ts:
        return
    now = evt
    s.qv24 = qv
    h, l = to_f(d.get("h")), to_f(d.get("l"))
    if h > 0: s.hi24 = h
    if l > 0: s.lo24 = l
    dk = int(now // 86400)
    if dk != s.day_key:
        s.day_key = dk; s.day_open = px; s.day_high = px; s.day_low = px; s.day_peak_pct = 0.0
    else:
        s.day_high = max(s.day_high, px); s.day_low = min(s.day_low, px)
    if s.fine.n:
        li = (s.fine.i - 1) % s.fine.cap
        lt, lp = s.fine.ts[li], s.fine.px[li]
    else:
        lt, lp = 0.0, 0.0
    if s.fine.n == 0 or (now - lt) >= 1.0 or abs(px - lp) / max(lp, 1e-12) >= 0.0002:
        s.fine.push(now, px)
    s.px = px
    s.px_ts = now
    _stats["tickers"] += 1

async def ws_tickers():
    idx, fails = 0, 0
    while not _shutdown.is_set():
        url = f"{WS_BASES[idx]}/ws/!miniTicker@arr"
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_queue=4096) as ws:
                log.info("miniTicker connected")
                fails = 0; _stats["ws_ok"]["ticker"] = True
                async for msg in ws:
                    if _shutdown.is_set(): break
                    _stats["ws_messages"] += 1
                    for item in _loads(msg):
                        try: on_ticker_item(item)
                        except Exception: pass
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _stats["ws_ok"]["ticker"] = False
            fails += 1
            if fails >= 3:
                idx = (idx + 1) % len(WS_BASES); fails = 0
            log.warning("ticker WS down (%s); retry 5s", e)
            await _shutdown_or_sleep(5)

def on_force_order(d: dict):
    o = d.get("o") or {}
    name = o.get("s") or ""
    s = SYMS.get(name)
    if not s: return
    notional = to_f(o.get("q")) * to_f(o.get("p"))
    if notional <= 0: return
    now = now_ts(); bucket = int(now // 10)
    long_usd = notional if o.get("S") == "SELL" else 0.0
    short_usd = notional if o.get("S") == "BUY" else 0.0
    if s.liq is None: s.liq = deque(maxlen=240)
    if s.liq and s.liq[-1][0] == bucket:
        s.liq[-1][1] += long_usd; s.liq[-1][2] += short_usd
    else:
        s.liq.append([bucket, long_usd, short_usd])

async def ws_force_order():
    while not _shutdown.is_set():
        try:
            async with websockets.connect(f"{WS_BASES[0]}/ws/!forceOrder@arr",
                                          ping_interval=20, ping_timeout=20) as ws:
                _stats["ws_ok"]["force"] = True
                async for msg in ws:
                    if _shutdown.is_set(): break
                    try: on_force_order(_loads(msg))
                    except Exception: pass
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _stats["ws_ok"]["force"] = False
            log.warning("forceOrder WS down (%s)", e)
            await _shutdown_or_sleep(5)

async def ws_hot_streams():
    global HOT_VER
    subscribed: set[str] = set()
    my_ver = -1
    sub_id = 0
    while not _shutdown.is_set():
        if not HOT_LIST:
            _stats["ws_ok"]["hot"] = False
            await _shutdown_or_sleep(5); continue
        try:
            async with websockets.connect(f"{WS_BASES[1]}/ws",
                                          ping_interval=20, ping_timeout=20,
                                          max_queue=2048) as ws:
                log.info("hot stream socket connected")
                _stats["ws_ok"]["hot"] = True

                async def _send(method: str, syms: set[str]):
                    nonlocal sub_id
                    if not syms: return
                    sub_id += 1
                    await ws.send(json.dumps({
                        "method": method,
                        "params": [f"{x.lower()}@aggTrade" for x in sorted(syms)] +
                                  [f"{x.lower()}@bookTicker" for x in sorted(syms)],
                        "id": sub_id}))

                want = {x.upper() for x in HOT_LIST}
                await _send("SUBSCRIBE", want)
                subscribed = set(want); my_ver = HOT_VER
                async for msg in ws:
                    if _shutdown.is_set(): break
                    d = _loads(msg)
                    if not isinstance(d, dict): continue
                    if "result" in d and "id" in d: continue
                    if HOT_VER != my_ver:
                        cur = {x.upper() for x in HOT_LIST}
                        await _send("UNSUBSCRIBE", subscribed - cur)
                        await _send("SUBSCRIBE", cur - subscribed)
                        subscribed = cur; my_ver = HOT_VER
                    ev = d.get("e"); name = d.get("s") or ""
                    if ev == "aggTrade":
                        px, q = to_f(d.get("p")), to_f(d.get("q"))
                        if px > 0 and q > 0:
                            n = px * q
                            sg = -n if d.get("m") else n
                            sp = SYMS.get(name)
                            if sp is not None: sp.qv_acc += n
                            dq = TAKER.get(name)
                            if dq is None: dq = TAKER[name] = deque(maxlen=36)
                            b = int(now_ts() // 5)
                            if dq and dq[-1][0] == b:
                                dq[-1][1] += sg; dq[-1][2] += n
                            else:
                                dq.append([b, sg, n])
                            buy = n if sg > 0 else 0.0
                            sell = n if sg < 0 else 0.0
                            wst = _whale_state.get(name)
                            if wst is None:
                                wst = _whale_state[name] = deque(maxlen=7)
                            wb = int(now_ts() // 10)
                            if wst and wst[-1][0] == wb:
                                wst[-1][1] += buy; wst[-1][2] += sell
                            else:
                                wst.append([wb, buy, sell])
                    elif ev == "bookTicker":
                        s = SYMS.get(name)
                        if s:
                            s.bid = to_f(d.get("b")); s.ask = to_f(d.get("a"))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _stats["ws_ok"]["hot"] = False
            subscribed = set()
            log.warning("hot streams WS down (%s); retry 5s", e)
            await _shutdown_or_sleep(5)

async def hotset_worker():
    global HOT_LIST, HOT_VER
    while not _shutdown.is_set():
        try:
            scored = []
            for s in SYMS.values():
                if not fresh_price(s, now_ts()) or (s.qv24 or 0) < VOLUME_GATE: continue
                bars = s.m1.last_bars(2)
                if len(bars) == 2 and bars[0][4] > 0:
                    scored.append((abs(bars[1][4] / bars[0][4] - 1.0), s.sym))
            scored.sort(reverse=True)
            rank = {sym: i for i, (_sc, sym) in enumerate(scored)}
            keep = [x for x in HOT_LIST if rank.get(x, 999) < 60]
            front = [sym for _sc, sym in scored[:24] if sym not in keep]
            now = now_ts()
            extras: set[str] = set()
            for n in ACTIVE_TRACK_SYMS:
                s2 = SYMS.get(n)
                if s2 and (s2.qv24 or 0) >= VOLUME_GATE: extras.add(n)
            for s2 in SYMS.values():
                if s2.last_alert_ts and now - s2.last_alert_ts < 1800 \
                   and (s2.qv24 or 0) >= VOLUME_GATE:
                    extras.add(s2.sym)
            merged: list[str] = []
            for x in front + keep + sorted(extras):
                if x not in merged:
                    merged.append(x)
            merged = merged[:HOT_LIST_CAP]
            if merged != HOT_LIST:
                HOT_LIST = merged; HOT_VER += 1
                live = set(HOT_LIST)
                for k in list(TAKER):
                    if k not in live: TAKER.pop(k, None)
        except Exception as e:
            log.error("hotset: %s", e)
        await _shutdown_or_sleep(30)

# ---------------------------------------------------------------- REST pollers
async def poll_funding():
    while not _shutdown.is_set():
        if not rest_ok():
            await _shutdown_or_sleep(30); continue
        try:
            async with SESSION.get(f"{FAPI}/fapi/v1/premiumIndex",
                                   timeout=aiohttp.ClientTimeout(total=15)) as r:
                rest_guard(r.status)
                if r.status == 200:
                    now = now_ts()
                    for d in await r.json():
                        s = SYMS.get(d.get("symbol") or "")
                        if not s: continue
                        fr = to_f(d.get("lastFundingRate"))
                        mp, ip = to_f(d.get("markPrice")), to_f(d.get("indexPrice"))
                        s.mark, s.indexp = mp, ip
                        if mp > 0 and ip > 0: s.basis = (mp / ip - 1.0)
                        if s.fr_snap_ts == 0: s.fr_snap_ts = now
                        elif now - s.fr_snap_ts >= 3600:
                            s.fr_1h = s.funding; s.fr_snap_ts = now
                        s.funding = fr
        except Exception as e:
            log.error("funding poll: %s", e)
        await _shutdown_or_sleep(FUNDING_POLL_SEC)

async def poll_exchange_info():
    while not _shutdown.is_set():
        if not rest_ok():
            await _shutdown_or_sleep(30); continue
        try:
            async with SESSION.get(f"{FAPI}/fapi/v1/exchangeInfo",
                                   timeout=aiohttp.ClientTimeout(total=20)) as r:
                rest_guard(r.status)
                if r.status == 200:
                    now = now_ts()
                    for d in (await r.json()).get("symbols", []):
                        if d.get("contractType") != "PERPETUAL" or d.get("quoteAsset") != "USDT": continue
                        if d.get("status") != "TRADING": continue
                        name = d.get("symbol")
                        ob = to_f(d.get("onboardDate")) / 1000.0
                        s = SYMS.get(name)
                        if s: s.onboard = ob
                        if ob and now - ob <= NEW_LISTING_MAX_AGE_H * 3600 \
                           and name not in _known_listings and allow_daily("listing", 40):
                            _known_listings.add(name)
                            await send_msg(f"🆕 NEW LISTING {name} on Binance Futures "
                                           f"({(now - ob)/3600:.1f}h ago)")
        except Exception as e:
            log.error("exchangeInfo: %s", e)
        await _shutdown_or_sleep(600)

_known_listings: set[str] = set()

async def poll_oi_ls():
    idx = 0
    while not _shutdown.is_set():
        if not rest_ok():
            await _shutdown_or_sleep(30); continue
        try:
            names = sorted(SYMS, key=lambda n: -(SYMS[n].qv24 or 0))
            names = [n for n in names if (SYMS[n].qv24 or 0) >= VOLUME_GATE]
            if names:
                batch = [names[(idx + i) % len(names)] for i in range(min(20, len(names)))]
                idx = (idx + len(batch)) % max(1, len(names))
                async def _one(name: str):
                    s = SYMS.get(name)
                    if s is None: return
                    now = now_ts()
                    try:
                        async with SESSION.get(f"{FAPI}/futures/data/openInterestHist",
                                               params={"symbol": name, "period": "5m", "limit": 7},
                                               timeout=aiohttp.ClientTimeout(total=10)) as r:
                            rest_guard(r.status)
                            if r.status == 200:
                                arr = await r.json()
                                if isinstance(arr, list) and len(arr) >= 2:
                                    s.oi_usd = to_f(arr[-1].get("sumOpenInterestValue"))
                                    s.oi_ref = to_f(arr[0].get("sumOpenInterestValue"))
                                    s.oi_ref_ts = now
                    except Exception:
                        pass
                    try:
                        async with SESSION.get(f"{FAPI}/futures/data/topLongShortAccountRatio",
                                               params={"symbol": name, "period": "5m", "limit": 1},
                                               timeout=aiohttp.ClientTimeout(total=10)) as r:
                            rest_guard(r.status)
                            if r.status == 200:
                                arr = await r.json()
                                if isinstance(arr, list) and arr:
                                    v = to_f(arr[0].get("longShortRatio"))
                                    s.ls = v if v > 0 else None
                    except Exception:
                        pass
                await bounded_gather([lambda n=n: _one(n) for n in batch], limit=4)
        except Exception as e:
            log.error("oi/ls: %s", e)
        await _shutdown_or_sleep(20)

async def poll_klines_extremes():
    async def _one(name: str):
        s = SYMS.get(name)
        if s is None: return
        try:
            async with SESSION.get(f"{FAPI}/fapi/v1/klines",
                                   params={"symbol": name, "interval": "1d", "limit": KLINES_LIMIT},
                                   timeout=aiohttp.ClientTimeout(total=15)) as r:
                rest_guard(r.status)
                if r.status == 200:
                    kl = await r.json()
                    if isinstance(kl, list) and len(kl) >= 2:
                        s.hi30 = max(to_f(k[2]) for k in kl)
                        s.lo30 = min(to_f(k[3]) for k in kl)
                        prev = kl[-2]
                        s.pdh = to_f(prev[2]); s.pdl = to_f(prev[3])
        except Exception:
            pass
    while not _shutdown.is_set():
        if not rest_ok():
            await _shutdown_or_sleep(60); continue
        try:
            names = [n for n, s in SYMS.items() if (s.qv24 or 0) >= VOLUME_GATE][:300]
            for i in range(0, len(names), 60):
                if _shutdown.is_set() or not rest_ok(): break
                await bounded_gather([lambda n=n: _one(n) for n in names[i:i + 60]], limit=4)
                await asyncio.sleep(0.5)
        except Exception as e:
            log.error("klines: %s", e)
        await _shutdown_or_sleep(KLINES_REFRESH_SEC)

async def poll_cmc():
    while not _shutdown.is_set():
        try:
            if COINMARKETCAP_API_KEY:
                async with SESSION.get(CMC_LISTINGS_URL, params={"limit": 500},
                                       headers={"X-CMC_PRO_API_KEY": COINMARKETCAP_API_KEY},
                                       timeout=aiohttp.ClientTimeout(total=20)) as r:
                    if r.status == 200:
                        for d in (await r.json()).get("data", []):
                            sym = (d.get("symbol") or "").upper()
                            quote_ = (d.get("quote") or {}).get("USD") or {}
                            mc = to_f(quote_.get("market_cap"))
                            if mc > 0: _market_cap[sym] = mc
        except Exception as e:
            log.error("cmc: %s", e)
        await _shutdown_or_sleep(3600)

_market_cap: dict[str, float] = {}

# ---------------------------------------------------------------- corr seeding
async def _klines5m_returns(name: str, limit: int = 290) -> Optional[dict[int, float]]:
    try:
        async with SESSION.get(f"{FAPI}/fapi/v1/klines",
                               params={"symbol": name, "interval": "5m", "limit": limit},
                               timeout=aiohttp.ClientTimeout(total=12)) as r:
            rest_guard(r.status)
            if r.status != 200: return None
            kl = await r.json()
    except Exception:
        return None
    if not isinstance(kl, list) or len(kl) < 30: return None
    out: dict[int, float] = {}; prev = None
    for k in kl:
        t = int(k[0]) // 1000 // 300
        c_ = to_f(k[4])
        if prev and c_ > 0 and prev > 0:
            out[t] = math.log(c_ / prev)
        if c_ > 0: prev = c_
    return out

async def corr_seed_worker():
    await _shutdown_or_sleep(45)
    while not _shutdown.is_set():
        if not rest_ok():
            await _shutdown_or_sleep(30); continue
        try:
            btc = await _klines5m_returns(BTC_SYMBOL)
            if btc:
                names = [n for n, s in sorted(SYMS.items(), key=lambda kv: -(kv[1].qv24 or 0))
                         if (s.qv24 or 0) >= VOLUME_GATE and n != BTC_SYMBOL][:CORR_SEED_SYMBOLS]
                now = now_ts(); warm = 0
                for n in names:
                    if _shutdown.is_set() or not rest_ok(): break
                    r = None
                    try:
                        s = SYMS.get(n)
                        if s is not None:
                            rs = await _klines5m_returns(n)
                            if rs:
                                common = set(rs) & set(btc)
                                if len(common) >= BTC_CORR_MIN_SAMPLES:
                                    ks = sorted(common)
                                    r = _pearson([rs[k] for k in ks], [btc[k] for k in ks])
                            s.corr_cache = (now_ts(), r)
                            if r is not None: warm += 1
                    except Exception:
                        pass
                    await asyncio.sleep(REST_DELAY)
                log.info("corr seed: %d/%d warm", warm, len(names))
        except Exception as e:
            log.error("corr_seed: %s", e)
        await _shutdown_or_sleep(CORR_SEED_REFRESH_SEC)

# ---------------------------------------------------------------- boot bars + fade watcher
async def boot_bars_worker():
    await _shutdown_or_sleep(45)
    while not _shutdown.is_set():
        if not rest_ok():
            await _shutdown_or_sleep(30); continue
        try:
            names = sorted(SYMS, key=lambda n: -(SYMS[n].qv24 or 0))
            names = [n for n in names if (SYMS[n].qv24 or 0) >= VOLUME_GATE][:BOOT_BARS_SYMBOLS]
            filled = 0
            for name in names:
                if _shutdown.is_set(): break
                s = SYMS.get(name)
                if s is None: continue
                try:
                    for interval, ring, lim, isec in (("1m", s.m1, 60, 60), ("5m", s.m5, 50, 300)):
                        last = ring.ts[(ring.i - 1) % ring.cap] if ring.n else 0.0
                        async with SESSION.get(
                                f"{FAPI}/fapi/v1/klines",
                                params={"symbol": name, "interval": interval, "limit": lim},
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
                            rest_guard(r.status)
                            if r.status != 200: continue
                            kl = await r.json()
                        if not isinstance(kl, list): continue
                        for k in kl:
                            # v5.1.1: canonicalise to bar-open + interval.
                            t = int(k[0]) / 1000.0 + isec
                            if t <= last or t <= 0: continue
                            ring.push(t, to_f(k[1]), to_f(k[2]), to_f(k[3]), to_f(k[4]), to_f(k[7]))
                    filled += 1
                except Exception:
                    pass
                await asyncio.sleep(REST_DELAY * 2)
            log.info("boot bars: filled %d/%d symbols", filled, len(names))
        except Exception as e:
            log.error("boot_bars: %s", e)
        await _shutdown_or_sleep(6 * 3600)

# ---------------------------------------------------------------- detectors
_mass_last = {"t": 0.0}
_mass_last_pump = {"t": 0.0}
_btc_anchor = {"day": 0, "base": None, "fired_up": False, "fired_dn": False}

async def detectors_worker():
    global _mass_suppress_until, _mass_pump_until
    while not _shutdown.is_set():
        try:
            now = now_ts()
            btc = SYMS.get(BTC_SYMBOL)
            btc5 = pct_change(btc, now, 300) if btc else None
            dumps = 0; pumps = 0
            for s in SYMS.values():
                if (s.qv24 or 0) < VOLUME_GATE: continue
                p5 = pct_change(s, now, 300)
                if p5 is None: continue
                if p5 <= -2.0: dumps += 1
                elif p5 >= 2.0: pumps += 1
            if dumps >= MASS_DUMP_MIN_COINS and now - _mass_last["t"] > MASS_DUMP_COOLDOWN_SEC:
                _mass_last["t"] = now
                _mass_suppress_until = now + MASS_SUPPRESS_SEC
                await send_msg(f"🌊 MASS DUMP: {dumps} coins −2%+ in 5m · "
                               f"BTC {('%+.2f%%' % btc5) if btc5 is not None else '—'} · "
                               "down-alerts suppressed 30m")
            if pumps >= MASS_DUMP_MIN_COINS and now - _mass_last_pump["t"] > MASS_DUMP_COOLDOWN_SEC:
                _mass_last_pump["t"] = now
                _mass_pump_until = now + MASS_SUPPRESS_SEC
                await send_msg(f"🌊 MASS PUMP: {pumps} coins +2%+ in 5m · "
                               f"BTC {('%+.2f%%' % btc5) if btc5 is not None else '—'} · "
                               "up-alerts digested 30m")
            for s in list(SYMS.values()):
                if (s.qv24 or 0) < VOLUME_GATE or s.px <= 0: continue
                p24 = pct_change(s, now, 86400)
                if p24 is None: continue
                s.day_peak_pct = max(s.day_peak_pct, p24)
                if s.hi30 > 0 and s.px > s.hi30 and allow_daily(f"hi30|{s.sym}", EXTREME_MAX_PER_DAY):
                    s.hi30 = s.px
                    await send_msg(f"🏔 {EXTREMES_LABEL} HIGH {s.sym}: new 30-day high {s.px:.6g}")
                elif s.lo30 > 0 and s.px < s.lo30 and allow_daily(f"lo30|{s.sym}", EXTREME_MAX_PER_DAY):
                    s.lo30 = s.px
                    await send_msg(f"🕳 {EXTREMES_LABEL} LOW {s.sym}: new 30-day low {s.px:.6g}")
                _mn5, mx5, cnt = s.m5.minmax_close(now - 86400, now)
                if p24 >= PEAK_MIN_GAIN and mx5 and cnt > 10:
                    drop = (mx5 - s.px) / mx5 * 100.0
                    if drop >= PEAK_MIN_DROP and allow_daily(f"peak|{s.sym}", 1, PEAK_COOLDOWN):
                        await send_msg(f"📉 PEAK FADE {s.sym}: +{p24:.0f}% day, now −{drop:.0f}% "
                                       f"off 24h peak {mx5:.6g}")
                if s.day_peak_pct >= TOPGAIN_MIN_DAY and p24 <= s.day_peak_pct - TOPGAIN_DROPOFF \
                   and p24 >= TOPGAIN_STILL and allow_daily(f"roll|{s.sym}", 1):
                    await send_msg(f"🎢 LOSING STEAM {s.sym}: was +{s.day_peak_pct:.1f}% today, "
                                   f"now +{p24:.1f}% — the rally is fading")
                base = base_of(s.sym)
                mc = _market_cap.get(base)
                if mc and mc <= LOWCAP_CAP_MAX and (s.qv24 or 0) >= LOWCAP_VOL_MIN:
                    p5 = pct_change(s, now, 300)
                    if p5 is not None and p5 >= LOWCAP_SPIKE_PCT:
                        raw = build_raw(s, now)
                        raw.update(await fetch_real_volume(s))
                        if (raw.get("vol_1m_x") or 0) >= LOWCAP_SURGE_X \
                           and allow_daily(f"lowcap|{s.sym}", 1, LOWCAP_COOLDOWN):
                            await send_msg(f"🐜 LOWCAP SPIKE {s.sym}: {p5:+.1f}%/5m, vol "
                                           f"{_vol_disp(raw.get('vol_1m_x'))} median, mcap ${mc/1e6:.1f}M")
            if BTC_BAND_ENABLED and btc and btc.px > 0:
                dk = int(now // 86400)
                if _btc_anchor["day"] != dk or _btc_anchor["base"] is None:
                    _btc_anchor.update({"day": dk, "base": btc.px,
                                        "fired_up": False, "fired_dn": False})
                else:
                    delta = btc.px - _btc_anchor["base"]
                    if not _btc_anchor["fired_up"] and delta >= BTC_BAND_MOVE_USD:
                        _btc_anchor["fired_up"] = True
                        await send_msg(f"🟠 BTC +${delta:,.0f} today vs open (now {btc.px:,.0f})")
                    elif not _btc_anchor["fired_dn"] and delta <= -BTC_BAND_MOVE_USD:
                        _btc_anchor["fired_dn"] = True
                        await send_msg(f"🟠 BTC −${abs(delta):,.0f} today vs open (now {btc.px:,.0f})")
        except Exception as e:
            log.error("detectors: %s", e)
        await _shutdown_or_sleep(60)

# ---------------------------------------------------------------- maintenance
async def bar_aggregator():
    last_min = int(now_ts() // 60)
    while not _shutdown.is_set():
        await asyncio.sleep(0.5)
        try:
            now = now_ts(); mn = int(now // 60)
            if mn <= last_min: continue
            last_min = mn
            t_bar = mn * 60.0
            for s in SYMS.values():
                hh = ll = close_p = open_p = None
                for k in range(s.fine.n):
                    idx = (s.fine.i - 1 - k) % s.fine.cap
                    t = s.fine.ts[idx]
                    if t < t_bar - 60: break
                    if t < t_bar:
                        v = s.fine.px[idx]
                        if close_p is None: close_p = v
                        open_p = v
                        hh = v if hh is None or v > hh else hh
                        ll = v if ll is None or v < ll else ll
                qv = s.qv_acc; s.qv_acc = 0.0
                if close_p is None:
                    prev = s.m1.close_at_or_before(t_bar - 60) or s.px
                    close_p = open_p = hh = ll = prev
                s.m1.push(t_bar, open_p, hh, ll, close_p, qv)
                if int(t_bar) % 300 == 0:
                    last5 = s.m1.last_bars(5)
                    if last5:
                        s.m5.push(t_bar, last5[0][1], max(b[2] for b in last5),
                                  min(b[3] for b in last5), last5[-1][4], sum(b[5] for b in last5))
        except Exception as e:
            log.error("aggregator: %s", e)

async def memory_governor():
    while not _shutdown.is_set():
        try:
            mb = rss_mb()
            if mb > MEM_SOFT_CAP_MB:
                _stats["memory_pressure_events"] += 1
                gc.collect()
                demoted = 0; now = now_ts()
                for name in list(SYMS):
                    s = SYMS[name]
                    if ((s.qv24 or 0) < SYM_DEMOTE_QV and ACTIVE_TRACK_SYMS.get(name, 0) == 0
                            and name != BTC_SYMBOL):
                        if s.low_since == 0.0: s.low_since = now
                        elif now - s.low_since > 1800:
                            SYMS.pop(name, None); TAKER.pop(name, None)
                            _whale_state.pop(name, None)
                            demoted += 1
                    else:
                        s.low_since = 0.0
                _stats["symbols_demoted"] += demoted
                log.warning("memory pressure: %.0fMB (cap %.0f) — demoted %d symbols",
                            mb, MEM_SOFT_CAP_MB, demoted)
        except Exception as e:
            log.error("memory governor: %s", e)
        await _shutdown_or_sleep(60)

async def janitor_worker():
    while not _shutdown.is_set():
        await _shutdown_or_sleep(900)
        try:
            now = now_ts(); freed = 0
            for k in [k for k, rb in _delivery_retry_after.items() if now >= rb]:
                _delivery_retry_after.pop(k, None); freed += 1
            if len(_known_listings) > 5000:
                _known_listings.clear(); freed += 1
            if len(_cooldown_block_distinct) > 4000:
                _cooldown_block_distinct.clear(); freed += 1
            if len(_cooldown_block_log) > 4000:
                for k, _v in _cooldown_block_log.most_common()[4000:]:
                    _cooldown_block_log.pop(k, None)
                freed += 1
            for name in list(_sym_push_log):
                dq = _sym_push_log[name]
                while dq and now - dq[0] > CHOP_WINDOW_SEC:
                    dq.popleft()
                if not dq:
                    _sym_push_log.pop(name, None); freed += 1
            stale = [n for n, s in SYMS.items()
                     if s.px_ts and now - s.px_ts > 48 * 3600
                     and ACTIVE_TRACK_SYMS.get(n, 0) == 0 and n != BTC_SYMBOL]
            for n in stale:
                SYMS.pop(n, None); TAKER.pop(n, None)
                _whale_state.pop(n, None); _whale_badge_ts.pop(n, None)
                freed += 1
            if freed:
                gc.collect()
                log.info("janitor: swept %d entries · RSS %.0fMB · syms %d",
                         freed, rss_mb(), len(SYMS))
        except Exception as e:
            log.error("janitor: %s", e)
        await _shutdown_or_sleep(900)

async def legacy_migration_worker():
    total = 0
    while not _shutdown.is_set():
        n = await asyncio.to_thread(migrate_legacy_batch, 2000)
        total += n
        if not n:
            log.info("Legacy migration done (%d rows)", total)
            return
        log.info("Legacy migration +%d", n)
        await asyncio.sleep(0.1)

async def snapshot_worker():
    await _shutdown_or_sleep(120)
    while not _shutdown.is_set():
        try:
            now = now_ts()
            data = {"btc_anchor": dict(_btc_anchor),
                    "listings": list(_known_listings)[:800],
                    "last_alerted": dict(sorted(_last_alerted.items(), key=lambda kv: kv[1])[-800:]),
                    "ts": now}
            def _write():
                tmp = STATE_SNAPSHOT_PATH + ".tmp"
                with open(tmp, "w") as f:
                    json.dump(data, f)
                os.replace(tmp, STATE_SNAPSHOT_PATH)
            await asyncio.to_thread(_write)
            def _clean():
                c = _db()
                try:
                    if ALERT_RETENTION_DAYS > 0:
                        cut = now - ALERT_RETENTION_DAYS * 86400
                        c.execute("DELETE FROM band_outcomes WHERE alert_id IN "
                                  "(SELECT id FROM alerts WHERE ts<? AND source NOT LIKE 'backfill%')", (cut,))
                        c.execute("DELETE FROM alert_predictions WHERE alert_id IN "
                                  "(SELECT id FROM alerts WHERE ts<? AND source NOT LIKE 'backfill%')", (cut,))
                        c.execute("DELETE FROM alerts WHERE ts<? AND source NOT LIKE 'backfill%'", (cut,))
                    c.execute("DELETE FROM shadow_candidates WHERE ts < ?", (now - 10 * 86400,))
                    c.commit()
                finally:
                    c.close()
            await asyncio.to_thread(_db_retry, _clean)
            def _checkpoint():
                try:
                    c = _db(); c.execute("PRAGMA wal_checkpoint(TRUNCATE)"); c.close()
                except sqlite3.OperationalError:
                    pass
            await asyncio.to_thread(_checkpoint)
        except Exception as e:
            log.error("snapshot: %s", e)
        await _shutdown_or_sleep(300)

def _load_snapshot():
    try:
        with open(STATE_SNAPSHOT_PATH) as f:
            d = json.load(f)
        a = d.get("btc_anchor") or {}
        if a.get("day") is not None: _btc_anchor.update(a)
        for x in (d.get("listings") or []): _known_listings.add(x)
        for k, v in (d.get("last_alerted") or {}).items(): _last_alerted[k] = float(v)
    except Exception:
        pass

async def prior_refresh_worker():
    global _last_train_done_ts
    await _shutdown_or_sleep(300)
    while not _shutdown.is_set():
        try:
            invalidate_prior_caches()
            _last_train_done_ts = now_ts()
        except Exception as e:
            log.error("prior refresh: %s", e)
        await _shutdown_or_sleep(1800)

async def heartbeat_worker():
    if HEARTBEAT_SILENCE_SEC <= 0: return
    while not _shutdown.is_set():
        await _shutdown_or_sleep(1800)
        if _shutdown.is_set(): return
        if now_ts() - _last_delivered_alert_ts >= HEARTBEAT_SILENCE_SEC:
            ok = await send_msg(
                f"💤 Heartbeat — no alerts delivered in {HEARTBEAT_SILENCE_SEC//3600}h\n"
                f"candidates {_stats['trigger_candidates']} · cooldown-blocks "
                f"{_stats['trigger_cooldown_blocks']} · btc-blocks {_stats['trigger_btc_blocks']} · "
                f"mass-blocks {_stats['trigger_mass_blocks']}\n"
                f"ws {_stats['ws_ok']} · tg-blocked "
                f"{max(0, round(_tg_blocked_until - now_ts()))}s · rss {rss_mb():.0f}MB · "
                f"tracks {len(_active_tracks)} · priors-only {NEXUS_VERSION}")
            if ok:
                _stats["heartbeats_sent"] += 1

async def digest_worker():
    while not _shutdown.is_set():
        try:
            now = datetime.now(VIENNA_TZ)
            nxt = now.replace(hour=DIGEST_HOUR, minute=DIGEST_MIN, second=0, microsecond=0)
            if nxt <= now: nxt += timedelta(days=1)
            await _shutdown_or_sleep((nxt - now).total_seconds())
            if _shutdown.is_set(): return
            def _q():
                c = _db(); cut = time.time() - 86400
                total = c.execute("SELECT COUNT(*) FROM alerts WHERE ts>=?", (cut,)).fetchone()[0]
                c.close(); return total
            total = await asyncio.to_thread(_q)
            out_n = await asyncio.to_thread(db_outcome_count)
            sc, shadow = await asyncio.to_thread(verdict_scorecard, 7)
            brier = await asyncio.to_thread(brier_audit, 7)
            missed = await asyncio.to_thread(missed_opportunity_audit, 7)
            edge = await asyncio.to_thread(edge_audit, 7)
            lines = [f"📊 Nexus {NEXUS_VERSION} daily digest — {total} alerts / 24h", ""]
            lines.append("Verdict scorecard (7d)")
            any_row = False
            for band, bname, *_ in BANDS:
                d = sc[band]; parts = []
                if d["lh"] or d["lm"]: parts.append(f"LONG {d['lh']}/{d['lh']+d['lm']}")
                if d["sh"] or d["sm"]: parts.append(f"SHORT {d['sh']}/{d['sh']+d['sm']}")
                a, m = shadow[band]
                if a or m: parts.append(f"NO-TRADE {a} avoided / {m} missed")
                if parts:
                    any_row = True
                    lines.append(f"{bname}: " + " · ".join(parts))
            if not any_row: lines.append("no verdicts yet")
            if edge["n_del"] or edge["n_sup"]:
                lines.append("")
                lines.append(f"🔬 EDGE (7d): delivered {edge['n_del']} vs suppressed {edge['n_sup']}")
                for band, bname, *_ in BANDS:
                    strat = edge["sel"].get(band) or {}
                    parts = []
                    for pb, pools in sorted(strat.items()):
                        d = pools.get("d"); sup = pools.get("s")
                        if d and sup:
                            parts.append(f"{pb}: {d[1]*100:.0f}% vs {sup[1]*100:.0f}%")
                        elif d:
                            parts.append(f"{pb}: {d[1]*100:.0f}% only")
                    if parts:
                        lines.append(f"  {bname}: " + " · ".join(parts[:4]))
                disc = edge.get("disc") or []
                if len(disc) >= 2:
                    mono = all(disc[i]["hr"] <= disc[i+1]["hr"] + 0.02 for i in range(len(disc)-1))
                    lines.append(f"  P-monotonic: {'✅ yes' if mono else '❌ no'}")
            if _stats["div_rows"]:
                lines.append(f"divergence rows tracked (session): {_stats['div_rows']}")
            if missed["candidates"]:
                lines.append("")
                lines.append("Missed opportunities (7d) — no-call candidates")
                lines.append(f"total {missed['candidates']} declined candidates")
                for band, bname, *_ in BANDS:
                    h, n = missed["hits"][band]
                    if n:
                        lines.append(f"{bname} target hit: {h}/{n} ({h/n*100:.0f}%)")
            if brier:
                lines.append("")
                lines.append("Probability accuracy (7d, p_final) · " +
                             " · ".join(f"{k} {v[0]}({v[1]})" for k, v in brier.items()))
            if _cooldown_block_log:
                top = ", ".join(f"{k}×{v}" for k, v in
                                _cooldown_block_log.most_common(5))
                lines.append("")
                lines.append(f"Cooldown blocks (session) · top: {top}")
            lines.append(f"RSS {rss_mb():.0f}MB · symbols {len(SYMS)} · outcomes {out_n}"
                         f" · exh {_stats['exh_tagged']}"
                         f" · thin {_stats['thin_volume_caps']}"
                         f" · whales {_stats['whale_alerts']}"
                         f" · div {_stats['div_rows']}"
                         f" · gov {_stats['gov_muted']}m/{_stats['gov_bypassed']}b/{_stats['gov_deduped']}d"
                         f" · 🪫 {_stats['parabolic_caps']}c/{_stats['parabolic_suppressed']}s"
                         f" · unclear {_stats['unclear_suppressed']}"
                         f" · incomplete {_stats['tracking_incomplete']}"
                         f" · 🧪 {_stats['wash_flags']}")
            await send_msg("\n".join(lines))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("digest: %s", e)
            await _shutdown_or_sleep(600)

async def stats_worker():
    while not _shutdown.is_set():
        try:
            log.info("msgs=%d tickers=%d alerts=%d sup=%d batch=%d burst=%d rablk=%d "
                     "flip=%d chop=%d whale=%d div=%d tracks=%d q=%d/%d rss=%.0fMB",
                     _stats["ws_messages"], _stats["tickers"], _stats["alerts_sent"],
                     _stats["alerts_suppressed"], _stats["alerts_batched"],
                     _stats["alerts_burst_collapsed"], _stats["alerts_realert_blocked"], _stats["flip_blocks"],
                     _stats["chop_blocks"], _stats["whale_alerts"], _stats["div_rows"],
                     len(_active_tracks),
                     _alert_queue.qsize(), _alert_queue_hwm, rss_mb())
        except Exception:
            pass
        await _shutdown_or_sleep(30)

# ---------------------------------------------------------------- bootstrap
def _parse_kline_csv(raw: bytes):
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        with z.open(z.namelist()[0]) as f:
            for line in io.TextIOWrapper(f, "utf-8"):
                parts = line.strip().split(",")
                if len(parts) < 8 or not parts[0].isdigit(): continue
                yield (int(parts[0]) / 1000.0, to_f(parts[1]), to_f(parts[2]),
                       to_f(parts[3]), to_f(parts[4]), to_f(parts[7]),
                       to_f(parts[10]) if len(parts) > 10 else 0.0)

def _bootstrap_mark_done(name: str, days: list[str], rows: int, inserted: int):
    conn = _db()
    try:
        conn.execute("""INSERT OR REPLACE INTO historical_bootstrap_state
            VALUES(?,?,?,?,?,?,?)""",
            (name, BOOTSTRAP_SOURCE, days[0], days[-1], rows, inserted, time.time()))
        conn.commit()
    finally:
        conn.close()

async def bootstrap_symbol(sem: asyncio.Semaphore, name: str, days: list[str]) -> int:
    async with sem:
        bars: list[tuple] = []
        for day in days:
            if _shutdown.is_set(): break
            url = f"{VISION_BASE}/data/futures/um/daily/klines/{name}/5m/{name}-5m-{day}.zip"
            try:
                async with SESSION.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
                    if r.status != 200: continue
                    bars.extend(_parse_kline_csv(await r.read()))
            except Exception:
                continue
        seen_ts: set[float] = set()
        uniq: list[tuple] = []
        for b in bars:
            if b[0] in seen_ts: continue
            seen_ts.add(b[0]); uniq.append(b)
        bars = sorted(uniq, key=lambda b: b[0])
        rows_done = len(bars)
        if rows_done < 150:
            log.warning("Bootstrap %s incomplete: %d bars — will retry next rescan",
                        name, rows_done)
            return 0
        episodes = []
        for i in range(48, len(bars) - 50):
            c_ = bars[i][4]
            if c_ <= 0: continue
            for wbars, thr, tf in ((1, 3.5, "5m"), (3, 8.0, "15m"),
                                   (12, 15.0, "1h"), (48, 22.0, "4h")):
                base = bars[i - wbars][4]
                if base <= 0: continue
                mv = (c_ / base - 1.0) * 100.0
                if abs(mv) >= thr - FLEXIBILITY_PCT:
                    episodes.append((abs(mv), i, mv, tf))
                    break
        episodes.sort(reverse=True)
        per_day: dict[str, int] = {}
        used: set[int] = set()
        inserted = 0
        for _mag, i, ret, label in episodes:
            day_key = datetime.fromtimestamp(bars[i][0], tz=timezone.utc).strftime("%Y-%m-%d")
            if per_day.get(day_key, 0) >= 3: continue
            if any(abs(i - u) < 12 for u in used): continue
            used.add(i)
            per_day[day_key] = per_day.get(day_key, 0) + 1
            _t, _o, _h, _l, c, qv, tbq = bars[i]
            direction = "up" if ret > 0 else "down"
            detected_ts = bars[i][0] + 300.0
            sign = 1.0 if direction == "up" else -1.0
            bi = max(0, i - 12)
            ext = (c / bars[bi][4] - 1.0) * 100.0 * sign if bars[bi][4] > 0 else 0.0
            prior = [b[5] for b in bars[max(0, i - 12):i] if b[5] > 0]
            med = statistics.median(prior) if len(prior) >= 5 else None
            vol5_x = (qv / med) if (med and qv > 0) else None
            taker = ((2 * tbq - qv) / qv) if qv > 0 else None
            raw = {"vol_1m_x": None, "vol_5m_pace_x": vol5_x, "volume_accel": None,
                   "oi_pct": None, "taker_imb": taker, "taker_1m": None, "fr": None,
                   "fr_delta": None, "basis": None, "btc_corr": None, "spread": None,
                   "quote_vol_24h": None, "low_vol": 0, "shape_15m": None,
                   "shape_1h": None, "shape_4h": None, "liq_15m": None,
                   "extension_pct": ext, "leg_decay": None, "liq_decel": None,
                   "climax_bar": 0.0, "vol_sigma": None, "exh_score": 0.0}
            score = compute_signal_score(ret, label, raw, None, False, direction)
            feat = build_feature_vector(ret, label, raw, None, 1.0, False, direction, detected_ts)
            entry = bars[i + 1][1] if i + 1 < len(bars) else bars[i][4]
            def _work():
                conn = _db()
                try:
                    cur = conn.execute("""INSERT OR IGNORE INTO alerts(symbol,category,direction,
                        timeframe,score,score_final,alert_price,detected_price,ts,detected_ts,
                        delivered_ts,features,feature_schema,archetype,calls,base_message,low_vol,
                        source,cluster_id,outcome_schema,episode_id,telegram_suppressed,
                        notification_version,config_version)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (name, "pump" if direction == "up" else "dump", direction, label, score,
                         score, c, c, detected_ts, detected_ts, detected_ts, _safe_json(feat),
                         FEATURE_SCHEMA_VERSION, "balanced", "{}", "backfill episode", 0,
                         BOOTSTRAP_SOURCE, int(detected_ts // 1800), OUTCOME_SCHEMA_VERSION,
                         _episode_id(name, direction, detected_ts), 1,
                         NOTIFICATION_VERSION, NEXUS_BUILD_ID))
                    if cur.rowcount:
                        aid = int(cur.lastrowid)
                        for band, _bn, d0, d1, tgt in BANDS:
                            j0 = i + 1 + int(d0) // 5
                            j1 = i + 1 + int(d1) // 5
                            covered = j1 <= len(bars)
                            mfe = 0.0; mae = 0.0
                            hit = 0 if covered else None
                            for j in range(j0, min(j1, len(bars))):
                                fav = (((bars[j][2] - entry) if sign > 0
                                        else (entry - bars[j][3])) / entry * 100.0)
                                adv = (((bars[j][3] - entry) if sign > 0
                                        else (entry - bars[j][2])) / entry * 100.0)
                                mfe = max(mfe, fav); mae = min(mae, adv)
                                if mfe >= tgt:
                                    hit = 1
                            if hit == 1:
                                outcome = "target"
                            elif covered:
                                outcome = "timeout"
                            else:
                                outcome = "partial"
                            conn.execute(SQL_BACKFILL_OUTCOME_INSERT,
                                (aid, band, tgt, hit, outcome, mfe, mae, None,
                                 detected_ts + d1 * 60, "backfill",
                                 OUTCOME_SCHEMA_VERSION, entry, 300))
                        return 1
                    return 0
                finally:
                    conn.commit(); conn.close()
            inserted += await asyncio.to_thread(_work)
        await asyncio.to_thread(_bootstrap_mark_done, name, days, rows_done, inserted)
        return inserted

async def _bootstrap_todo() -> list[str]:
    c = _db()
    try:
        done = {r[0] for r in c.execute(
            "SELECT symbol FROM historical_bootstrap_state WHERE version=?",
            (BOOTSTRAP_SOURCE,))}
    finally:
        c.close()
    names = sorted(n for n, s in SYMS.items() if (s.qv24 or 0) >= VOLUME_GATE)
    return [n for n in names if n not in done]

async def _bootstrap_run(todo: list[str]) -> int:
    today = datetime.now(timezone.utc).date()
    days = [(today - timedelta(days=k)).isoformat() for k in range(1, BOOTSTRAP_DAYS + 1)]
    sem = asyncio.Semaphore(BOOTSTRAP_WORKERS)
    inserted_total = 0
    since_refresh = 0
    for i in range(0, len(todo), 12):
        if _shutdown.is_set(): break
        batch = todo[i:i + 12]
        results = await asyncio.gather(*(bootstrap_symbol(sem, n, days) for n in batch),
                                       return_exceptions=True)
        for r in results:
            if isinstance(r, int):
                inserted_total += r
                since_refresh += r
                _stats["backfill_events"] += r
        if since_refresh >= BOOTSTRAP_RETRAIN_EVERY:
            since_refresh = 0
            invalidate_prior_caches()
    return inserted_total

async def bootstrap_worker():
    if not HISTORICAL_BOOTSTRAP:
        return
    await _shutdown_or_sleep(60)
    try:
        last_n = -1; stable = 0
        for _ in range(20):
            n = sum(1 for s in SYMS.values() if (s.qv24 or 0) >= VOLUME_GATE)
            log.info("Bootstrap warmup: %d gate symbols…", n)
            if n >= BOOTSTRAP_WARMUP_MIN_SYMBOLS and n == last_n:
                stable += 1
                if stable >= 2: break
            else:
                stable = 0
            last_n = n
            await _shutdown_or_sleep(30)
        todo = await _bootstrap_todo()
        if not todo:
            log.info("Bootstrap: nothing to do")
        else:
            log.info("Bootstrap: %d symbols × %dd (universe warm: %d)",
                     len(todo), BOOTSTRAP_DAYS, last_n)
            n = await _bootstrap_run(todo[:200])
            log.info("Bootstrap pass: %d events", n)
        invalidate_prior_caches()
        while not _shutdown.is_set():
            await _shutdown_or_sleep(BOOTSTRAP_RESCAN_SEC)
            if _shutdown.is_set(): break
            todo = await _bootstrap_todo()
            if todo:
                log.info("Bootstrap rescan: %d symbols pending", len(todo))
                await _bootstrap_run(todo[:50])
    except Exception as e:
        log.error("bootstrap: %s", e)

# ---------------------------------------------------------------- health
async def health_handler(_request):
    now = now_ts()
    gate = [s for s in SYMS.values() if (s.qv24 or 0) >= VOLUME_GATE]
    corr_warm = sum(1 for s in gate if s.corr_cache[0] and now - s.corr_cache[0] < 7200)
    outcomes = await asyncio.to_thread(db_outcome_count)
    return web.json_response({
        "version": NEXUS_VERSION, "build": NEXUS_BUILD_ID,
        "engine": "priors-only (constitution rule 5)",
        "outcome_schema": OUTCOME_SCHEMA_VERSION,
        "uptime_sec": round(now - _BOOT_TS), "rss_mb": round(rss_mb(), 1),
        "db_size_mb": round(db_size_mb(), 1),
        "mem_soft_cap_mb": MEM_SOFT_CAP_MB,
        "memory_pressure_events": _stats["memory_pressure_events"],
        "symbols_demoted": _stats["symbols_demoted"],
        "symbols": len(SYMS), "hot_symbols": len(HOT_LIST),
        "active_tracks": len(_active_tracks),
        "tracking_errors": _stats["tracking_errors"],
        "tracking_errors_alarm": _stats["tracking_errors"] > 50,
        "tracking_incomplete": _stats["tracking_incomplete"],
        "alerts_sent": _stats["alerts_sent"],
        "fire_alert_errors": _stats["fire_alert_errors"],
        "last_fire_alert_error": _last_fire_alert_error or None,
        "alerts_suppressed": _stats["alerts_suppressed"],
        "alerts_batched": _stats["alerts_batched"],
        "alerts_burst_collapsed": _stats["alerts_burst_collapsed"],
        "alerts_realert_blocked": _stats["alerts_realert_blocked"],
        "unclear_suppressed": _stats["unclear_suppressed"],
        "flip_blocks": _stats["flip_blocks"],
        "chop_blocks": _stats["chop_blocks"],
        "escalation_blocks": _stats["escalation_blocks"],
        "whale_alerts": _stats["whale_alerts"],
        "div_rows": _stats["div_rows"],
        "alerts_confirm_cancelled": _stats["alerts_confirm_cancelled"],
        "thin_volume_caps": _stats["thin_volume_caps"],
        "alert_queue": _alert_queue.qsize(),
        "alert_queue_hwm": _alert_queue_hwm,
        "alert_queue_drops": _stats["alert_queue_drops"],
        "last_alert_age_sec": round(now - _last_delivered_alert_ts),
        "tg_blocked_sec": max(0, round(_tg_blocked_until - now)),
        "replay_queue": len(_replay_queue),
        "telegram_429": _stats["telegram_429"],
        "exh_tagged": _stats["exh_tagged"],
        "trigger_candidates": _stats["trigger_candidates"],
        "trigger_cooldown_blocks": _stats["trigger_cooldown_blocks"],
        "trigger_pump_blocks": _stats["trigger_pump_blocks"],
        "trigger_btc_blocks": _stats["trigger_btc_blocks"],
        "mass_pump_remaining_sec": max(0, round(_mass_pump_until - now)),
        "mass_suppress_remaining_sec": max(0, round(_mass_suppress_until - now)),
        "wave_pending": len(_wave_pending),
        "corr_warm_pct": round(100.0 * corr_warm / len(gate), 1) if gate else 0.0,
        "cluster_weight_mode": CLUSTER_WEIGHT_MODE,
        "prior_cache_gen": _hist_cache_gen,
        "prior_backfill_fallbacks": _stats["prior_backfill_fallbacks"],
        "prior_backfill_weight": SOURCE_WEIGHTS["backfill"],
        "prior_cache_age_sec": round(now - _last_train_done_ts) if _last_train_done_ts else None,
        "outcomes_total": outcomes,
        "rest_blocked": not rest_ok(), "ws": _stats["ws_ok"],
        "config_ok": config_is_valid(),
    })

async def health_server():
    app = web.Application()
    app.router.add_get("/", lambda r: web.json_response({"service": "nexus5", "health": "/health"}))
    app.router.add_get("/health", health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log.info("Health server on :%d", PORT)
    await _shutdown.wait()

# ---------------------------------------------------------------- boot smoke test
def _boot_smoke_test():
    st = TrackState(0, "SMOKEUSDT", "up", "5m", now_ts(), 100.0,
                    entry_ts=now_ts() - 250 * 60, entry_price=100.0, is_call=True)
    for b in BANDS_ORDER:
        st.done.add(b)
        assert st.bands[b] is not None
    st.band_dirs["30m"] = "down"
    assert TRACK_MAX_SAMPLE_GAP_SEC > 0 and TRACK_MAX_SAMPLE_GAP_SEC <= TRACK_FRESH_SEC
    assert SCALP_VOL_FAST_USD == 200_000 and SCALP_VOL_MIN_USD == 500_000
    assert P_DISPLAY_CAP <= 0.95 and DIV_MIN_WEIGHT > 0 and DIV_PRIOR < 0.7
    _cp = chase_penalty_frac
    assert abs(_cp(CHASE_T0)) < 1e-9
    assert abs(_cp(100.0) - 0.30) < 1e-9
    _vals = [_cp(float(x)) for x in range(0, 105, 5)]
    assert all(0.0 <= v <= 0.30 + 1e-9 for v in _vals)
    assert all(a <= b + 1e-12 for a, b in zip(_vals, _vals[1:]))
    log.info("smoke: chase curve OK — f(70)=%.4f", _cp(70.0))
    assert _decision_word(None, "up", True) == "UNCLEAR"
    assert _decision_word(0.75, "up", True) == "STRONG LONG"
    assert _decision_word(0.75, "up", False) == "LONG"
    assert _decision_word(0.64, "down", True) == "SHORT"
    assert _decision_word(0.55, "up", True) == "LEAN LONG"
    assert _decision_word(0.48, "up", True) == "UNCLEAR"
    assert _decision_word(0.30, "up", True) == "AVOID"
    log.info("smoke: decision vocabulary OK (STRONG/LONG/LEAN/UNCLEAR/AVOID)")
    assert TIER_RANK["STRONG LONG"] == 6 and TIER_RANK["UNCLEAR"] == 2 and TIER_RANK["AVOID"] == 1
    assert "/" not in make_trade_link("牛来USDT").split("?")[0].split("futures/")[1]
    assert _risk_tier(2.5) == "LOW" and _risk_tier(1.5) == "MED" \
       and _risk_tier(1.0) == "HIGH" and _risk_tier(0.5) == "EXTREME"
    assert MOMENTUM_MAX_PTS == 4.0
    assert NOTIFY_MIN_P == 0.45
    assert len(BANDS) == 4 and BANDS_NAME["now"] == "NOW"
    assert callable(forecast_for) and callable(risk_for)
    assert _pctl([1.0, 2.0, 3.0, 4.0], 0.50) == 3.0
    # v5.1.1 integrity checks
    assert "WHERE band_outcomes.hit IS NULL" in SQL_OUTCOME_UPSERT
    bl = BandLive()
    assert bl.hit == -1 and bl.coverage_ok is True and bl.samples == 0
    assert fresh_price(None, now_ts()) is False
    log.info("smoke: v5.1.1 outcome-integrity guards present")
    log.info("Boot smoke test passed (v%s)", NEXUS_VERSION)

# ---------------------------------------------------------------- main
async def main():
    global SESSION
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try: loop.add_signal_handler(sig, _shutdown.set)
        except NotImplementedError: pass
    _boot_smoke_test()
    db_init()
    _load_snapshot()
    gc.freeze()
    SESSION = aiohttp.ClientSession()
    log.info("Nexus %s starting · engine=priors-only · mem=%.0fMB · memcap=%.0fMB · workers=%d · "
             "cluster=%s · gov=%s · outcome-integrity=on",
             NEXUS_VERSION, rss_mb(), MEM_SOFT_CAP_MB, ALERT_WORKERS,
             CLUSTER_WEIGHT_MODE, "on" if GOVERNOR_ENABLED else "off")
    tasks = [asyncio.create_task(coro, name=name) for name, coro in {
        "ws_tickers": ws_tickers(), "ws_force": ws_force_order(), "hotset": hotset_worker(),
        "ws_hot": ws_hot_streams(), "aggregator": bar_aggregator(), "scanner": scanner_worker(),
        "detectors": detectors_worker(), "funding": poll_funding(),
        "exinfo": poll_exchange_info(), "oi_ls": poll_oi_ls(),
        "klines": poll_klines_extremes(), "cmc": poll_cmc(), "recovery": recovery_worker(),
        "returns": returns_sweeper(), "shadow": shadow_resolver(),
        "priors": prior_refresh_worker(),
        "legacy": legacy_migration_worker(), "bootstrap": bootstrap_worker(),
        "snapshot": snapshot_worker(), "memgov": memory_governor(), "digest": digest_worker(),
        "heartbeat": heartbeat_worker(), "replay": _replay_worker(),
        "lean_batch": lean_batch_worker(), "wave_digest": wave_digest_worker(),
        "bihour": two_hour_report_worker(),
        "boot_bars": boot_bars_worker(),
        "corr_seed": corr_seed_worker(), "janitor": janitor_worker(),
        "chase_audit": chase_audit_task(),
        "stats": stats_worker(), "health": health_server()}.items()]
    for i in range(ALERT_WORKERS):
        tasks.append(asyncio.create_task(_alert_worker(), name=f"alert_worker_{i}"))
    spawn(deferred_startup_notice(
        f"🟢 Nexus {NEXUS_VERSION} online · priors-only · STRONG/LEAN/UNCLEAR/AVOID vocabulary · "
        f"fade-gated · governor {'on' if GOVERNOR_ENABLED else 'off'} · "
        f"🪫 parabolic guard · outcome-integrity · P≤{P_DISPLAY_CAP:.0%} · {ALERT_WORKERS} workers · 24/7"))
    await _shutdown.wait()
    log.info("Shutting down…")
    for t in tasks: t.cancel()
    for t in _BACKGROUND_TASKS: t.cancel()
    await asyncio.gather(*tasks, *_BACKGROUND_TASKS, return_exceptions=True)
    if SESSION: await SESSION.close()
    log.info("Bye.")

if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
        log.info("uvloop enabled")
    except Exception:
        pass
    asyncio.run(main())
