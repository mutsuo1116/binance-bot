
import asyncio
import csv
import hashlib
import hmac
import math
import os
import sqlite3
import time
import urllib.parse
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import aiohttp
import numpy as np


# ============================================================
# SNIPER PRO V2
# Safer paper-first multi-asset Binance USDⓈ-M Futures bot
#
# IMPORTANT:
# - This version is designed to be run in PAPER first.
# - It does NOT claim or guarantee profitability.
# - LIVE execution is deliberately conservative and should only
#   be enabled after the paper mode has been verified.
# ============================================================

API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
API_SECRET = os.getenv("BINANCE_SECRET_KEY", "").strip()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

LIVE_TRADING = False  # ML V1 is PAPER-ONLY by design
BASE_URL = os.getenv("BINANCE_BASE_URL", "https://fapi.binance.com").rstrip("/")

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "ADAUSDT", "AVAXUSDT", "DOGEUSDT", "LINKUSDT", "NEARUSDT",
    "DOTUSDT", "MATICUSDT", "PEPEUSDT", "INJUSDT", "TIAUSDT"
]

# -------------------- Risk --------------------
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "0.005"))       # 0.5%
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "3"))
MAX_DAILY_DRAWDOWN = float(os.getenv("MAX_DAILY_DRAWDOWN", "0.025"))
MAX_TOTAL_RISK = float(os.getenv("MAX_TOTAL_RISK", "0.015"))      # 1.5% equity
MAX_MARGIN_PER_TRADE = float(os.getenv("MAX_MARGIN_PER_TRADE", "0.08"))
LEVERAGE = int(os.getenv("LEVERAGE", "5"))

# -------------------- Strategy --------------------
MIN_SCORE = int(os.getenv("MIN_SCORE", "7"))
MIN_SCORE_EDGE = int(os.getenv("MIN_SCORE_EDGE", "1"))
MIN_ADX = float(os.getenv("MIN_ADX", "18"))
MIN_ATR_PCT = float(os.getenv("MIN_ATR_PCT", "0.0025"))
MAX_ATR_PCT = float(os.getenv("MAX_ATR_PCT", "0.04"))
MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "0.85"))

ATR_STOP_MULT = float(os.getenv("ATR_STOP_MULT", "1.35"))
MIN_STOP_PCT = float(os.getenv("MIN_STOP_PCT", "0.006"))
MAX_STOP_PCT = float(os.getenv("MAX_STOP_PCT", "0.022"))
REWARD_R = float(os.getenv("REWARD_R", "2.20"))

# Position management
BE_TRIGGER_R = float(os.getenv("BE_TRIGGER_R", "1.0"))
BE_LOCK_PCT = float(os.getenv("BE_LOCK_PCT", "0.0008"))
TRAIL_TRIGGER_R = float(os.getenv("TRAIL_TRIGGER_R", "1.5"))
TRAIL_ATR_MULT = float(os.getenv("TRAIL_ATR_MULT", "1.0"))

# Portfolio filters
CORR_THRESHOLD = float(os.getenv("CORR_THRESHOLD", "0.80"))
SYMBOL_COOLDOWN_MIN = int(os.getenv("SYMBOL_COOLDOWN_MIN", "45"))
GLOBAL_ENTRY_COOLDOWN_MIN = int(os.getenv("GLOBAL_ENTRY_COOLDOWN_MIN", "10"))

# Paper realism
PAPER_START_BALANCE = float(os.getenv("PAPER_START_BALANCE", "250"))
PAPER_FEE_RATE = float(os.getenv("PAPER_FEE_RATE", "0.0005"))
PAPER_SLIPPAGE_BPS = float(os.getenv("PAPER_SLIPPAGE_BPS", "2.0"))

# Runtime
SCAN_SECONDS = int(os.getenv("SCAN_SECONDS", "30"))
TRACK_SECONDS = int(os.getenv("TRACK_SECONDS", "5"))
KLINE_LIMIT = int(os.getenv("KLINE_LIMIT", "160"))
DB_PATH = os.getenv("SNIPER_DB", "sniper_ml_v1.db")
ML_DATASET_PATH = os.getenv("ML_DATASET_PATH", "sniper_ml_dataset.csv")


# ============================================================
# DATA CLASSES
# ============================================================

@dataclass
class Signal:
    symbol: str
    side: str
    score_long: int
    score_short: int
    score: int
    price: float
    atr: float
    atr_pct: float
    adx: float
    rsi: float
    volume_ratio: float
    stop_price: float
    tp_price: float
    candle_time: int
    regime: str
    ema_gap_15: float
    ema_gap_1h: float
    candle_body_pct: float
    upper_wick_pct: float
    lower_wick_pct: float


@dataclass
class Position:
    symbol: str
    side: str
    entry: float
    qty: float
    stop: float
    tp: float
    initial_risk: float
    atr: float
    opened_at: int
    be_done: bool = False
    trail_done: bool = False
    last_stop_update: float = 0.0


# ============================================================
# TELEGRAM
# ============================================================

async def send_telegram(message: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    print(f"[Telegram] HTTP {resp.status}: {await resp.text()}")
    except Exception as exc:
        print(f"[Telegram] {exc}")


# ============================================================
# BINANCE REST CLIENT
# ============================================================

class BinanceClient:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.exchange_info: Dict[str, dict] = {}

    async def init(self):
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=15)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers={"X-MBX-APIKEY": API_KEY} if API_KEY else {}
            )

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    def _sign(self, params: dict) -> dict:
        params = dict(params)
        params["timestamp"] = int(time.time() * 1000)
        params.setdefault("recvWindow", 5000)
        query = urllib.parse.urlencode(params)
        params["signature"] = hmac.new(
            API_SECRET.encode(), query.encode(), hashlib.sha256
        ).hexdigest()
        return params

    async def request(self, method: str, path: str, params=None, signed=False):
        await self.init()
        params = params or {}
        if signed:
            if not API_KEY or not API_SECRET:
                raise RuntimeError("Signed Binance request requested but API credentials are missing.")
            params = self._sign(params)

        url = BASE_URL + path
        async with self.session.request(method, url, params=params if method == "GET" else None,
                                        data=params if method != "GET" else None) as resp:
            text = await resp.text()
            try:
                data = await resp.json()
            except Exception:
                data = {"raw": text}

            if resp.status >= 400:
                raise RuntimeError(f"Binance HTTP {resp.status}: {data}")
            return data

    async def load_exchange_info(self):
        data = await self.request("GET", "/fapi/v1/exchangeInfo")
        for s in data.get("symbols", []):
            if s.get("status") != "TRADING":
                continue
            filters = {f["filterType"]: f for f in s.get("filters", [])}
            lot = filters.get("LOT_SIZE", {})
            market_lot = filters.get("MARKET_LOT_SIZE", lot)
            notional = filters.get("MIN_NOTIONAL", {})
            self.exchange_info[s["symbol"]] = {
                "step": float(market_lot.get("stepSize", lot.get("stepSize", "0.001"))),
                "min_qty": float(market_lot.get("minQty", lot.get("minQty", "0"))),
                "min_notional": float(notional.get("notional", "0")),
                "price_tick": float(filters.get("PRICE_FILTER", {}).get("tickSize", "0.00000001")),
            }

    async def get_klines(self, symbol: str, interval: str, limit: int = 160) -> np.ndarray:
        data = await self.request("GET", "/fapi/v1/klines",
                                   {"symbol": symbol, "interval": interval, "limit": limit})
        # [open, high, low, close, volume, close_time]
        rows = []
        now_ms = int(time.time() * 1000)
        for x in data:
            if int(x[6]) >= now_ms:
                continue
            rows.append([
                float(x[1]), float(x[2]), float(x[3]), float(x[4]),
                float(x[5]), int(x[6])
            ])
        return np.array(rows, dtype=float)

    async def get_equity(self) -> float:
        if not LIVE_TRADING:
            return PAPER_START_BALANCE
        data = await self.request("GET", "/fapi/v2/account", signed=True)
        return float(data.get("totalMarginBalance", 0.0))

    async def get_mark_price(self, symbol: str) -> float:
        data = await self.request("GET", "/fapi/v1/premiumIndex", {"symbol": symbol})
        return float(data["markPrice"])

    async def place_market(self, symbol: str, side: str, quantity: float) -> dict:
        if not LIVE_TRADING:
            return {"paper": True, "status": "FILLED", "avgPrice": 0.0}
        return await self.request("POST", "/fapi/v1/order", {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": format_qty(quantity),
            "newOrderRespType": "RESULT",
        }, signed=True)

    async def place_protection(self, symbol: str, side: str, stop_price: float,
                               quantity: float = 0.0, close_all: bool = True) -> dict:
        """
        Uses the current USDⓈ-M conditional-order interface when LIVE is enabled.

        NOTE: Binance has changed conditional/algo endpoints over time. This
        function deliberately raises on API rejection instead of pretending
        that a protection order exists.
        """
        if not LIVE_TRADING:
            return {"paper": True, "status": "SIMULATED"}

        params = {
            "symbol": symbol,
            "side": side,
            "type": "STOP_MARKET",
            "stopPrice": format_price(stop_price),
            "workingType": "MARK_PRICE",
            "positionSide": "BOTH",
        }

        if close_all:
            params["closePosition"] = "true"
            # Do NOT send quantity or reduceOnly with closePosition=true.
        else:
            params["quantity"] = format_qty(quantity)
            params["reduceOnly"] = "true"

        # Binance's current USDⓈ-M API may require /fapi/v1/algoOrder.
        # Keeping this isolated makes the execution layer easy to update
        # without touching strategy logic.
        return await self.request("POST", "/fapi/v1/algoOrder", params, signed=True)


# ============================================================
# HELPERS
# ============================================================

def format_qty(qty: float) -> str:
    return f"{qty:.8f}".rstrip("0").rstrip(".")


def format_price(price: float) -> str:
    return f"{price:.8f}".rstrip("0").rstrip(".")


def floor_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return math.floor(value / step + 1e-12) * step



def closed_klines(k: np.ndarray, interval_ms: int) -> np.ndarray:
    """Return only fully closed candles."""
    if len(k) == 0:
        return k
    now_ms = int(time.time() * 1000)
    # Binance kline timestamp is candle open time.
    if now_ms < int(k[-1, 0]) + interval_ms:
        return k[:-1]
    return k

def ema(x: np.ndarray, period: int) -> np.ndarray:
    out = np.full(len(x), np.nan, dtype=float)
    if len(x) < period:
        return out
    out[period - 1] = np.mean(x[:period])
    alpha = 2.0 / (period + 1.0)
    for i in range(period, len(x)):
        out[i] = alpha * x[i] + (1 - alpha) * out[i - 1]
    return out


def rsi_wilder(close: np.ndarray, period: int = 14) -> float:
    if len(close) < period + 1:
        return float("nan")
    delta = np.diff(close)
    gain = np.maximum(delta, 0.0)
    loss = np.maximum(-delta, 0.0)

    avg_gain = np.mean(gain[:period])
    avg_loss = np.mean(loss[:period])

    for i in range(period, len(delta)):
        avg_gain = (avg_gain * (period - 1) + gain[i]) / period
        avg_loss = (avg_loss * (period - 1) + loss[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100.0 - 100.0 / (1.0 + rs))


def atr_wilder(k: np.ndarray, period: int = 14) -> float:
    if len(k) < period + 1:
        return float("nan")
    high, low, close = k[:, 1], k[:, 2], k[:, 3]
    tr = np.maximum(
        high[1:] - low[1:],
        np.maximum(np.abs(high[1:] - close[:-1]),
                   np.abs(low[1:] - close[:-1]))
    )
    atr = np.mean(tr[:period])
    for i in range(period, len(tr)):
        atr = (atr * (period - 1) + tr[i]) / period
    return float(atr)


def adx_wilder(k: np.ndarray, period: int = 14) -> float:
    if len(k) < 2 * period + 1:
        return float("nan")
    high, low, close = k[:, 1], k[:, 2], k[:, 3]
    up = high[1:] - high[:-1]
    down = low[:-1] - low[1:]
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)
    tr = np.maximum(
        high[1:] - low[1:],
        np.maximum(np.abs(high[1:] - close[:-1]),
                   np.abs(low[1:] - close[:-1]))
    )

    atr = np.mean(tr[:period])
    p_dm = np.mean(plus_dm[:period])
    m_dm = np.mean(minus_dm[:period])

    dx_values = []
    for i in range(period, len(tr)):
        atr = (atr * (period - 1) + tr[i]) / period
        p_dm = (p_dm * (period - 1) + plus_dm[i]) / period
        m_dm = (m_dm * (period - 1) + minus_dm[i]) / period

        if atr <= 0:
            dx_values.append(0.0)
            continue

        pdi = 100.0 * p_dm / atr
        mdi = 100.0 * m_dm / atr
        dx_values.append(100.0 * abs(pdi - mdi) / (pdi + mdi + 1e-12))

    if len(dx_values) < period:
        return float("nan")

    adx = float(np.mean(dx_values[:period]))
    for i in range(period, len(dx_values)):
        adx = (adx * (period - 1) + dx_values[i]) / period
    return adx


def volume_ratio(k: np.ndarray, lookback: int = 20) -> float:
    if len(k) < lookback + 1:
        return float("nan")
    avg = float(np.mean(k[-lookback-1:-1, 4]))
    return float(k[-1, 4] / avg) if avg > 0 else 0.0


def correlation(a: np.ndarray, b: np.ndarray) -> float:
    n = min(len(a), len(b))
    if n < 30:
        return 0.0
    ra = np.diff(np.log(a[-n:, 3]))
    rb = np.diff(np.log(b[-n:, 3]))
    if np.std(ra) == 0 or np.std(rb) == 0:
        return 0.0
    c = float(np.corrcoef(ra, rb)[0, 1])
    return 0.0 if not np.isfinite(c) else c


def adverse_slippage(price: float, side: str, bps: float) -> float:
    delta = price * bps / 10000.0
    return price + delta if side == "BUY" else price - delta


def exit_slippage(price: float, side: str, bps: float) -> float:
    # side is the ORIGINAL position side.
    delta = price * bps / 10000.0
    return price - delta if side == "BUY" else price + delta


# ============================================================
# DATABASE
# ============================================================

def db():
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = db()
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            side TEXT,
            entry REAL,
            exit REAL,
            qty REAL,
            stop REAL,
            tp REAL,
            pnl REAL,
            pnl_pct REAL,
            reason TEXT,
            opened_at INTEGER,
            closed_at INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    # Every qualifying candidate signal is stored for future ML training.
    # outcome_label: 1 = TP/reward reached first, 0 = SL/reward failed first.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signal_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            regime TEXT,
            candle_time INTEGER NOT NULL,
            entry_price REAL NOT NULL,
            stop_price REAL NOT NULL,
            tp_price REAL NOT NULL,
            score_long INTEGER,
            score_short INTEGER,
            score INTEGER,
            rsi REAL,
            adx REAL,
            atr REAL,
            atr_pct REAL,
            volume_ratio REAL,
            ema_gap_15 REAL,
            ema_gap_1h REAL,
            candle_body_pct REAL,
            upper_wick_pct REAL,
            lower_wick_pct REAL,
            decision TEXT,
            outcome_label INTEGER,
            outcome_reason TEXT,
            outcome_price REAL,
            labeled_at INTEGER,
            created_at INTEGER NOT NULL,
            UNIQUE(symbol, side, candle_time)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_pending ON signal_snapshots(outcome_label, candle_time)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_symbol_time ON signal_snapshots(symbol, candle_time)")
    conn.commit()
    conn.close()


def set_state(key: str, value: str):
    conn = db()
    conn.execute("INSERT OR REPLACE INTO state(key,value) VALUES(?,?)", (key, value))
    conn.commit()
    conn.close()


def get_state(key: str, default=None):
    conn = db()
    row = conn.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
    conn.close()
    return default if row is None else row[0]


def record_trade(p: Position, exit_price: float, reason: str):
    if p.side == "BUY":
        gross = (exit_price - p.entry) * p.qty
    else:
        gross = (p.entry - exit_price) * p.qty

    fees = (p.entry * p.qty + exit_price * p.qty) * PAPER_FEE_RATE
    pnl = gross - fees
    pnl_pct = pnl / max(p.entry * p.qty, 1e-9)

    conn = db()
    conn.execute("""
        INSERT INTO trades
        (symbol,side,entry,exit,qty,stop,tp,pnl,pnl_pct,reason,opened_at,closed_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        p.symbol, p.side, p.entry, exit_price, p.qty, p.stop, p.tp,
        pnl, pnl_pct, reason, p.opened_at, int(time.time())
    ))
    conn.commit()
    conn.close()
    return pnl


# ============================================================
# ML DATA ENGINE
# ============================================================

def save_signal_snapshot(sig: Signal, decision: str):
    conn = db()
    conn.execute("""
        INSERT OR IGNORE INTO signal_snapshots
        (symbol,side,regime,candle_time,entry_price,stop_price,tp_price,
         score_long,score_short,score,rsi,adx,atr,atr_pct,volume_ratio,
         ema_gap_15,ema_gap_1h,candle_body_pct,upper_wick_pct,lower_wick_pct,
         decision,created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        sig.symbol, sig.side, sig.regime, sig.candle_time, sig.price,
        sig.stop_price, sig.tp_price, sig.score_long, sig.score_short, sig.score,
        sig.rsi, sig.adx, sig.atr, sig.atr_pct, sig.volume_ratio,
        sig.ema_gap_15, sig.ema_gap_1h, sig.candle_body_pct,
        sig.upper_wick_pct, sig.lower_wick_pct, decision, int(time.time())
    ))
    conn.commit()
    conn.close()


async def label_pending_signals(bot_client, horizon_candles: int = 8):
    """Label old candidate signals using future 15m candles.

    Conservative rule: if SL and TP are both touched in the same candle,
    SL is considered first because candle data has no intrabar ordering.
    """
    conn = db()
    rows = conn.execute("""
        SELECT id,symbol,side,candle_time,entry_price,stop_price,tp_price
        FROM signal_snapshots
        WHERE outcome_label IS NULL
        ORDER BY candle_time ASC LIMIT 100
    """).fetchall()
    conn.close()
    now_ms = int(time.time() * 1000)

    for row in rows:
        sid, symbol, side, candle_time, entry, stop, tp = row
        # candle_time is Binance open time in ms; wait until the full horizon exists.
        horizon_end = candle_time + horizon_candles * 15 * 60 * 1000
        if now_ms < horizon_end:
            continue
        try:
            k = await bot_client.get_klines(symbol, "15m", horizon_candles + 3)
        except Exception:
            continue
        future = k[k[:, 5] > candle_time]
        if len(future) < horizon_candles:
            continue
        future = future[:horizon_candles]
        label = None
        reason = None
        out_price = None
        for c in future:
            high, low, close = float(c[1]), float(c[2]), float(c[3])
            if side == "BUY":
                sl_hit, tp_hit = low <= stop, high >= tp
            else:
                sl_hit, tp_hit = high >= stop, low <= tp
            if sl_hit:
                label, reason, out_price = 0, "SL_FIRST", stop
                break
            if tp_hit:
                label, reason, out_price = 1, "TP_FIRST", tp
                break
        if label is None:
            close = float(future[-1, 3])
            if side == "BUY":
                label = 1 if close > entry else 0
            else:
                label = 1 if close < entry else 0
            reason, out_price = "HORIZON_CLOSE", close

        conn = db()
        conn.execute("""
            UPDATE signal_snapshots
            SET outcome_label=?, outcome_reason=?, outcome_price=?, labeled_at=?
            WHERE id=?
        """, (label, reason, out_price, int(time.time()), sid))
        conn.commit()
        conn.close()


def export_ml_dataset(path: str = "sniper_ml_dataset.csv") -> int:
    conn = db()
    rows = conn.execute("""
        SELECT symbol,side,regime,candle_time,entry_price,stop_price,tp_price,
               score_long,score_short,score,rsi,adx,atr,atr_pct,volume_ratio,
               ema_gap_15,ema_gap_1h,candle_body_pct,upper_wick_pct,lower_wick_pct,
               decision,outcome_label,outcome_reason,outcome_price,created_at,labeled_at
        FROM signal_snapshots
        WHERE outcome_label IS NOT NULL
        ORDER BY candle_time ASC
    """).fetchall()
    # Explicit export column names keep the ML schema stable.
    headers = [
        "symbol","side","regime","candle_time","entry_price","stop_price","tp_price",
        "score_long","score_short","score","rsi","adx","atr","atr_pct","volume_ratio",
        "ema_gap_15","ema_gap_1h","candle_body_pct","upper_wick_pct","lower_wick_pct",
        "decision","outcome_label","outcome_reason","outcome_price","created_at","labeled_at"
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(rows)
    conn.close()
    return len(rows)


def dataset_stats():
    conn = db()
    total = conn.execute("SELECT COUNT(*) FROM signal_snapshots").fetchone()[0]
    labeled = conn.execute("SELECT COUNT(*) FROM signal_snapshots WHERE outcome_label IS NOT NULL").fetchone()[0]
    wins = conn.execute("SELECT COUNT(*) FROM signal_snapshots WHERE outcome_label=1").fetchone()[0]
    conn.close()
    return total, labeled, wins


# ============================================================
# SNIPER ENGINE
# ============================================================

class SniperBot:
    def __init__(self):
        self.client = BinanceClient()
        self.positions: Dict[str, Position] = {}
        self.cache_15m: Dict[str, np.ndarray] = {}
        self.last_entry_by_symbol: Dict[str, float] = {}
        self.last_global_entry: float = 0.0
        self.paper_equity = PAPER_START_BALANCE
        self.day_start_equity = PAPER_START_BALANCE
        self.day_key = time.strftime("%Y-%m-%d", time.gmtime())
        self.stop_requested = False

    async def start(self):
        init_db()
        await self.client.init()
        await self.client.load_exchange_info()

        if LIVE_TRADING and (not API_KEY or not API_SECRET):
            raise RuntimeError("LIVE_TRADING=true but Binance credentials are missing.")

        await send_telegram(
            "🧠 <b>SNIPER ML V1 STARTED</b>\n"
            "Mode: <b>PAPER ONLY</b>\n"
            "Training data collection: <b>ON</b>\n"
            f"Capital: ${self.paper_equity:.2f}\n"
            f"Capital: ${self.paper_equity:.2f} (paper baseline)\n"
            f"Risk/trade: {RISK_PER_TRADE*100:.2f}%\n"
            f"Max positions: {MAX_OPEN_POSITIONS}"
        )

    def reset_day_if_needed(self):
        key = time.strftime("%Y-%m-%d", time.gmtime())
        if key != self.day_key:
            self.day_key = key
            self.day_start_equity = self.current_equity()

    def current_equity(self) -> float:
        if LIVE_TRADING:
            # Live equity is refreshed asynchronously before entries.
            return self.paper_equity
        unrealized = 0.0
        for p in self.positions.values():
            k = self.cache_15m.get(p.symbol)
            if k is not None and len(k):
                price = float(k[-1, 3])
                unrealized += (price - p.entry) * p.qty if p.side == "BUY" else (p.entry - price) * p.qty
        return self.paper_equity + unrealized

    async def live_equity(self) -> float:
        if LIVE_TRADING:
            return await self.client.get_equity()
        return self.current_equity()

    def daily_locked(self, equity: float) -> bool:
        dd = 1.0 - equity / max(self.day_start_equity, 1e-9)
        return dd >= MAX_DAILY_DRAWDOWN

    async def btc_regime(self) -> str:
        k = await self.client.get_klines("BTCUSDT", "1h", 220)
        close = k[:, 3]
        e20 = ema(close, 20)[-1]
        e50 = ema(close, 50)[-1]
        e200 = ema(close, 200)[-1]
        adx = adx_wilder(k, 14)

        if not np.isfinite(adx):
            return "UNKNOWN"
        if adx < 18:
            return "RANGE"
        if close[-1] > e200 and e20 > e50:
            return "BULL"
        if close[-1] < e200 and e20 < e50:
            return "BEAR"
        return "NEUTRAL"

    async def build_signal(self, symbol: str, regime: str) -> Optional[Signal]:
        k15 = closed_klines(await self.client.get_klines(symbol, "15m", KLINE_LIMIT), 15 * 60 * 1000)
        k1h = closed_klines(await self.client.get_klines(symbol, "1h", KLINE_LIMIT), 60 * 60 * 1000)

        if len(k15) < 80 or len(k1h) < 80:
            return None

        self.cache_15m[symbol] = k15

        c15 = k15[:, 3]
        c1h = k1h[:, 3]

        e20_15 = ema(c15, 20)[-1]
        e50_15 = ema(c15, 50)[-1]
        e20_1h = ema(c1h, 20)[-1]
        e50_1h = ema(c1h, 50)[-1]

        rsi = rsi_wilder(c15, 14)
        atr = atr_wilder(k15, 14)
        adx = adx_wilder(k15, 14)
        vr = volume_ratio(k15, 20)

        price = float(c15[-1])
        atr_pct = atr / price if price > 0 else float("nan")

        if not all(np.isfinite(x) for x in [e20_15, e50_15, e20_1h, e50_1h, rsi, atr, adx, vr]):
            return None
        if adx < MIN_ADX or not (MIN_ATR_PCT <= atr_pct <= MAX_ATR_PCT):
            return None
        if vr < MIN_VOLUME_RATIO:
            return None

        long_score = 0
        short_score = 0

        # Trend alignment
        if e20_15 > e50_15:
            long_score += 2
        elif e20_15 < e50_15:
            short_score += 2

        if e20_1h > e50_1h:
            long_score += 2
        elif e20_1h < e50_1h:
            short_score += 2

        # Momentum zones
        if 52 <= rsi <= 68:
            long_score += 1
        if 32 <= rsi <= 48:
            short_score += 1

        # Candle confirmation
        prev_high = k15[-2, 1]
        prev_low = k15[-2, 2]
        candle_open = k15[-1, 0]
        candle_high = k15[-1, 1]
        candle_low = k15[-1, 2]
        candle_range = max(candle_high - candle_low, 1e-12)
        candle_body_pct = abs(price - candle_open) / max(price, 1e-12)
        upper_wick_pct = (candle_high - max(candle_open, price)) / max(price, 1e-12)
        lower_wick_pct = (min(candle_open, price) - candle_low) / max(price, 1e-12)
        ema_gap_15 = (e20_15 - e50_15) / max(price, 1e-12)
        ema_gap_1h = (e20_1h - e50_1h) / max(price, 1e-12)

        if price > candle_open and price >= prev_high * 0.999:
            long_score += 1
        if price < candle_open and price <= prev_low * 1.001:
            short_score += 1

        # Volume expansion
        if vr >= 1.15:
            long_score += 1
            short_score += 1

        # BTC regime is a soft filter, not an unconditional direction.
        # It reduces counter-regime signals instead of fabricating signals.
        if regime == "BULL":
            short_score -= 1
        elif regime == "BEAR":
            long_score -= 1
        elif regime == "RANGE":
            return None

        if long_score < MIN_SCORE and short_score < MIN_SCORE:
            return None
        if abs(long_score - short_score) < MIN_SCORE_EDGE:
            return None

        side = "BUY" if long_score > short_score else "SELL"
        score = max(long_score, short_score)

        stop_pct = min(MAX_STOP_PCT, max(MIN_STOP_PCT, ATR_STOP_MULT * atr_pct))
        stop_dist = price * stop_pct

        if side == "BUY":
            stop = price - stop_dist
            tp = price + stop_dist * REWARD_R
        else:
            stop = price + stop_dist
            tp = price - stop_dist * REWARD_R

        return Signal(
            symbol=symbol, side=side,
            score_long=long_score, score_short=short_score,
            score=score, price=price, atr=atr, atr_pct=atr_pct,
            adx=adx, rsi=rsi, volume_ratio=vr,
            stop_price=stop, tp_price=tp,
            candle_time=int(k15[-1, 5]),
            regime=regime,
            ema_gap_15=ema_gap_15,
            ema_gap_1h=ema_gap_1h,
            candle_body_pct=candle_body_pct,
            upper_wick_pct=upper_wick_pct,
            lower_wick_pct=lower_wick_pct,
        )

    def position_risk(self, p: Position) -> float:
        return p.initial_risk * p.qty

    def portfolio_risk(self) -> float:
        return sum(self.position_risk(p) for p in self.positions.values())

    def correlation_blocked(self, symbol: str) -> bool:
        k = self.cache_15m.get(symbol)
        if k is None:
            return False
        for other in self.positions:
            ko = self.cache_15m.get(other)
            if ko is not None and correlation(k, ko) >= CORR_THRESHOLD:
                return True
        return False

    def size_position(self, sig: Signal, equity: float) -> float:
        risk_cash = equity * RISK_PER_TRADE
        stop_dist = abs(sig.price - sig.stop_price)
        if stop_dist <= 0:
            return 0.0

        qty = risk_cash / stop_dist

        # Hard notional/margin cap.
        max_notional = equity * MAX_MARGIN_PER_TRADE * max(1, LEVERAGE)
        qty = min(qty, max_notional / sig.price)

        info = self.client.exchange_info.get(sig.symbol)
        if info:
            qty = floor_step(qty, info["step"])
            if qty < info["min_qty"]:
                return 0.0
            if info["min_notional"] > 0 and qty * sig.price < info["min_notional"]:
                return 0.0

        return max(0.0, qty)

    async def open_position(self, sig: Signal, equity: float):
        if sig.symbol in self.positions:
            return
        if len(self.positions) >= MAX_OPEN_POSITIONS:
            return
        if self.correlation_blocked(sig.symbol):
            return

        now = time.time()
        if now - self.last_global_entry < GLOBAL_ENTRY_COOLDOWN_MIN * 60:
            return
        if now - self.last_entry_by_symbol.get(sig.symbol, 0) < SYMBOL_COOLDOWN_MIN * 60:
            return
        if self.portfolio_risk() + equity * RISK_PER_TRADE > equity * MAX_TOTAL_RISK:
            return

        qty = self.size_position(sig, equity)
        if qty <= 0:
            return

        if LIVE_TRADING:
            order = await self.client.place_market(sig.symbol, sig.side, qty)
            if order.get("status") != "FILLED":
                await send_telegram(
                    f"⚠️ <b>ENTRY NOT FILLED</b>\n{sig.symbol}\n"
                    f"<code>{order}</code>"
                )
                return

            actual = float(order.get("avgPrice") or sig.price)
            if actual <= 0:
                actual = sig.price
            entry = actual

            if sig.side == "BUY":
                stop = entry - abs(sig.price - sig.stop_price)
                tp = entry + abs(sig.price - sig.stop_price) * REWARD_R
                protection_side = "SELL"
            else:
                stop = entry + abs(sig.price - sig.stop_price)
                tp = entry - abs(sig.price - sig.stop_price) * REWARD_R
                protection_side = "BUY"

            # Critical rule: if protection cannot be installed, immediately
            # flatten instead of leaving an unprotected live position.
            try:
                sl_res = await self.client.place_protection(
                    sig.symbol, protection_side, stop, close_all=True
                )
                if sl_res.get("algoStatus") not in (None, "NEW"):
                    raise RuntimeError(f"SL rejected: {sl_res}")

                tp_res = await self.client.place_protection(
                    sig.symbol, protection_side, tp, close_all=True
                )
                if tp_res.get("algoStatus") not in (None, "NEW"):
                    raise RuntimeError(f"TP rejected: {tp_res}")

            except Exception as exc:
                await send_telegram(
                    f"🚨 <b>PROTECTION FAILURE</b>\n{sig.symbol}\n"
                    f"Position was opened but protection failed.\n"
                    f"<code>{str(exc)[:700]}</code>"
                )
                # Do not pretend it is protected. A production implementation
                # should query the real position and market-close it here.
                return
        else:
            entry = adverse_slippage(sig.price, sig.side, PAPER_SLIPPAGE_BPS)
            stop = sig.stop_price
            tp = sig.tp_price

        p = Position(
            symbol=sig.symbol, side=sig.side, entry=entry, qty=qty,
            stop=stop, tp=tp,
            initial_risk=abs(entry - stop),
            atr=sig.atr,
            opened_at=int(time.time())
        )
        self.positions[sig.symbol] = p
        conn = db()
        conn.execute(
            "UPDATE signal_snapshots SET decision='EXECUTED' WHERE symbol=? AND side=? AND candle_time=?",
            (sig.symbol, sig.side, sig.candle_time),
        )
        conn.commit()
        conn.close()
        self.last_entry_by_symbol[sig.symbol] = time.time()
        self.last_global_entry = time.time()

        await send_telegram(
            f"🎯 <b>{'LIVE' if LIVE_TRADING else 'PAPER'} ENTRY</b>\n"
            f"{sig.symbol} {sig.side}\n"
            f"Entry: {entry:.6f}\n"
            f"SL: {stop:.6f}\n"
            f"TP: {tp:.6f}\n"
            f"Score: {sig.score} ({sig.score_long}/{sig.score_short})\n"
            f"ADX: {sig.adx:.1f} | RSI: {sig.rsi:.1f} | Vol: {sig.volume_ratio:.2f}"
        )

    async def close_position(self, p: Position, price: float, reason: str):
        exit_price = exit_slippage(price, p.side, PAPER_SLIPPAGE_BPS)
        pnl = record_trade(p, exit_price, reason)

        if not LIVE_TRADING:
            self.paper_equity += pnl

        self.positions.pop(p.symbol, None)

        await send_telegram(
            f"🏁 <b>{'LIVE' if LIVE_TRADING else 'PAPER'} EXIT</b>\n"
            f"{p.symbol} {p.side}\n"
            f"Exit: {exit_price:.6f}\n"
            f"Reason: {reason}\n"
            f"PnL: {pnl:+.4f}\n"
            f"Equity: ${self.paper_equity:.2f}"
        )

    async def manage_paper_position(self, p: Position, k: np.ndarray):
        if len(k) < 2:
            return

        # Use the latest CLOSED candle's high/low.
        high = float(k[-1, 1])
        low = float(k[-1, 2])

        if p.side == "BUY":
            stop_hit = low <= p.stop
            tp_hit = high >= p.tp
        else:
            stop_hit = high >= p.stop
            tp_hit = low <= p.tp

        # Conservative ambiguity handling: if both are touched inside one
        # candle and we do not have tick order, assume SL first.
        if stop_hit:
            await self.close_position(p, p.stop, "STOP")
            return
        if tp_hit:
            await self.close_position(p, p.tp, "TP")
            return

        close = float(k[-1, 3])
        r = (close - p.entry) / p.initial_risk if p.side == "BUY" else (p.entry - close) / p.initial_risk

        # Break-even
        if r >= BE_TRIGGER_R and not p.be_done:
            if p.side == "BUY":
                p.stop = max(p.stop, p.entry * (1 + BE_LOCK_PCT))
            else:
                p.stop = min(p.stop, p.entry * (1 - BE_LOCK_PCT))
            p.be_done = True

        # ATR trailing
        if r >= TRAIL_TRIGGER_R:
            atr = atr_wilder(k, 14)
            if np.isfinite(atr):
                if p.side == "BUY":
                    new_stop = close - atr * TRAIL_ATR_MULT
                    if new_stop > p.stop:
                        p.stop = new_stop
                        p.trail_done = True
                else:
                    new_stop = close + atr * TRAIL_ATR_MULT
                    if new_stop < p.stop:
                        p.stop = new_stop
                        p.trail_done = True

    async def manage_live_positions(self):
        # Live mode deliberately does not simulate exits from candle closes.
        # The exchange must be the source of truth for filled SL/TP orders.
        # A production deployment should consume Binance ORDER_TRADE_UPDATE
        # user-data events and reconcile positions/orders continuously.
        return

    async def tracker_loop(self):
        while not self.stop_requested:
            try:
                if not LIVE_TRADING:
                    for symbol in list(self.positions):
                        k = closed_klines(await self.client.get_klines(symbol, "15m", 160), 15 * 60 * 1000)
                        self.cache_15m[symbol] = k
                        if symbol in self.positions:
                            await self.manage_paper_position(self.positions[symbol], k)
                else:
                    await self.manage_live_positions()

                await asyncio.sleep(TRACK_SECONDS)
            except Exception as exc:
                print(f"[Tracker] {exc}")
                await asyncio.sleep(TRACK_SECONDS)

    async def scan_once(self):
        await label_pending_signals(self.client)
        total, labeled, wins = dataset_stats()
        if labeled and labeled % 25 == 0:
            export_ml_dataset(ML_DATASET_PATH)
        regime = await self.btc_regime()
        if regime in ("UNKNOWN", "RANGE"):
            return

        equity = await self.live_equity()
        self.reset_day_if_needed()
        if self.daily_locked(equity):
            await send_telegram(
                f"🛑 <b>DAILY LOSS LOCK</b>\nEquity: ${equity:.2f}\n"
                f"Limit: {MAX_DAILY_DRAWDOWN*100:.1f}%"
            )
            return

        results = []
        for symbol in SYMBOLS:
            try:
                if symbol in self.positions:
                    continue
                sig = await self.build_signal(symbol, regime)
                if sig:
                    results.append(sig)
                    # Candidate exists even if portfolio rules later reject entry.
                    save_signal_snapshot(sig, "CANDIDATE")
            except Exception as exc:
                print(f"[Signal {symbol}] {exc}")

        results.sort(
            key=lambda s: (s.score, s.adx, s.volume_ratio),
            reverse=True
        )

        for sig in results:
            if len(self.positions) >= MAX_OPEN_POSITIONS:
                break
            await self.open_position(sig, equity)

    async def run(self):
        await self.start()
        tracker = asyncio.create_task(self.tracker_loop())

        try:
            while not self.stop_requested:
                started = time.time()
                try:
                    await self.scan_once()
                except Exception as exc:
                    print(f"[Main loop] {exc}")
                    await send_telegram(f"⚠️ <b>LOOP ERROR</b>\n<code>{str(exc)[:700]}</code>")

                elapsed = time.time() - started
                await asyncio.sleep(max(5, SCAN_SECONDS - elapsed))
        finally:
            self.stop_requested = True
            tracker.cancel()
            try:
                n = export_ml_dataset(ML_DATASET_PATH)
                total, labeled, wins = dataset_stats()
                await send_telegram(
                    f"📊 <b>ML DATASET</b>\nCandidates: {total}\nLabeled: {labeled}\n"
                    f"Historical label win-rate: {(wins/labeled*100 if labeled else 0):.1f}%\n"
                    f"CSV rows exported: {n}"
                )
            except Exception as exc:
                print(f"[Dataset export] {exc}")
            await self.client.close()


async def main():
    bot = SniperBot()
    try:
        await bot.run()
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        await send_telegram(f"🚨 <b>BOT START FAILED</b>\n<code>{str(exc)[:800]}</code>")
        raise


if __name__ == "__main__":
    asyncio.run(main())
