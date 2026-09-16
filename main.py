
import os
import time
import hmac
import hashlib
import logging
import sqlite3
import threading
import asyncio
import math
import uuid
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from datetime import datetime, timezone
from urllib.parse import urlencode

import requests
from flask import Flask
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

# ============================================================
# CONFIG
# ============================================================

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY", "").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
LIVE_TRADING = os.getenv("LIVE_TRADING", "false").lower() == "true"

BINANCE_BASE_URL = os.getenv("BINANCE_BASE_URL", "https://fapi.binance.com").rstrip("/")
PAPER_START_EQUITY = float(os.getenv("PAPER_START_EQUITY", "300"))

LEVERAGE = int(os.getenv("LEVERAGE", "5"))
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "0.005"))
MAX_MARGIN_PER_TRADE = float(os.getenv("MAX_MARGIN_PER_TRADE", "0.08"))
MAX_TOTAL_MARGIN = float(os.getenv("MAX_TOTAL_MARGIN", "0.24"))
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "3"))
MAX_DAILY_DRAWDOWN = float(os.getenv("MAX_DAILY_DRAWDOWN", "0.025"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "45"))
GLOBAL_ENTRY_COOLDOWN_MINUTES = int(os.getenv("GLOBAL_ENTRY_COOLDOWN_MINUTES", "10"))

MIN_SCORE = float(os.getenv("MIN_SCORE", "7"))
MIN_ADX = float(os.getenv("MIN_ADX", "18"))
MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "0.85"))
MIN_ATR_PCT = float(os.getenv("MIN_ATR_PCT", "0.0025"))
MAX_ATR_PCT = float(os.getenv("MAX_ATR_PCT", "0.04"))

ATR_STOP_MULT = float(os.getenv("ATR_STOP_MULT", "1.35"))
MIN_STOP_PCT = float(os.getenv("MIN_STOP_PCT", "0.006"))
MAX_STOP_PCT = float(os.getenv("MAX_STOP_PCT", "0.022"))
REWARD_R = float(os.getenv("REWARD_R", "2.20"))

BE_TRIGGER_R = float(os.getenv("BE_TRIGGER_R", "1.0"))
BE_LOCK_PCT = float(os.getenv("BE_LOCK_PCT", "0.0008"))
TRAIL_TRIGGER_R = float(os.getenv("TRAIL_TRIGGER_R", "1.5"))
TRAIL_ATR_MULT = float(os.getenv("TRAIL_ATR_MULT", "1.0"))

SCAN_INTERVAL_SECONDS = max(30, int(os.getenv("SCAN_INTERVAL_SECONDS", "60")))
KLINE_LIMIT = min(500, max(100, int(os.getenv("KLINE_LIMIT", "150"))))
REQUEST_TIMEOUT = max(5, int(os.getenv("REQUEST_TIMEOUT", "15")))
RATE_LIMIT_COOLDOWN_SECONDS = max(60, int(os.getenv("RATE_LIMIT_COOLDOWN_SECONDS", "90")))
HTTP_MIN_INTERVAL = max(0.0, float(os.getenv("HTTP_MIN_INTERVAL", "0.03")))

DB_PATH = os.getenv("DB_PATH", "bot.db")
ALLOWED_CHAT_ID = str(TELEGRAM_CHAT_ID)

PREFERRED_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
    "UNIUSDT", "ATOMUSDT", "LTCUSDT", "NEARUSDT", "APTUSDT",
    "ARBUSDT", "OPUSDT", "INJUSDT", "SUIUSDT", "RENDERUSDT",
    "TIAUSDT", "SEIUSDT", "IMXUSDT", "PEPEUSDT",
]

# ============================================================
# GLOBAL STATE
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("futures-bot")

app = Flask(__name__)
STOP_EVENT = threading.Event()
TRADING_THREAD = None
TELEGRAM_LOOP = None
TELEGRAM_APP = None

BOT_PAUSED = False
EMERGENCY_STOP = False
BINANCE_TIME_OFFSET = 0

RATE_LIMIT_UNTIL = 0.0
RATE_LIMIT_LOCK = threading.Lock()
HTTP_GATE_LOCK = threading.Lock()
LAST_HTTP_TIME = 0.0

EXCHANGE_SYMBOLS = {}
VALID_SYMBOLS = []
STATE_LOCK = threading.Lock()
SIGNAL_CACHE = {}
SIGNAL_CACHE_LOCK = threading.Lock()

# ============================================================
# ERRORS
# ============================================================

class BinanceError(RuntimeError):
    def __init__(self, status, message, code=None, payload=None):
        self.status = status
        self.code = code
        self.payload = payload
        super().__init__(message)

class RateLimitError(BinanceError):
    pass

class TemporaryBinanceError(BinanceError):
    pass

# ============================================================
# UTILS / STATE
# ============================================================

def now_ms():
    return int(time.time() * 1000)

def log(msg, *args):
    logger.info(msg, *args)

def get_day_key():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def rate_limit_active():
    with RATE_LIMIT_LOCK:
        return time.time() < RATE_LIMIT_UNTIL

def activate_rate_limit(seconds=RATE_LIMIT_COOLDOWN_SECONDS):
    global RATE_LIMIT_UNTIL
    with RATE_LIMIT_LOCK:
        RATE_LIMIT_UNTIL = max(RATE_LIMIT_UNTIL, time.time() + seconds)

def rate_limit_remaining():
    with RATE_LIMIT_LOCK:
        return max(0, int(RATE_LIMIT_UNTIL - time.time()))

def db_connect():
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = db_connect()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            mode TEXT NOT NULL,
            entry REAL NOT NULL,
            stop REAL NOT NULL,
            tp REAL NOT NULL,
            qty REAL NOT NULL,
            notional REAL NOT NULL,
            risk_usdt REAL NOT NULL,
            opened_at INTEGER NOT NULL,
            closed_at INTEGER,
            exit_price REAL,
            pnl REAL DEFAULT 0,
            result TEXT,
            status TEXT DEFAULT 'OPEN'
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()
    conn.close()

def get_state(key, default=None):
    conn = db_connect()
    row = conn.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
    conn.close()
    return default if row is None else row["value"]

def set_state(key, value):
    conn = db_connect()
    conn.execute("""
        INSERT INTO state(key,value) VALUES(?,?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
    """, (key, str(value)))
    conn.commit()
    conn.close()

def db_open_trade(symbol, side, mode, entry, stop, tp, qty, notional, risk_usdt):
    conn = db_connect()
    cur = conn.execute("""
        INSERT INTO trades(symbol,side,mode,entry,stop,tp,qty,notional,risk_usdt,opened_at,status)
        VALUES(?,?,?,?,?,?,?,?,?,?,'OPEN')
    """, (symbol, side, mode, entry, stop, tp, qty, notional, risk_usdt, int(time.time())))
    trade_id = cur.lastrowid
    conn.commit()
    conn.close()
    return trade_id

def db_get_open_trades():
    conn = db_connect()
    rows = conn.execute("""
        SELECT * FROM trades WHERE status='OPEN' ORDER BY opened_at ASC
    """).fetchall()
    conn.close()
    return rows

def db_get_open_trade(symbol):
    conn = db_connect()
    row = conn.execute("""
        SELECT * FROM trades
        WHERE symbol=? AND status='OPEN'
        ORDER BY opened_at DESC LIMIT 1
    """, (symbol,)).fetchone()
    conn.close()
    return row

def db_close_trade(trade_id, exit_price, pnl, result):
    conn = db_connect()
    conn.execute("""
        UPDATE trades
        SET closed_at=?, exit_price=?, pnl=?, result=?, status='CLOSED'
        WHERE id=?
    """, (int(time.time()), exit_price, pnl, result, trade_id))
    conn.commit()
    conn.close()

def db_stats():
    conn = db_connect()
    total = conn.execute("SELECT COUNT(*) n FROM trades WHERE status='CLOSED'").fetchone()["n"]
    wins = conn.execute("SELECT COUNT(*) n FROM trades WHERE status='CLOSED' AND pnl>0").fetchone()["n"]
    losses = conn.execute("SELECT COUNT(*) n FROM trades WHERE status='CLOSED' AND pnl<0").fetchone()["n"]
    pnl = conn.execute("SELECT COALESCE(SUM(pnl),0) p FROM trades WHERE status='CLOSED'").fetchone()["p"]
    avg_win = conn.execute("SELECT COALESCE(AVG(pnl),0) p FROM trades WHERE status='CLOSED' AND pnl>0").fetchone()["p"]
    avg_loss = conn.execute("SELECT COALESCE(AVG(pnl),0) p FROM trades WHERE status='CLOSED' AND pnl<0").fetchone()["p"]
    conn.close()
    return {
        "total": total, "wins": wins, "losses": losses, "pnl": pnl,
        "win_rate": (wins / total * 100) if total else 0,
        "avg_win": avg_win, "avg_loss": avg_loss,
    }

# ============================================================
# TELEGRAM
# ============================================================

def telegram_send(text):
    global TELEGRAM_LOOP
    if not TELEGRAM_LOOP or not TELEGRAM_APP or not TELEGRAM_CHAT_ID:
        return
    try:
        future = asyncio.run_coroutine_threadsafe(
            TELEGRAM_APP.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            ),
            TELEGRAM_LOOP,
        )
        future.result(timeout=15)
    except Exception as exc:
        logger.error("Telegram send error: %s", exc)

def authorized(update):
    return bool(
        update.effective_chat
        and ALLOWED_CHAT_ID
        and str(update.effective_chat.id) == ALLOWED_CHAT_ID
    )

async def deny(update):
    if update.callback_query:
        await update.callback_query.answer("Access denied", show_alert=True)
    elif update.message:
        await update.message.reply_text("⛔ Access denied.")

def main_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Status", callback_data="status"),
            InlineKeyboardButton("📈 Positions", callback_data="positions"),
        ],
        [
            InlineKeyboardButton("🔎 Signals", callback_data="signals"),
            InlineKeyboardButton("📋 Stats", callback_data="stats"),
        ],
        [
            InlineKeyboardButton("⏸ Pause", callback_data="pause"),
            InlineKeyboardButton("▶️ Resume", callback_data="resume"),
        ],
        [
            InlineKeyboardButton("🚨 Emergency", callback_data="emergency"),
            InlineKeyboardButton("💥 Close All", callback_data="close_confirm"),
        ],
    ])

# ============================================================
# BINANCE CLIENT
# ============================================================

class BinanceClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "BinanceSniperBot/2.0"})
        if BINANCE_API_KEY:
            self.session.headers.update({"X-MBX-APIKEY": BINANCE_API_KEY})

    def _gate(self):
        global LAST_HTTP_TIME
        with HTTP_GATE_LOCK:
            wait = HTTP_MIN_INTERVAL - (time.monotonic() - LAST_HTTP_TIME)
            if wait > 0:
                time.sleep(wait)
            LAST_HTTP_TIME = time.monotonic()

    def _handle_response(self, response):
        text = response.text[:1200]
        try:
            payload = response.json()
        except Exception:
            payload = {"raw": text}
        code = payload.get("code") if isinstance(payload, dict) else None

        if response.status_code == 429 or code == -1003:
            retry_after = response.headers.get("Retry-After")
            seconds = RATE_LIMIT_COOLDOWN_SECONDS
            try:
                if retry_after:
                    seconds = max(seconds, int(float(retry_after)))
            except Exception:
                pass
            activate_rate_limit(seconds)
            raise RateLimitError(response.status_code, f"Binance rate limit: {text}", code, payload)

        if response.status_code in (418, 502, 503, 504):
            raise TemporaryBinanceError(response.status_code, f"Binance temporary error: {text}", code, payload)

        if response.status_code >= 400:
            raise BinanceError(response.status_code, f"Binance {response.status_code}: {text}", code, payload)

        return payload

    def _request(self, method, path, params=None, signed=False, retries=2):
        if rate_limit_active():
            raise RateLimitError(429, f"Rate-limit cooldown active for {rate_limit_remaining()}s", -1003)

        params = dict(params or {})
        if signed:
            params["timestamp"] = now_ms() + BINANCE_TIME_OFFSET
            params.setdefault("recvWindow", 10000)
            query = urlencode(params, doseq=True)
            params["signature"] = hmac.new(
                BINANCE_SECRET_KEY.encode(),
                query.encode(),
                hashlib.sha256,
            ).hexdigest()

        url = BINANCE_BASE_URL + path
        last_exc = None

        safe_retry = method.upper() in ("GET",)
        for attempt in range(retries + 1):
            try:
                self._gate()
                response = self.session.request(
                    method.upper(),
                    url,
                    params=params,
                    timeout=REQUEST_TIMEOUT,
                )
                return self._handle_response(response)
            except RateLimitError:
                raise
            except TemporaryBinanceError as exc:
                last_exc = exc
                if not safe_retry or attempt >= retries:
                    raise
                time.sleep(1.5 * (attempt + 1))
            except requests.RequestException as exc:
                last_exc = exc
                if not safe_retry or attempt >= retries:
                    raise TemporaryBinanceError(0, f"Network error: {exc}")
                time.sleep(1.0 * (attempt + 1))

        raise last_exc or RuntimeError("Unknown Binance request failure")

    def public_get(self, path, params=None):
        return self._request("GET", path, params, signed=False)

    def signed_request(self, method, path, params=None):
        return self._request(method, path, params, signed=True)

    def sync_server_time(self):
        global BINANCE_TIME_OFFSET
        data = self.public_get("/fapi/v1/time")
        BINANCE_TIME_OFFSET = int(data["serverTime"]) - now_ms()

    def get_exchange_info(self):
        return self.public_get("/fapi/v1/exchangeInfo")

    def get_klines(self, symbol, interval, limit=150):
        return self.public_get("/fapi/v1/klines", {
            "symbol": symbol, "interval": interval, "limit": limit
        })

    def get_price(self, symbol):
        data = self.public_get("/fapi/v1/ticker/price", {"symbol": symbol})
        return float(data["price"])

    def get_funding(self, symbol):
        data = self.public_get("/fapi/v1/premiumIndex", {"symbol": symbol})
        return float(data.get("lastFundingRate", 0))

    def get_account(self):
        return self.signed_request("GET", "/fapi/v2/account")

    def get_equity(self):
        account = self.get_account()
        return float(account.get("totalMarginBalance", account.get("totalWalletBalance", 0)))

    def get_position_risk(self):
        return self.signed_request("GET", "/fapi/v2/positionRisk")

    def get_position_mode(self):
        data = self.signed_request("GET", "/fapi/v1/positionSide/dual")
        return bool(data.get("dualSidePosition", False))

    def get_open_orders(self, symbol=None):
        return self.signed_request("GET", "/fapi/v1/openOrders", {"symbol": symbol} if symbol else {})

    def get_open_algo_orders(self, symbol=None):
        params = {"symbol": symbol} if symbol else {}
        data = self.signed_request("GET", "/fapi/v1/openAlgoOrders", params)
        if isinstance(data, dict):
            return data.get("orders", [])
        return data if isinstance(data, list) else []

    def set_leverage(self, symbol, leverage):
        return self.signed_request("POST", "/fapi/v1/leverage", {
            "symbol": symbol, "leverage": leverage
        })

    def set_margin_type(self, symbol):
        try:
            return self.signed_request("POST", "/fapi/v1/marginType", {
                "symbol": symbol, "marginType": "ISOLATED"
            })
        except BinanceError as exc:
            if exc.code == -4046 or "-4046" in str(exc):
                return None
            raise

    def place_order(self, params):
        return self.signed_request("POST", "/fapi/v1/order", params)

    def place_algo_order(self, params):
        params = dict(params)
        params.setdefault("algoType", "CONDITIONAL")
        return self.signed_request("POST", "/fapi/v1/algoOrder", params)

    def cancel_order(self, symbol, order_id=None, client_order_id=None):
        params = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = order_id
        elif client_order_id:
            params["origClientOrderId"] = client_order_id
        else:
            raise ValueError("order_id or client_order_id required")
        return self.signed_request("DELETE", "/fapi/v1/order", params)

    def cancel_algo_order(self, symbol, algo_id=None, client_algo_id=None):
        params = {"symbol": symbol}
        if algo_id is not None:
            params["algoId"] = algo_id
        elif client_algo_id:
            params["clientAlgoId"] = client_algo_id
        else:
            raise ValueError("algo_id or client_algo_id required")
        return self.signed_request("DELETE", "/fapi/v1/algoOrder", params)

    def cancel_all_orders(self, symbol):
        return self.signed_request("DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol})

    def cancel_all_algo_orders(self, symbol):
        return self.signed_request("DELETE", "/fapi/v1/algoOpenOrders", {"symbol": symbol})

BINANCE = BinanceClient()

# ============================================================
# EXCHANGE FILTERS
# ============================================================

def load_exchange_symbols():
    global EXCHANGE_SYMBOLS, VALID_SYMBOLS
    info = BINANCE.get_exchange_info()
    result = {}
    for item in info.get("symbols", []):
        symbol = item.get("symbol")
        if not symbol or item.get("status") != "TRADING" or item.get("quoteAsset") != "USDT":
            continue
        filters = {f.get("filterType"): f for f in item.get("filters", [])}
        pf = filters.get("PRICE_FILTER", {})
        lf = filters.get("LOT_SIZE", {})
        nf = filters.get("NOTIONAL") or filters.get("MIN_NOTIONAL") or {}
        result[symbol] = {
            "tick_size": float(pf.get("tickSize", 0) or 0),
            "step_size": float(lf.get("stepSize", 0) or 0),
            "min_qty": float(lf.get("minQty", 0) or 0),
            "max_qty": float(lf.get("maxQty", 0) or 0),
            "min_notional": float(nf.get("minNotional", 0) or 0),
        }
    EXCHANGE_SYMBOLS = result
    VALID_SYMBOLS = [s for s in PREFERRED_SYMBOLS if s in result]
    logger.info("Loaded %d tradable symbols", len(VALID_SYMBOLS))

def floor_to_step(value, step):
    if step <= 0:
        return float(value)
    v = Decimal(str(value))
    s = Decimal(str(step))
    return float((v / s).to_integral_value(rounding=ROUND_DOWN) * s)

def round_price(symbol, price, direction="down"):
    step = EXCHANGE_SYMBOLS[symbol]["tick_size"]
    if step <= 0:
        return float(price)
    v = Decimal(str(price))
    s = Decimal(str(step))
    rounding = ROUND_UP if direction == "up" else ROUND_DOWN
    return float((v / s).to_integral_value(rounding=rounding) * s)

def round_qty(symbol, qty):
    return floor_to_step(qty, EXCHANGE_SYMBOLS[symbol]["step_size"])

# ============================================================
# INDICATORS
# ============================================================

def ema(values, period):
    if len(values) < period:
        return []
    alpha = 2.0 / (period + 1.0)
    out = [float(values[0])]
    for x in values[1:]:
        out.append(alpha * float(x) + (1 - alpha) * out[-1])
    return out

def rsi(values, period=14):
    if len(values) <= period:
        return []
    gains, losses = [], []
    for i in range(1, len(values)):
        d = values[i] - values[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    out = [50.0] * period
    for i in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period
        if avg_loss == 0:
            value = 100.0
        else:
            rs = avg_gain / avg_loss
            value = 100.0 - 100.0 / (1.0 + rs)
        out.append(value)
    return out

def atr(highs, lows, closes, period=14):
    if len(closes) <= period:
        return []
    tr = []
    for i in range(1, len(closes)):
        tr.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        ))
    if len(tr) < period:
        return []
    current = sum(tr[:period]) / period
    out = [current]
    for i in range(period, len(tr)):
        current = ((current * (period - 1)) + tr[i]) / period
        out.append(current)
    return out

def adx(highs, lows, closes, period=14):
    if len(closes) < period * 2 + 2:
        return []
    trs, plus_dm, minus_dm = [], [], []
    for i in range(1, len(closes)):
        high, low = highs[i], lows[i]
        ph, pl, pc = highs[i - 1], lows[i - 1], closes[i - 1]
        trs.append(max(high - low, abs(high - pc), abs(low - pc)))
        up, down = high - ph, pl - low
        plus_dm.append(up if up > down and up > 0 else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)
    if len(trs) < period:
        return []
    a = sum(trs[:period]) / period
    p = sum(plus_dm[:period]) / period
    m = sum(minus_dm[:period]) / period
    dx = []
    for i in range(period, len(trs)):
        a = ((a * (period - 1)) + trs[i]) / period
        p = ((p * (period - 1)) + plus_dm[i]) / period
        m = ((m * (period - 1)) + minus_dm[i]) / period
        if a == 0:
            dx.append(0.0)
            continue
        pdi = 100 * p / a
        mdi = 100 * m / a
        denom = pdi + mdi
        dx.append(0.0 if denom == 0 else 100 * abs(pdi - mdi) / denom)
    if len(dx) < period:
        return []
    value = sum(dx[:period]) / period
    out = [value]
    for x in dx[period:]:
        value = ((value * (period - 1)) + x) / period
        out.append(value)
    return out

# ============================================================
# MARKET DATA / SIGNAL
# ============================================================

def parse_klines(raw):
    now = now_ms()
    return [
        {
            "open_time": int(r[0]),
            "open": float(r[1]),
            "high": float(r[2]),
            "low": float(r[3]),
            "close": float(r[4]),
            "volume": float(r[5]),
            "close_time": int(r[6]),
        }
        for r in raw
        if int(r[6]) < now
    ]

def get_market_snapshot(symbol):
    raw15 = BINANCE.get_klines(symbol, "15m", KLINE_LIMIT)
    raw1h = BINANCE.get_klines(symbol, "1h", KLINE_LIMIT)
    c15, c1h = parse_klines(raw15), parse_klines(raw1h)
    if len(c15) < 80 or len(c1h) < 80:
        return None
    return c15, c1h

def calculate_signal(symbol):
    # A short cache prevents Telegram /signals from multiplying REST load.
    with SIGNAL_CACHE_LOCK:
        cached = SIGNAL_CACHE.get(symbol)
    if cached and time.time() - cached[0] < 25:
        return cached[1]

    snapshot = get_market_snapshot(symbol)
    if not snapshot:
        return None
    c15, c1h = snapshot
    close = [x["close"] for x in c15]
    high = [x["high"] for x in c15]
    low = [x["low"] for x in c15]
    volume = [x["volume"] for x in c15]
    close1h = [x["close"] for x in c1h]

    e20 = ema(close, 20)
    e50 = ema(close, 50)
    e20h = ema(close1h, 20)
    e50h = ema(close1h, 50)
    rsi_v = rsi(close, 14)
    atr_v = atr(high, low, close, 14)
    adx_v = adx(high, low, close, 14)
    if not all([e20, e50, e20h, e50h, rsi_v, atr_v, adx_v]):
        return None

    price = close[-1]
    current_atr = atr_v[-1]
    current_adx = adx_v[-1]
    atr_pct = current_atr / price if price else 0
    if not MIN_ATR_PCT <= atr_pct <= MAX_ATR_PCT:
        return None
    if current_adx < MIN_ADX:
        return None

    avg_vol = sum(volume[-21:-1]) / 20 if len(volume) >= 21 else 0
    volume_ratio = volume[-1] / avg_vol if avg_vol > 0 else 0
    if volume_ratio < MIN_VOLUME_RATIO:
        return None

    funding = BINANCE.get_funding(symbol)
    last, prev = c15[-1], c15[-2]
    long_score = 0.0
    short_score = 0.0

    if e20[-1] > e50[-1]:
        long_score += 2
    if e20[-1] < e50[-1]:
        short_score += 2
    if e20h[-1] > e50h[-1]:
        long_score += 2
    if e20h[-1] < e50h[-1]:
        short_score += 2

    if 52 <= rsi_v[-1] <= 68:
        long_score += 1
    if 32 <= rsi_v[-1] <= 48:
        short_score += 1

    if last["close"] > prev["high"] * 0.999 and last["close"] > last["open"]:
        long_score += 1
    if last["close"] < prev["low"] * 1.001 and last["close"] < last["open"]:
        short_score += 1

    if volume_ratio >= 1.15:
        long_score += 1
        short_score += 1

    if funding > 0.0008:
        long_score -= 2
        short_score += 1
    elif funding < -0.0008:
        short_score -= 2
        long_score += 1

    if long_score < MIN_SCORE and short_score < MIN_SCORE:
        return None
    if abs(long_score - short_score) < 1:
        return None

    if long_score > short_score:
        side, score = "BUY", long_score
    else:
        side, score = "SELL", short_score

    stop_pct = max(MIN_STOP_PCT, min(MAX_STOP_PCT, ATR_STOP_MULT * atr_pct))
    if side == "BUY":
        stop = price * (1 - stop_pct)
        tp = price * (1 + stop_pct * REWARD_R)
    else:
        stop = price * (1 + stop_pct)
        tp = price * (1 - stop_pct * REWARD_R)

    signal = {
        "symbol": symbol, "side": side, "score": score, "price": price,
        "stop": stop, "tp": tp, "stop_pct": stop_pct, "atr": current_atr,
        "atr_pct": atr_pct, "adx": current_adx, "rsi": rsi_v[-1],
        "volume_ratio": volume_ratio, "funding": funding,
        "candle_time": last["close_time"],
    }
    with SIGNAL_CACHE_LOCK:
        SIGNAL_CACHE[symbol] = (time.time(), signal)
    return signal

def find_best_signal():
    candidates = []
    for symbol in VALID_SYMBOLS:
        try:
            signal = calculate_signal(symbol)
            if signal:
                candidates.append(signal)
        except RateLimitError:
            raise
        except Exception as exc:
            logger.warning("Signal error %s: %s", symbol, exc)
    candidates.sort(key=lambda x: (x["score"], x["adx"], x["volume_ratio"]), reverse=True)
    return candidates[0] if candidates else None

# ============================================================
# ACCOUNT / RISK
# ============================================================

def get_positions():
    result = {}
    for p in BINANCE.get_position_risk():
        try:
            qty = float(p.get("positionAmt", 0))
            if abs(qty) <= 0:
                continue
            result[p["symbol"]] = {
                "qty": qty,
                "entry": float(p.get("entryPrice", 0)),
                "mark": float(p.get("markPrice", 0)),
                "unrealized": float(p.get("unRealizedProfit", 0)),
                "leverage": int(float(p.get("leverage", LEVERAGE))),
            }
        except Exception:
            continue
    return result

def current_margin_used(positions):
    equity = BINANCE.get_equity()
    if equity <= 0:
        return 1.0
    margin = sum(abs(p["qty"] * p["entry"]) / max(p["leverage"], 1) for p in positions.values())
    return margin / equity

def get_equity():
    if not LIVE_TRADING:
        value = get_state("paper_equity")
        if value is None:
            value = PAPER_START_EQUITY
            set_state("paper_equity", value)
        return float(value)
    return BINANCE.get_equity()

def daily_locked():
    equity = get_equity()
    if equity <= 0:
        return True
    key = get_day_key()
    saved_day = get_state("risk_day")
    start = get_state("risk_start_equity")
    if saved_day != key or not start:
        set_state("risk_day", key)
        set_state("risk_start_equity", equity)
        set_state("daily_lock_notified", "")
        return False
    dd = 1 - equity / max(float(start), 1e-9)
    return dd >= MAX_DAILY_DRAWDOWN

def calculate_quantity(symbol, entry, stop):
    equity = get_equity()
    risk_usdt = equity * RISK_PER_TRADE
    stop_distance = abs(entry - stop)
    if stop_distance <= 0:
        return None
    notional_by_risk = risk_usdt * entry / stop_distance
    max_notional = equity * MAX_MARGIN_PER_TRADE * LEVERAGE
    notional = min(notional_by_risk, max_notional)
    info = EXCHANGE_SYMBOLS[symbol]
    min_notional = info["min_notional"]
    if min_notional > 0 and notional < min_notional:
        notional = min_notional
    qty = round_qty(symbol, notional / entry)
    if qty <= 0:
        return None
    if qty < info["min_qty"] or (info["max_qty"] > 0 and qty > info["max_qty"]):
        return None
    if qty * entry < min_notional:
        return None
    return {"qty": qty, "notional": qty * entry, "risk": stop_distance * qty}

def can_enter(symbol, positions, equity):
    if BOT_PAUSED or EMERGENCY_STOP or rate_limit_active():
        return False
    if symbol in positions:
        return False
    if db_get_open_trade(symbol):
        return False
    if len(positions) >= MAX_OPEN_POSITIONS:
        return False
    margin = sum(abs(p["qty"] * p["entry"]) / max(p["leverage"], 1) for p in positions.values())
    if equity <= 0 or margin / equity >= MAX_TOTAL_MARGIN:
        return False
    now = time.time()
    last = float(get_state(f"last_entry_{symbol}", "0"))
    global_last = float(get_state("last_global_entry", "0"))
    if now - last < COOLDOWN_MINUTES * 60:
        return False
    if now - global_last < GLOBAL_ENTRY_COOLDOWN_MINUTES * 60:
        return False
    return True

# ============================================================
# ORDER / PROTECTION
# ============================================================

def unique_id(prefix):
    return f"{prefix}{uuid.uuid4().hex[:18]}"

def set_margin_and_leverage(symbol):
    if not LIVE_TRADING:
        return True
    BINANCE.set_margin_type(symbol)
    BINANCE.set_leverage(symbol, LEVERAGE)
    return True

def get_position(symbol):
    return get_positions().get(symbol)

def cancel_our_protection(symbol):
    # Algo orders are separate from normal openOrders since Binance's migration.
    try:
        for o in BINANCE.get_open_algo_orders(symbol):
            cid = str(o.get("clientAlgoId", ""))
            if cid.startswith(("SNPRSL_", "SNPRTP_")):
                try:
                    BINANCE.cancel_algo_order(symbol, algo_id=o.get("algoId"), client_algo_id=cid)
                except BinanceError as exc:
                    if exc.code not in (-2011, -2013):
                        raise
    except RateLimitError:
        raise

    try:
        for o in BINANCE.get_open_orders(symbol):
            cid = str(o.get("clientOrderId", ""))
            if cid.startswith(("SNPR_",)):
                try:
                    BINANCE.cancel_order(symbol, order_id=o.get("orderId"))
                except BinanceError as exc:
                    if exc.code not in (-2011, -2013):
                        raise
    except RateLimitError:
        raise

def place_protection(symbol, position, stop_price, tp_price):
    if not LIVE_TRADING:
        return True

    cancel_our_protection(symbol)
    qty = abs(position["qty"])
    if qty <= 0:
        return False

    side = "SELL" if position["qty"] > 0 else "BUY"

    # Direction-aware rounding avoids placing a trigger on the wrong side of price.
    stop_direction = "down" if side == "SELL" else "up"
    tp_direction = "up" if side == "SELL" else "down"
    stop_trigger = round_price(symbol, stop_price, stop_direction)
    tp_trigger = round_price(symbol, tp_price, tp_direction)

    # Final sanity check against current mark price.
    mark = position.get("mark") or BINANCE.get_price(symbol)
    if side == "SELL" and not (stop_trigger < mark and tp_trigger > mark):
        logger.error("Invalid LONG protection %s stop=%s mark=%s tp=%s", symbol, stop_trigger, mark, tp_trigger)
        return False
    if side == "BUY" and not (stop_trigger > mark and tp_trigger < mark):
        logger.error("Invalid SHORT protection %s stop=%s mark=%s tp=%s", symbol, stop_trigger, mark, tp_trigger)
        return False

    sl = BINANCE.place_algo_order({
        "symbol": symbol,
        "side": side,
        "type": "STOP_MARKET",
        "positionSide": "BOTH",
        "triggerPrice": str(stop_trigger),
        "closePosition": "true",
        "workingType": "MARK_PRICE",
        "priceProtect": "false",
        "clientAlgoId": unique_id("SNPRSL_"),
        "newOrderRespType": "ACK",
    })

    try:
        tp = BINANCE.place_algo_order({
            "symbol": symbol,
            "side": side,
            "type": "TAKE_PROFIT_MARKET",
            "positionSide": "BOTH",
            "triggerPrice": str(tp_trigger),
            "closePosition": "true",
            "workingType": "MARK_PRICE",
            "priceProtect": "false",
            "clientAlgoId": unique_id("SNPRTP_"),
            "newOrderRespType": "ACK",
        })
    except Exception:
        try:
            if sl.get("algoId"):
                BINANCE.cancel_algo_order(symbol, algo_id=sl["algoId"])
        finally:
            raise
    return bool(sl and tp)

def emergency_close_symbol(symbol):
    if not LIVE_TRADING:
        return
    try:
        p = get_position(symbol)
        if not p:
            return
        qty = round_qty(symbol, abs(p["qty"]))
        if qty <= 0:
            return
        side = "SELL" if p["qty"] > 0 else "BUY"
        BINANCE.place_order({
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": qty,
            "reduceOnly": "true",
            "newOrderRespType": "RESULT",
        })
        try:
            BINANCE.cancel_all_orders(symbol)
        except Exception:
            pass
        try:
            BINANCE.cancel_all_algo_orders(symbol)
        except Exception:
            pass
    except Exception as exc:
        logger.error("Emergency close failed %s: %s", symbol, exc)

def open_live_trade(signal):
    symbol, side = signal["symbol"], signal["side"]
    if rate_limit_active():
        return False

    positions = get_positions()
    if symbol in positions or db_get_open_trade(symbol):
        return False

    qty_data = calculate_quantity(symbol, signal["price"], signal["stop"])
    if not qty_data:
        logger.info("Skip %s: quantity invalid", symbol)
        return False

    set_margin_and_leverage(symbol)
    try:
        order = BINANCE.place_order({
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": qty_data["qty"],
            "newOrderRespType": "RESULT",
            "newClientOrderId": unique_id("SNPRIN_"),
        })
    except TemporaryBinanceError:
        # A timeout after a MARKET order is ambiguous. Never blindly retry an
        # entry, because the first request may already have filled.
        position = get_position(symbol)
        if not position:
            raise
        order = {"orderId": "RECOVERED_AFTER_NETWORK_ERROR"}

    if not order or not order.get("orderId"):
        raise RuntimeError(f"{symbol}: entry order rejected")

    # Confirm actual position and actual entry. Never build protection from ticker price alone.
    position = None
    for _ in range(5):
        time.sleep(0.4)
        position = get_position(symbol)
        if position:
            break
    if not position:
        raise RuntimeError(f"{symbol}: entry acknowledged but position not visible")

    entry = position["entry"]
    stop_pct = signal["stop_pct"]
    if position["qty"] > 0:
        stop = entry * (1 - stop_pct)
        tp = entry * (1 + stop_pct * REWARD_R)
    else:
        stop = entry * (1 + stop_pct)
        tp = entry * (1 - stop_pct * REWARD_R)

    try:
        place_protection(symbol, position, stop, tp)
    except Exception:
        telegram_send(f"🚨 <b>{symbol}</b>: protection installation failed. Emergency close requested.")
        emergency_close_symbol(symbol)
        raise

    db_open_trade(
        symbol=symbol,
        side="LONG" if position["qty"] > 0 else "SHORT",
        mode="LIVE",
        entry=entry,
        stop=stop,
        tp=tp,
        qty=abs(position["qty"]),
        notional=abs(position["qty"]) * entry,
        risk_usdt=abs(entry - stop) * abs(position["qty"]),
    )
    set_state("last_global_entry", time.time())
    set_state(f"last_entry_{symbol}", time.time())

    telegram_send(
        f"⚡ <b>ENTRY</b>\n"
        f"{symbol} <b>{side}</b>\n"
        f"Entry: <code>{entry:.8g}</code>\n"
        f"SL: <code>{stop:.8g}</code>\n"
        f"TP: <code>{tp:.8g}</code>\n"
        f"Score: <b>{signal['score']:.1f}</b>\n"
        f"RSI: {signal['rsi']:.1f} | ADX: {signal['adx']:.1f}\n"
        f"ATR: {signal['atr_pct']*100:.2f}% | Vol: {signal['volume_ratio']:.2f}x"
    )
    return True

# ============================================================
# POSITION MANAGEMENT
# ============================================================

def update_live_protection(row, position):
    entry = float(position["entry"])
    mark = float(position["mark"])
    risk_distance = abs(float(row["entry"]) - float(row["stop"]))
    if risk_distance <= 0:
        return

    if row["side"] == "LONG":
        r = (mark - entry) / risk_distance
        if r < BE_TRIGGER_R:
            return
        if r >= TRAIL_TRIGGER_R:
            atr15 = atr_from_symbol(row["symbol"])
            new_stop = max(entry * (1 + BE_LOCK_PCT), mark - atr15 * TRAIL_ATR_MULT)
        else:
            new_stop = entry * (1 + BE_LOCK_PCT)
        tp = float(row["tp"])
        if new_stop <= float(row["stop"]):
            return
    else:
        r = (entry - mark) / risk_distance
        if r < BE_TRIGGER_R:
            return
        if r >= TRAIL_TRIGGER_R:
            atr15 = atr_from_symbol(row["symbol"])
            new_stop = min(entry * (1 - BE_LOCK_PCT), mark + atr15 * TRAIL_ATR_MULT)
        else:
            new_stop = entry * (1 - BE_LOCK_PCT)
        tp = float(row["tp"])
        if new_stop >= float(row["stop"]):
            return

    # Never loosen the stop. Replace protection atomically enough for this REST architecture:
    # cancel old algo orders, then create new SL/TP; if replacement fails, old protection may
    # already be gone, so immediately attempt emergency close.
    try:
        place_protection(row["symbol"], position, new_stop, tp)
        conn = db_connect()
        conn.execute("UPDATE trades SET stop=? WHERE id=?", (new_stop, row["id"]))
        conn.commit()
        conn.close()
    except Exception:
        emergency_close_symbol(row["symbol"])
        raise

def atr_from_symbol(symbol):
    raw = BINANCE.get_klines(symbol, "15m", 70)
    candles = parse_klines(raw)
    if len(candles) < 20:
        return 0.0
    highs = [x["high"] for x in candles]
    lows = [x["low"] for x in candles]
    closes = [x["close"] for x in candles]
    vals = atr(highs, lows, closes, 14)
    return vals[-1] if vals else 0.0

def reconcile_live_positions():
    positions = get_positions()
    rows = {row["symbol"]: row for row in db_get_open_trades() if row["mode"] == "LIVE"}

    # If Binance has a position that DB doesn't know about, do not trade it blindly.
    # Install protection only if we have enough state to reconstruct it; otherwise alert.
    unknown_symbols = []
    for symbol, p in positions.items():
        row = rows.get(symbol)
        if not row:
            unknown_symbols.append(symbol)
            continue
        try:
            open_algo = BINANCE.get_open_algo_orders(symbol)
            ours = [o for o in open_algo if str(o.get("clientAlgoId","")).startswith(("SNPRSL_","SNPRTP_"))]
            if len(ours) < 2:
                place_protection(symbol, p, row["stop"], row["tp"])
            update_live_protection(row, p)
        except RateLimitError:
            raise
        except Exception as exc:
            logger.error("Live protection error %s: %s", symbol, exc)
            telegram_send(f"🚨 Protection manager error: <b>{symbol}</b>\n<code>{str(exc)[:500]}</code>")

    # Positions that disappeared from Binance are closed. Reconcile DB rows so
    # stats/cooldowns cannot get stuck forever. Exact realized PnL is finalized
    # from the current ticker as a conservative fallback.
    for symbol, row in rows.items():
        if symbol in positions:
            continue
        try:
            exit_price = BINANCE.get_price(symbol)
            if row["side"] == "LONG":
                pnl = (exit_price - row["entry"]) * row["qty"]
            else:
                pnl = (row["entry"] - exit_price) * row["qty"]
            fee_rate = 0.0005
            fees = (row["entry"] + exit_price) * row["qty"] * fee_rate
            db_close_trade(row["id"], exit_price, pnl - fees, "EXTERNAL/PROTECTION CLOSE")
        except RateLimitError:
            raise
        except Exception as exc:
            logger.warning("Closed-trade reconciliation failed %s: %s", symbol, exc)

    unknown_value = ",".join(sorted(unknown_symbols))
    set_state("unknown_live_positions", unknown_value)
    if unknown_symbols:
        telegram_send(
            "⚠️ <b>UNKNOWN LIVE POSITION</b>\n"
            + ", ".join(sorted(unknown_symbols))
            + "\nNew entries are blocked until reviewed."
        )
    return positions

# ============================================================
# PAPER
# ============================================================

def paper_open_trade(signal):
    q = calculate_quantity(signal["symbol"], signal["price"], signal["stop"])
    if not q:
        return False
    trade_id = db_open_trade(
        signal["symbol"], "LONG" if signal["side"] == "BUY" else "SHORT", "PAPER",
        signal["price"], signal["stop"], signal["tp"],
        q["qty"], q["notional"], q["risk"]
    )
    set_state("last_global_entry", time.time())
    set_state(f"last_entry_{signal['symbol']}", time.time())
    telegram_send(
        f"🧪 <b>PAPER ENTRY</b>\n{signal['symbol']} {signal['side']}\n"
        f"Entry: {signal['price']:.8g}\nSL: {signal['stop']:.8g}\nTP: {signal['tp']:.8g}"
    )
    return trade_id

def close_paper_trade(row, exit_price, reason):
    if row["side"] == "LONG":
        gross = (exit_price - row["entry"]) * row["qty"]
    else:
        gross = (row["entry"] - exit_price) * row["qty"]
    fee_rate = 0.0005
    fees = (row["entry"] + exit_price) * row["qty"] * fee_rate
    pnl = gross - fees
    equity = get_equity() + pnl
    set_state("paper_equity", equity)
    db_close_trade(row["id"], exit_price, pnl, reason)
    telegram_send(
        f"🔔 <b>PAPER CLOSED</b>\n{row['symbol']} {row['side']}\n"
        f"PnL: <b>{pnl:+.2f} USDT</b>\nEquity: <b>{equity:.2f}</b>\n{reason}"
    )

def manage_paper_positions():
    for row in db_get_open_trades():
        if row["mode"] != "PAPER":
            continue
        try:
            price = BINANCE.get_price(row["symbol"])
            if row["side"] == "LONG":
                if price <= row["stop"]:
                    close_paper_trade(row, row["stop"], "STOP LOSS")
                elif price >= row["tp"]:
                    close_paper_trade(row, row["tp"], "TAKE PROFIT")
            else:
                if price >= row["stop"]:
                    close_paper_trade(row, row["stop"], "STOP LOSS")
                elif price <= row["tp"]:
                    close_paper_trade(row, row["tp"], "TAKE PROFIT")
        except RateLimitError:
            raise
        except Exception as exc:
            logger.error("Paper position error: %s", exc)

# ============================================================
# TRADING LOOP
# ============================================================

def trading_loop():
    global BOT_PAUSED, EMERGENCY_STOP
    time.sleep(2)

    try:
        init_db()
        BINANCE.sync_server_time()
        load_exchange_symbols()
        if LIVE_TRADING and BINANCE.get_position_mode():
            raise RuntimeError("Hedge Mode is enabled. This bot requires One-way Mode (positionSide=BOTH).")
    except Exception as exc:
        logger.exception("Startup failed")
        telegram_send(f"🚨 <b>BOT START FAILED</b>\n<code>{str(exc)[:800]}</code>")
        return

    if LIVE_TRADING and (not BINANCE_API_KEY or not BINANCE_SECRET_KEY):
        telegram_send("🚨 LIVE_TRADING=true but Binance API credentials are missing. Trading thread stopped.")
        return

    set_state("bot_paused", get_state("bot_paused", "0"))
    set_state("emergency_stop", get_state("emergency_stop", "0"))
    BOT_PAUSED = get_state("bot_paused", "0") == "1"
    EMERGENCY_STOP = get_state("emergency_stop", "0") == "1"

    telegram_send(
        f"🤖 <b>BOT ONLINE</b>\n"
        f"Mode: <b>{'LIVE' if LIVE_TRADING else 'PAPER'}</b>\n"
        f"Symbols: {len(VALID_SYMBOLS)}\n"
        f"Risk/trade: {RISK_PER_TRADE*100:.2f}%\n"
        f"Max positions: {MAX_OPEN_POSITIONS}\n"
        f"Leverage: {LEVERAGE}x"
    )

    while not STOP_EVENT.is_set():
        cycle_start = time.time()
        try:
            if rate_limit_active():
                wait = max(1, rate_limit_remaining())
                logger.warning("Rate-limit cooldown: %ss", wait)
                STOP_EVENT.wait(min(wait, SCAN_INTERVAL_SECONDS))
                continue

            if LIVE_TRADING:
                positions = reconcile_live_positions()
            else:
                manage_paper_positions()
                positions = {}

            if BOT_PAUSED or EMERGENCY_STOP:
                STOP_EVENT.wait(SCAN_INTERVAL_SECONDS)
                continue

            if daily_locked():
                STOP_EVENT.wait(SCAN_INTERVAL_SECONDS)
                continue

            equity = get_equity()
            if equity <= 0:
                STOP_EVENT.wait(SCAN_INTERVAL_SECONDS)
                continue

            if len(positions) >= MAX_OPEN_POSITIONS:
                STOP_EVENT.wait(SCAN_INTERVAL_SECONDS)
                continue

            if LIVE_TRADING and get_state("unknown_live_positions", ""):
                STOP_EVENT.wait(SCAN_INTERVAL_SECONDS)
                continue

            margin = sum(
                abs(p["qty"] * p["entry"]) / max(p["leverage"], 1)
                for p in positions.values()
            )
            if margin / equity >= MAX_TOTAL_MARGIN:
                STOP_EVENT.wait(SCAN_INTERVAL_SECONDS)
                continue

            signal = find_best_signal()
            if signal and can_enter(signal["symbol"], positions, equity):
                if LIVE_TRADING:
                    open_live_trade(signal)
                else:
                    paper_open_trade(signal)

        except RateLimitError as exc:
            activate_rate_limit(RATE_LIMIT_COOLDOWN_SECONDS)
            logger.error("BINANCE RATE LIMIT: %s", exc)
            telegram_send(
                f"🛑 <b>BINANCE RATE LIMIT</b>\n"
                f"New entries stopped for {RATE_LIMIT_COOLDOWN_SECONDS}s.\n"
                f"<code>{str(exc)[:500]}</code>"
            )
            STOP_EVENT.wait(min(RATE_LIMIT_COOLDOWN_SECONDS, 120))
        except Exception as exc:
            logger.exception("Main trading loop error")
            telegram_send(f"⚠️ <b>ENGINE ERROR</b>\n<code>{str(exc)[:700]}</code>")
            STOP_EVENT.wait(15)

        elapsed = time.time() - cycle_start
        STOP_EVENT.wait(max(1, SCAN_INTERVAL_SECONDS - int(elapsed)))

# ============================================================
# STATUS / TELEGRAM COMMANDS
# ============================================================

def status_text():
    mode = "🔴 LIVE" if LIVE_TRADING else "🧪 PAPER"
    state = "PAUSED" if BOT_PAUSED else "RUNNING"
    if EMERGENCY_STOP:
        state = "EMERGENCY STOP"
    try:
        equity = get_equity()
        locked = daily_locked()
        positions = get_positions() if LIVE_TRADING else {}
        margin = sum(abs(p["qty"] * p["entry"]) / max(p["leverage"],1) for p in positions.values())
        return (
            f"🤖 <b>BOT STATUS</b>\n\n"
            f"Mode: <b>{mode}</b>\n"
            f"Engine: <b>{state}</b>\n"
            f"Equity: <b>{equity:.2f} USDT</b>\n"
            f"Margin: <b>{margin:.2f} USDT</b>\n"
            f"Positions: <b>{len(positions)}</b>\n"
            f"Daily lock: <b>{'YES' if locked else 'NO'}</b>\n"
            f"Rate limit: <b>{rate_limit_remaining()}s</b>"
        )
    except Exception as exc:
        return f"🤖 <b>BOT STATUS</b>\n\n⚠️ <code>{str(exc)[:700]}</code>"

async def start_command(update, context):
    if not authorized(update):
        await deny(update); return
    await update.message.reply_text(status_text(), parse_mode=ParseMode.HTML, reply_markup=main_keyboard())

async def status_command(update, context):
    if not authorized(update):
        await deny(update); return
    await update.message.reply_text(status_text(), parse_mode=ParseMode.HTML, reply_markup=main_keyboard())

async def positions_command(update, context):
    if not authorized(update):
        await deny(update); return
    if LIVE_TRADING:
        positions = get_positions()
        if not positions:
            await update.message.reply_text("📭 No live positions.")
            return
        lines = ["📈 <b>LIVE POSITIONS</b>\n"]
        for s,p in positions.items():
            lines.append(f"<b>{s}</b> {'LONG' if p['qty']>0 else 'SHORT'}\nEntry {p['entry']:.8g} | Mark {p['mark']:.8g}\nPnL {p['unrealized']:+.4f}\n")
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)
    else:
        rows = [r for r in db_get_open_trades() if r["mode"]=="PAPER"]
        if not rows:
            await update.message.reply_text("📭 No paper positions.")
            return
        await update.message.reply_text(
            "\n".join(f"{r['symbol']} {r['side']} | Entry {r['entry']:.8g} | SL {r['stop']:.8g} | TP {r['tp']:.8g}" for r in rows)
        )

async def stats_command(update, context):
    if not authorized(update):
        await deny(update); return
    s = db_stats()
    await update.message.reply_text(
        f"📋 <b>STATS</b>\n\nTrades: <b>{s['total']}</b>\n"
        f"Wins: <b>{s['wins']}</b>\nLosses: <b>{s['losses']}</b>\n"
        f"Win rate: <b>{s['win_rate']:.1f}%</b>\n"
        f"Net PnL: <b>{s['pnl']:+.2f}</b>\n"
        f"Avg win: <b>{s['avg_win']:+.2f}</b>\nAvg loss: <b>{s['avg_loss']:+.2f}</b>",
        parse_mode=ParseMode.HTML
    )

async def signals_command(update, context):
    if not authorized(update):
        await deny(update); return
    if rate_limit_active():
        await update.message.reply_text(f"🛑 Binance cooldown active: {rate_limit_remaining()}s")
        return
    await update.message.reply_text("🔎 Scanning...")
    try:
        candidates = []
        for symbol in VALID_SYMBOLS:
            try:
                s = calculate_signal(symbol)
                if s:
                    candidates.append(s)
            except RateLimitError:
                raise
            except Exception:
                continue
        candidates.sort(key=lambda x:x["score"], reverse=True)
        if not candidates:
            await update.message.reply_text("No high-score signals.")
            return
        lines = ["🔎 <b>TOP SIGNALS</b>\n"]
        for s in candidates[:7]:
            lines.append(
                f"{s['symbol']} <b>{s['side']}</b> score <b>{s['score']:.1f}</b>\n"
                f"RSI {s['rsi']:.1f} | ADX {s['adx']:.1f} | Vol {s['volume_ratio']:.2f}x\n"
            )
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)
    except RateLimitError:
        activate_rate_limit()
        await update.message.reply_text("🛑 Binance rate limit hit. New entries are paused temporarily.")

async def pause_command(update, context):
    global BOT_PAUSED
    if not authorized(update):
        await deny(update); return
    BOT_PAUSED = True
    set_state("bot_paused","1")
    await update.message.reply_text("⏸ <b>BOT PAUSED</b>\nNo new trades will be opened.", parse_mode=ParseMode.HTML)

async def resume_command(update, context):
    global BOT_PAUSED, EMERGENCY_STOP
    if not authorized(update):
        await deny(update); return
    BOT_PAUSED = False
    EMERGENCY_STOP = False
    set_state("bot_paused","0")
    set_state("emergency_stop","0")
    await update.message.reply_text("▶️ <b>BOT RESUMED</b>", parse_mode=ParseMode.HTML)

async def mode_command(update, context):
    if not authorized(update):
        await deny(update); return
    await update.message.reply_text(
        f"Current mode: <b>{'LIVE' if LIVE_TRADING else 'PAPER'}</b>\n"
        f"LIVE_TRADING=<code>{str(LIVE_TRADING).lower()}</code>",
        parse_mode=ParseMode.HTML
    )

async def risk_command(update, context):
    if not authorized(update):
        await deny(update); return
    await update.message.reply_text(
        f"⚙️ <b>RISK</b>\n\nRisk/trade: {RISK_PER_TRADE*100:.2f}%\n"
        f"Max margin/trade: {MAX_MARGIN_PER_TRADE*100:.1f}%\n"
        f"Max total margin: {MAX_TOTAL_MARGIN*100:.1f}%\n"
        f"Max positions: {MAX_OPEN_POSITIONS}\nLeverage: {LEVERAGE}x\n"
        f"Daily DD lock: {MAX_DAILY_DRAWDOWN*100:.1f}%",
        parse_mode=ParseMode.HTML
    )

async def emergency_command(update, context):
    global BOT_PAUSED, EMERGENCY_STOP
    if not authorized(update):
        await deny(update); return
    BOT_PAUSED = True
    EMERGENCY_STOP = True
    set_state("bot_paused","1")
    set_state("emergency_stop","1")
    await update.message.reply_text(
        "🚨 <b>EMERGENCY STOP ACTIVE</b>\nNew trades disabled. Existing positions are NOT automatically closed.",
        parse_mode=ParseMode.HTML
    )

async def close_all_command(update, context):
    if not authorized(update):
        await deny(update); return
    await close_all_bot_positions()
    await update.message.reply_text("💥 Close-all executed.", reply_markup=main_keyboard())

async def close_all_bot_positions():
    global BOT_PAUSED
    BOT_PAUSED = True
    set_state("bot_paused","1")
    rows = db_get_open_trades()
    if not rows:
        return

    for row in rows:
        try:
            if row["mode"] == "PAPER":
                close_paper_trade(row, BINANCE.get_price(row["symbol"]), "MANUAL CLOSE ALL")
            else:
                p = get_position(row["symbol"])
                if p:
                    qty = round_qty(row["symbol"], abs(p["qty"]))
                    if qty > 0:
                        side = "SELL" if p["qty"] > 0 else "BUY"
                        BINANCE.place_order({
                            "symbol": row["symbol"], "side": side, "type": "MARKET",
                            "quantity": qty, "reduceOnly": "true", "newOrderRespType": "RESULT"
                        })
                try:
                    BINANCE.cancel_all_orders(row["symbol"])
                except Exception:
                    pass
                try:
                    BINANCE.cancel_all_algo_orders(row["symbol"])
                except Exception:
                    pass
                exit_price = BINANCE.get_price(row["symbol"])
                if row["side"] == "LONG":
                    gross = (exit_price - row["entry"]) * row["qty"]
                else:
                    gross = (row["entry"] - exit_price) * row["qty"]
                fees = (row["entry"] + exit_price) * row["qty"] * 0.0005
                db_close_trade(row["id"], exit_price, gross - fees, "MANUAL CLOSE ALL")
        except RateLimitError:
            raise
        except Exception as exc:
            logger.error("Close-all error %s: %s", row["symbol"], exc)

async def callback_handler(update, context):
    global BOT_PAUSED, EMERGENCY_STOP
    query = update.callback_query
    if not authorized(update):
        await deny(update); return
    await query.answer()
    action = query.data

    if action == "status":
        await query.edit_message_text(status_text(), parse_mode=ParseMode.HTML, reply_markup=main_keyboard())
    elif action == "positions":
        await query.edit_message_text("Use /positions for position details.", reply_markup=main_keyboard())
    elif action == "stats":
        s = db_stats()
        await query.edit_message_text(
            f"📋 Trades {s['total']} | Wins {s['wins']} | Losses {s['losses']}\n"
            f"Win rate {s['win_rate']:.1f}% | PnL {s['pnl']:+.2f}",
            reply_markup=main_keyboard()
        )
    elif action == "signals":
        await query.edit_message_text("Use /signals to scan. Scanning is rate-limited and cached.", reply_markup=main_keyboard())
    elif action == "pause":
        BOT_PAUSED = True
        set_state("bot_paused","1")
        await query.edit_message_text("⏸ <b>PAUSED</b>", parse_mode=ParseMode.HTML, reply_markup=main_keyboard())
    elif action == "resume":
        BOT_PAUSED = False
        EMERGENCY_STOP = False
        set_state("bot_paused","0")
        set_state("emergency_stop","0")
        await query.edit_message_text("▶️ <b>RESUMED</b>", parse_mode=ParseMode.HTML, reply_markup=main_keyboard())
    elif action == "emergency":
        BOT_PAUSED = True
        EMERGENCY_STOP = True
        set_state("bot_paused","1")
        set_state("emergency_stop","1")
        await query.edit_message_text("🚨 <b>EMERGENCY STOP</b>\nExisting positions are not auto-closed.", parse_mode=ParseMode.HTML, reply_markup=main_keyboard())
    elif action == "close_confirm":
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ YES, CLOSE ALL", callback_data="close_execute"),
            InlineKeyboardButton("↩️ Cancel", callback_data="cancel"),
        ]])
        await query.edit_message_text("⚠️ <b>Close all bot positions?</b>", parse_mode=ParseMode.HTML, reply_markup=kb)
    elif action == "close_execute":
        await close_all_bot_positions()
        await query.edit_message_text("💥 <b>CLOSE ALL EXECUTED</b>", parse_mode=ParseMode.HTML, reply_markup=main_keyboard())
    elif action == "cancel":
        await query.edit_message_text(status_text(), parse_mode=ParseMode.HTML, reply_markup=main_keyboard())

async def heartbeat_job(context):
    try:
        if daily_locked() and get_state("daily_lock_notified","") != get_day_key():
            telegram_send(
                f"🛑 <b>DAILY RISK LOCK</b>\n"
                f"Drawdown limit: {MAX_DAILY_DRAWDOWN*100:.1f}%\nNew entries disabled today."
            )
            set_state("daily_lock_notified", get_day_key())
    except RateLimitError:
        pass
    except Exception as exc:
        logger.error("Heartbeat error: %s", exc)

# ============================================================
# STARTUP
# ============================================================

def build_telegram_app():
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("positions", positions_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CommandHandler("signals", signals_command))
    application.add_handler(CommandHandler("pause", pause_command))
    application.add_handler(CommandHandler("resume", resume_command))
    application.add_handler(CommandHandler("mode", mode_command))
    application.add_handler(CommandHandler("risk", risk_command))
    application.add_handler(CommandHandler("emergency", emergency_command))
    application.add_handler(CommandHandler("closeall", close_all_command))
    application.add_handler(CallbackQueryHandler(callback_handler))
    application.job_queue.run_repeating(heartbeat_job, interval=300, first=30)
    return application

@app.get("/")
def health():
    return "Binance Futures bot is running.", 200

def run_telegram():
    global TELEGRAM_LOOP, TELEGRAM_APP
    TELEGRAM_LOOP = asyncio.new_event_loop()
    asyncio.set_event_loop(TELEGRAM_LOOP)
    TELEGRAM_APP = build_telegram_app()
    TELEGRAM_APP.run_polling(close_loop=False, stop_signals=None)

def start():
    global TRADING_THREAD
    init_db()

    if TELEGRAM_BOT_TOKEN:
        tg = threading.Thread(target=run_telegram, name="telegram", daemon=True)
        tg.start()

    TRADING_THREAD = threading.Thread(target=trading_loop, name="trading", daemon=True)
    TRADING_THREAD.start()

    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)

if __name__ == "__main__":
    start()
