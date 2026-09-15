import os
import time
import hmac
import hashlib
import logging
import sqlite3
import threading
import asyncio
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from datetime import datetime, timezone
from urllib.parse import urlencode

import requests

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)


# ============================================================
# CONFIG
# ============================================================

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY", "").strip()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# IMPORTANT:
# false = PAPER, no real orders
# true  = LIVE, real Binance Futures orders
LIVE_TRADING = os.getenv("LIVE_TRADING", "false").lower() == "true"

# Binance Futures
BINANCE_BASE_URL = os.getenv(
    "BINANCE_BASE_URL",
    "https://fapi.binance.com"
).rstrip("/")

# Paper account starting balance
PAPER_START_EQUITY = float(
    os.getenv("PAPER_START_EQUITY", "300")
)

# Trading
LEVERAGE = int(os.getenv("LEVERAGE", "5"))

RISK_PER_TRADE = float(
    os.getenv("RISK_PER_TRADE", "0.005")
)  # 0.5% of equity

MAX_MARGIN_PER_TRADE = float(
    os.getenv("MAX_MARGIN_PER_TRADE", "0.08")
)

MAX_TOTAL_MARGIN = float(
    os.getenv("MAX_TOTAL_MARGIN", "0.24")
)

MAX_OPEN_POSITIONS = int(
    os.getenv("MAX_OPEN_POSITIONS", "3")
)

MAX_DAILY_DRAWDOWN = float(
    os.getenv("MAX_DAILY_DRAWDOWN", "0.025")
)

COOLDOWN_MINUTES = int(
    os.getenv("COOLDOWN_MINUTES", "45")
)

GLOBAL_ENTRY_COOLDOWN_MINUTES = int(
    os.getenv("GLOBAL_ENTRY_COOLDOWN_MINUTES", "10")
)

# Strategy
MIN_SCORE = float(
    os.getenv("MIN_SCORE", "7")
)

MIN_ADX = float(
    os.getenv("MIN_ADX", "18")
)

MIN_VOLUME_RATIO = float(
    os.getenv("MIN_VOLUME_RATIO", "0.85")
)

MIN_ATR_PCT = float(
    os.getenv("MIN_ATR_PCT", "0.0025")
)

MAX_ATR_PCT = float(
    os.getenv("MAX_ATR_PCT", "0.04")
)

ATR_STOP_MULT = float(
    os.getenv("ATR_STOP_MULT", "1.35")
)

MIN_STOP_PCT = float(
    os.getenv("MIN_STOP_PCT", "0.006")
)

MAX_STOP_PCT = float(
    os.getenv("MAX_STOP_PCT", "0.022")
)

REWARD_R = float(
    os.getenv("REWARD_R", "2.20")
)

BE_TRIGGER_R = float(
    os.getenv("BE_TRIGGER_R", "1.0")
)

BE_LOCK_PCT = float(
    os.getenv("BE_LOCK_PCT", "0.0008")
)

TRAIL_TRIGGER_R = float(
    os.getenv("TRAIL_TRIGGER_R", "1.5")
)

TRAIL_ATR_MULT = float(
    os.getenv("TRAIL_ATR_MULT", "1.0")
)

# Scanner
SCAN_INTERVAL_SECONDS = int(
    os.getenv("SCAN_INTERVAL_SECONDS", "60")
)

KLINE_LIMIT = 150

# Telegram security
ALLOWED_CHAT_ID = str(TELEGRAM_CHAT_ID)

# Database
DB_PATH = os.getenv(
    "DB_PATH",
    "bot.db"
)

# Binance symbols
PREFERRED_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "AVAXUSDT",
    "LINKUSDT",
    "DOTUSDT",
    "UNIUSDT",
    "ATOMUSDT",
    "LTCUSDT",
    "NEARUSDT",
    "APTUSDT",
    "ARBUSDT",
    "OPUSDT",
    "INJUSDT",
    "SUIUSDT",
    "RENDERUSDT",
    "TIAUSDT",
    "SEIUSDT",
    "IMXUSDT",
    "PEPEUSDT",
]


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("sniper-bot")


# ============================================================
# GLOBAL STATE
# ============================================================

STOP_EVENT = threading.Event()
TRADING_THREAD = None

TELEGRAM_LOOP = None
TELEGRAM_APP = None

BOT_PAUSED = False
EMERGENCY_STOP = False

BINANCE_TIME_OFFSET = 0

EXCHANGE_SYMBOLS = {}
VALID_SYMBOLS = []

STATE_LOCK = threading.Lock()


# ============================================================
# DATABASE
# ============================================================

def db_connect():
    conn = sqlite3.connect(
        DB_PATH,
        timeout=30,
        check_same_thread=False
    )
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

    row = conn.execute(
        "SELECT value FROM state WHERE key = ?",
        (key,)
    ).fetchone()

    conn.close()

    if row is None:
        return default

    return row["value"]


def set_state(key, value):
    conn = db_connect()

    conn.execute(
        """
        INSERT INTO state(key, value)
        VALUES(?, ?)
        ON CONFLICT(key)
        DO UPDATE SET value=excluded.value
        """,
        (key, str(value))
    )

    conn.commit()
    conn.close()


def db_open_trade(
    symbol,
    side,
    mode,
    entry,
    stop,
    tp,
    qty,
    notional,
    risk_usdt,
):
    conn = db_connect()

    cur = conn.execute(
        """
        INSERT INTO trades(
            symbol,
            side,
            mode,
            entry,
            stop,
            tp,
            qty,
            notional,
            risk_usdt,
            opened_at,
            status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN')
        """,
        (
            symbol,
            side,
            mode,
            entry,
            stop,
            tp,
            qty,
            notional,
            risk_usdt,
            int(time.time()),
        )
    )

    trade_id = cur.lastrowid

    conn.commit()
    conn.close()

    return trade_id


def db_get_open_trades():
    conn = db_connect()

    rows = conn.execute(
        """
        SELECT *
        FROM trades
        WHERE status = 'OPEN'
        ORDER BY opened_at ASC
        """
    ).fetchall()

    conn.close()

    return rows


def db_get_open_trade(symbol):
    conn = db_connect()

    row = conn.execute(
        """
        SELECT *
        FROM trades
        WHERE symbol = ?
        AND status = 'OPEN'
        ORDER BY opened_at DESC
        LIMIT 1
        """,
        (symbol,)
    ).fetchone()

    conn.close()

    return row


def db_close_trade(
    trade_id,
    exit_price,
    pnl,
    result
):
    conn = db_connect()

    conn.execute(
        """
        UPDATE trades
        SET
            closed_at = ?,
            exit_price = ?,
            pnl = ?,
            result = ?,
            status = 'CLOSED'
        WHERE id = ?
        """,
        (
            int(time.time()),
            exit_price,
            pnl,
            result,
            trade_id,
        )
    )

    conn.commit()
    conn.close()


def db_stats():
    conn = db_connect()

    total = conn.execute(
        "SELECT COUNT(*) AS n FROM trades WHERE status='CLOSED'"
    ).fetchone()["n"]

    wins = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM trades
        WHERE status='CLOSED'
        AND pnl > 0
        """
    ).fetchone()["n"]

    losses = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM trades
        WHERE status='CLOSED'
        AND pnl < 0
        """
    ).fetchone()["n"]

    pnl = conn.execute(
        """
        SELECT COALESCE(SUM(pnl), 0) AS p
        FROM trades
        WHERE status='CLOSED'
        """
    ).fetchone()["p"]

    avg_win = conn.execute(
        """
        SELECT COALESCE(AVG(pnl), 0) AS p
        FROM trades
        WHERE status='CLOSED'
        AND pnl > 0
        """
    ).fetchone()["p"]

    avg_loss = conn.execute(
        """
        SELECT COALESCE(AVG(pnl), 0) AS p
        FROM trades
        WHERE status='CLOSED'
        AND pnl < 0
        """
    ).fetchone()["p"]

    conn.close()

    win_rate = (
        wins / total * 100
        if total > 0
        else 0
    )

    return {
        "total": total,
        "wins": wins,
        "losses": losses,
        "pnl": pnl,
        "win_rate": win_rate,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
    }


# ============================================================
# TELEGRAM HELPERS
# ============================================================

def telegram_send(text):
    """
    Thread-safe Telegram notification.
    Trading engine runs in a separate thread,
    Telegram runs on asyncio.
    """

    global TELEGRAM_LOOP

    if not TELEGRAM_LOOP:
        logger.warning("Telegram loop unavailable")
        return

    if not TELEGRAM_CHAT_ID:
        return

    try:
        future = asyncio.run_coroutine_threadsafe(
            TELEGRAM_APP.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            ),
            TELEGRAM_LOOP
        )

        future.result(timeout=15)

    except Exception as e:
        logger.error(
            "Telegram send error: %s",
            e
        )


def authorized(update: Update):
    if not update.effective_chat:
        return False

    return str(update.effective_chat.id) == ALLOWED_CHAT_ID


async def deny(update: Update):
    if update.callback_query:
        await update.callback_query.answer(
            "Access denied",
            show_alert=True
        )
    elif update.message:
        await update.message.reply_text(
            "⛔ Access denied."
        )


# ============================================================
# BINANCE CLIENT
# ============================================================

class BinanceClient:

    def __init__(self):
        self.session = requests.Session()

        if BINANCE_API_KEY:
            self.session.headers.update({
                "X-MBX-APIKEY": BINANCE_API_KEY
            })

    def sync_server_time(self):
        global BINANCE_TIME_OFFSET

        data = self.public_get(
            "/fapi/v1/time"
        )

        server_time = int(
            data["serverTime"]
        )

        local_time = int(
            time.time() * 1000
        )

        BINANCE_TIME_OFFSET = (
            server_time - local_time
        )

    def public_get(
        self,
        path,
        params=None
    ):
        url = BINANCE_BASE_URL + path

        response = self.session.get(
            url,
            params=params or {},
            timeout=15
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"Binance {response.status_code}: "
                f"{response.text[:500]}"
            )

        return response.json()

    def signed_request(
        self,
        method,
        path,
        params=None
    ):
        params = dict(params or {})

        params["timestamp"] = (
            int(time.time() * 1000)
            + BINANCE_TIME_OFFSET
        )

        params.setdefault(
            "recvWindow",
            10000
        )

        query = urlencode(
            params,
            doseq=True
        )

        signature = hmac.new(
            BINANCE_SECRET_KEY.encode(),
            query.encode(),
            hashlib.sha256
        ).hexdigest()

        params["signature"] = signature

        url = BINANCE_BASE_URL + path

        if method.upper() == "GET":
            response = self.session.get(
                url,
                params=params,
                timeout=15
            )
        elif method.upper() == "POST":
            response = self.session.post(
                url,
                params=params,
                timeout=15
            )
        elif method.upper() == "DELETE":
            response = self.session.delete(
                url,
                params=params,
                timeout=15
            )
        else:
            raise ValueError(
                f"Unsupported method {method}"
            )

        if response.status_code >= 400:
            raise RuntimeError(
                f"Binance {response.status_code}: "
                f"{response.text[:800]}"
            )

        return response.json()

    def get_exchange_info(self):
        return self.public_get(
            "/fapi/v1/exchangeInfo"
        )

    def get_klines(
        self,
        symbol,
        interval,
        limit=150
    ):
        return self.public_get(
            "/fapi/v1/klines",
            {
                "symbol": symbol,
                "interval": interval,
                "limit": limit,
            }
        )

    def get_price(self, symbol):
        data = self.public_get(
            "/fapi/v1/ticker/price",
            {"symbol": symbol}
        )

        return float(data["price"])

    def get_funding(self, symbol):
        try:
            data = self.public_get(
                "/fapi/v1/premiumIndex",
                {"symbol": symbol}
            )

            return float(
                data.get(
                    "lastFundingRate",
                    0
                )
            )

        except Exception:
            return 0.0

    def get_account(self):
        return self.signed_request(
            "GET",
            "/fapi/v2/account"
        )

    def get_equity(self):
        account = self.get_account()

        return float(
            account.get(
                "totalWalletBalance",
                0
            )
        )

    def get_position_risk(self):
        return self.signed_request(
            "GET",
            "/fapi/v2/positionRisk"
        )

    def get_open_orders(self, symbol=None):
        params = {}

        if symbol:
            params["symbol"] = symbol

        return self.signed_request(
            "GET",
            "/fapi/v1/openOrders",
            params
        )

    def set_leverage(self, symbol, leverage):
        return self.signed_request(
            "POST",
            "/fapi/v1/leverage",
            {
                "symbol": symbol,
                "leverage": leverage,
            }
        )

    def set_margin_type(self, symbol):
        try:
            return self.signed_request(
                "POST",
                "/fapi/v1/marginType",
                {
                    "symbol": symbol,
                    "marginType": "ISOLATED",
                }
            )

        except Exception as e:
            # Binance returns an error if already isolated.
            if "-4046" in str(e):
                return None

            raise

    def place_order(self, params):
        return self.signed_request(
            "POST",
            "/fapi/v1/order",
            params
        )
        
class BinanceClient:

    def place_order(self, params):
        return self.signed_request(
            "POST",
            "/fapi/v1/order",
            params
        )

    def place_algo_order(self, params):
        params = dict(params)

        params.setdefault(
class BinanceClient:

    def __init__(self):
        self.session = requests.Session()

        if BINANCE_API_KEY:
            self.session.headers.update({
                "X-MBX-APIKEY": BINANCE_API_KEY
            })

    def sync_server_time(self):
        global BINANCE_TIME_OFFSET

        data = self.public_get(
            "/fapi/v1/time"
        )

        server_time = int(
            data["serverTime"]
        )

        local_time = int(
            time.time() * 1000
        )

        BINANCE_TIME_OFFSET = (
            server_time - local_time
        )

    def public_get(
        self,
        path,
        params=None
    ):
        url = BINANCE_BASE_URL + path

        response = self.session.get(
            url,
            params=params or {},
            timeout=15
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"Binance {response.status_code}: "
                f"{response.text[:500]}"
            )

        return response.json()

    def signed_request(
        self,
        method,
        path,
        params=None
    ):
        params = dict(params or {})

        params["timestamp"] = (
            int(time.time() * 1000)
            + BINANCE_TIME_OFFSET
        )

        params.setdefault(
            "recvWindow",
            10000
        )

        query = urlencode(
            params,
            doseq=True
        )

        signature = hmac.new(
            BINANCE_SECRET_KEY.encode(),
            query.encode(),
            hashlib.sha256
        ).hexdigest()

        params["signature"] = signature

        url = BINANCE_BASE_URL + path

        if method.upper() == "GET":
            response = self.session.get(
                url,
                params=params,
                timeout=15
            )

        elif method.upper() == "POST":
            response = self.session.post(
                url,
                params=params,
                timeout=15
            )

        elif method.upper() == "DELETE":
            response = self.session.delete(
                url,
                params=params,
                timeout=15
            )

        else:
            raise ValueError(
                f"Unsupported method {method}"
            )

        if response.status_code >= 400:
            raise RuntimeError(
                f"Binance {response.status_code}: "
                f"{response.text[:800]}"
            )

        return response.json()

    def get_exchange_info(self):
        return self.public_get(
            "/fapi/v1/exchangeInfo"
        )

    def get_klines(
        self,
        symbol,
        interval,
        limit=150
    ):
        return self.public_get(
            "/fapi/v1/klines",
            {
                "symbol": symbol,
                "interval": interval,
                "limit": limit,
            }
        )

    def get_price(self, symbol):
        data = self.public_get(
            "/fapi/v1/ticker/price",
            {
                "symbol": symbol
            }
        )

        return float(
            data["price"]
        )

    def get_funding(self, symbol):
        try:
            data = self.public_get(
                "/fapi/v1/premiumIndex",
                {
                    "symbol": symbol
                }
            )

            return float(
                data.get(
                    "lastFundingRate",
                    0
                )
            )

        except Exception:
            return 0.0

    def get_account(self):
        return self.signed_request(
            "GET",
            "/fapi/v2/account"
        )

    def get_equity(self):
        account = self.get_account()

        return float(
            account.get(
                "totalWalletBalance",
                0
            )
        )

    def get_position_risk(self):
        return self.signed_request(
            "GET",
            "/fapi/v2/positionRisk"
        )

    def get_open_orders(self, symbol=None):
        params = {}

        if symbol:
            params["symbol"] = symbol

        return self.signed_request(
            "GET",
            "/fapi/v1/openOrders",
            params
        )

    def set_leverage(
        self,
        symbol,
        leverage
    ):
        return self.signed_request(
            "POST",
            "/fapi/v1/leverage",
            {
                "symbol": symbol,
                "leverage": leverage,
            }
        )

    def set_margin_type(self, symbol):
        try:
            return self.signed_request(
                "POST",
                "/fapi/v1/marginType",
                {
                    "symbol": symbol,
                    "marginType": "ISOLATED",
                }
            )

        except Exception as e:
            if "-4046" in str(e):
                return None

            raise

    def place_order(self, params):
        return self.signed_request(
            "POST",
            "/fapi/v1/order",
            params
        )

    def place_algo_order(self, params):
        params = dict(params)

        params.setdefault(
            "algoType",
            "CONDITIONAL"
        )

        return self.signed_request(
            "POST",
            "/fapi/v1/algoOrder",
            params
        )

        def place_algo_order(self, params):
        params = dict(params)

        params.setdefault(
            "algoType",
            "CONDITIONAL"
        )

        return self.signed_request(
            "POST",
            "/fapi/v1/algoOrder",
            params
        )

    def cancel_order(
        self,
        symbol,
        order_id
    ):
        return self.signed_request(
            "DELETE",
            "/fapi/v1/order",
            {
                "symbol": symbol,
                "orderId": order_id,
            }
        )

    def cancel_all_orders(self, symbol):
        return self.signed_request(
            "DELETE",
            "/fapi/v1/allOpenOrders",
            {
                "symbol": symbol
            }
        )


BINANCE = BinanceClient()



# ============================================================
# EXCHANGE FILTERS
# ============================================================

def load_exchange_symbols():

    global EXCHANGE_SYMBOLS
    global VALID_SYMBOLS

    info = BINANCE.get_exchange_info()

    symbols = info.get(
        "symbols",
        []
    )

    result = {}

    for item in symbols:

        symbol = item.get(
            "symbol"
        )

        if not symbol:
            continue

        if item.get("status") != "TRADING":
            continue

        if item.get("quoteAsset") != "USDT":
            continue

        filters = {}

        for f in item.get(
            "filters",
            []
        ):
            filters[
                f.get("filterType")
            ] = f

        price_filter = filters.get(
            "PRICE_FILTER",
            {}
        )

        lot_filter = filters.get(
            "LOT_SIZE",
            {}
        )

        notional_filter = (
            filters.get("NOTIONAL")
            or
            filters.get("MIN_NOTIONAL")
            or
            {}
        )

        result[symbol] = {
            "tick_size": float(
                price_filter.get(
                    "tickSize",
                    0
                )
            ),
            "step_size": float(
                lot_filter.get(
                    "stepSize",
                    0
                )
            ),
            "min_qty": float(
                lot_filter.get(
                    "minQty",
                    0
                )
            ),
            "max_qty": float(
                lot_filter.get(
                    "maxQty",
                    0
                )
            ),
            "min_notional": float(
                notional_filter.get(
                    "minNotional",
                    0
                )
            ),
        }

    EXCHANGE_SYMBOLS = result

    VALID_SYMBOLS = [
        s
        for s in PREFERRED_SYMBOLS
        if s in EXCHANGE_SYMBOLS
    ]

    logger.info(
        "Loaded %s tradable symbols",
        len(VALID_SYMBOLS)
    )


def round_price(symbol, price, direction="down"):
    tick = EXCHANGE_SYMBOLS[
        symbol
    ]["tick_size"]

    if tick <= 0:
        return price

    value = Decimal(
        str(price)
    )

    step = Decimal(
        str(tick)
    )

    rounding = (
        ROUND_UP
        if direction == "up"
        else ROUND_DOWN
    )

    rounded = (
        value / step
    ).quantize(
        Decimal("1"),
        rounding=rounding
    ) * step

    return float(rounded)


def round_qty(symbol, qty):
    step = EXCHANGE_SYMBOLS[
        symbol
    ]["step_size"]

    if step <= 0:
        return qty

    value = Decimal(
        str(qty)
    )

    step_decimal = Decimal(
        str(step)
    )

    rounded = (
        value / step_decimal
    ).quantize(
        Decimal("1"),
        rounding=ROUND_DOWN
    ) * step_decimal

    return float(rounded)


# ============================================================
# INDICATORS
# ============================================================

def ema(values, period):
    if len(values) < period:
        return []

    alpha = 2 / (
        period + 1
    )

    result = [
        float(values[0])
    ]

    for price in values[1:]:
        result.append(
            alpha * float(price)
            + (1 - alpha)
            * result[-1]
        )

    return result


def rsi(values, period=14):
    if len(values) <= period:
        return []

    gains = []
    losses = []

    for i in range(1, len(values)):
        change = (
            values[i]
            - values[i - 1]
        )

        gains.append(
            max(change, 0)
        )

        losses.append(
            max(-change, 0)
        )

    avg_gain = (
        sum(gains[:period])
        / period
    )

    avg_loss = (
        sum(losses[:period])
        / period
    )

    result = [
        50.0
    ] * period

    for i in range(
        period,
        len(gains)
    ):
        avg_gain = (
            (avg_gain * (period - 1))
            + gains[i]
        ) / period

        avg_loss = (
            (avg_loss * (period - 1))
            + losses[i]
        ) / period

        if avg_loss == 0:
            value = 100
        else:
            rs = (
                avg_gain
                / avg_loss
            )

            value = (
                100
                - 100 / (1 + rs)
            )

        result.append(value)

    return result


def atr(
    highs,
    lows,
    closes,
    period=14
):
    if len(closes) <= period:
        return []

    tr = []

    for i in range(1, len(closes)):
        current_high = highs[i]
        current_low = lows[i]
        previous_close = closes[i - 1]

        true_range = max(
            current_high - current_low,
            abs(
                current_high
                - previous_close
            ),
            abs(
                current_low
                - previous_close
            )
        )

        tr.append(true_range)

    current_atr = (
        sum(tr[:period])
        / period
    )

    result = [
        current_atr
    ]

    for i in range(
        period,
        len(tr)
    ):
        current_atr = (
            (
                current_atr
                * (period - 1)
            )
            + tr[i]
        ) / period

        result.append(
            current_atr
        )

    return result


def adx(
    highs,
    lows,
    closes,
    period=14
):
    if len(closes) < (
        period * 2 + 2
    ):
        return []

    trs = []
    plus_dm = []
    minus_dm = []

    for i in range(1, len(closes)):

        high = highs[i]
        low = lows[i]

        previous_high = highs[i - 1]
        previous_low = lows[i - 1]
        previous_close = closes[i - 1]

        tr = max(
            high - low,
            abs(
                high
                - previous_close
            ),
            abs(
                low
                - previous_close
            )
        )

        up_move = (
            high
            - previous_high
        )

        down_move = (
            previous_low
            - low
        )

        plus = (
            up_move
            if (
                up_move > down_move
                and up_move > 0
            )
            else 0
        )

        minus = (
            down_move
            if (
                down_move > up_move
                and down_move > 0
            )
            else 0
        )

        trs.append(tr)
        plus_dm.append(plus)
        minus_dm.append(minus)

    if len(trs) < period:
        return []

    atr_value = (
        sum(trs[:period])
        / period
    )

    plus_value = (
        sum(plus_dm[:period])
        / period
    )

    minus_value = (
        sum(minus_dm[:period])
        / period
    )

    dx_values = []

    for i in range(
        period,
        len(trs)
    ):
        atr_value = (
            (
                atr_value
                * (period - 1)
            )
            + trs[i]
        ) / period

        plus_value = (
            (
                plus_value
                * (period - 1)
            )
            + plus_dm[i]
        ) / period

        minus_value = (
            (
                minus_value
                * (period - 1)
            )
            + minus_dm[i]
        ) / period

        if atr_value == 0:
            dx_values.append(0)
            continue

        plus_di = (
            100
            * plus_value
            / atr_value
        )

        minus_di = (
            100
            * minus_value
            / atr_value
        )

        denominator = (
            plus_di
            + minus_di
        )

        if denominator == 0:
            dx = 0
        else:
            dx = (
                100
                * abs(
                    plus_di
                    - minus_di
                )
                / denominator
            )

        dx_values.append(dx)

    if len(dx_values) < period:
        return []

    adx_value = (
        sum(dx_values[:period])
        / period
    )

    result = [
        adx_value
    ]

    for i in range(
        period,
        len(dx_values)
    ):
        adx_value = (
            (
                adx_value
                * (period - 1)
            )
            + dx_values[i]
        ) / period

        result.append(
            adx_value
        )

    return result


# ============================================================
# MARKET DATA
# ============================================================

def parse_klines(raw):
    candles = []

    for row in raw:

        candles.append({
            "open_time": int(row[0]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5]),
            "close_time": int(row[6]),
        })

    # Remove currently forming candle.
    now_ms = int(
        time.time() * 1000
    )

    candles = [
        x for x in candles
        if x["close_time"] < now_ms
    ]

    return candles


def get_market_snapshot(symbol):

    raw_15m = BINANCE.get_klines(
        symbol,
        "15m",
        KLINE_LIMIT
    )

    raw_1h = BINANCE.get_klines(
        symbol,
        "1h",
        KLINE_LIMIT
    )

    candles_15m = parse_klines(
        raw_15m
    )

    candles_1h = parse_klines(
        raw_1h
    )

    if len(candles_15m) < 80:
        return None

    if len(candles_1h) < 80:
        return None

    return (
        candles_15m,
        candles_1h
    )


# ============================================================
# SIGNAL ENGINE
# ============================================================

def calculate_signal(symbol):

    try:

        snapshot = get_market_snapshot(
            symbol
        )

        if not snapshot:
            return None

        candles_15m, candles_1h = snapshot

        close15 = [
            x["close"]
            for x in candles_15m
        ]

        high15 = [
            x["high"]
            for x in candles_15m
        ]

        low15 = [
            x["low"]
            for x in candles_15m
        ]

        volume15 = [
            x["volume"]
            for x in candles_15m
        ]

        close1h = [
            x["close"]
            for x in candles_1h
        ]

        ema20_15 = ema(
            close15,
            20
        )

        ema50_15 = ema(
            close15,
            50
        )

        ema20_1h = ema(
            close1h,
            20
        )

        ema50_1h = ema(
            close1h,
            50
        )

        rsi15 = rsi(
            close15,
            14
        )

        atr15 = atr(
            high15,
            low15,
            close15,
            14
        )

        adx15 = adx(
            high15,
            low15,
            close15,
            14
        )

        if not all([
            ema20_15,
            ema50_15,
            ema20_1h,
            ema50_1h,
            rsi15,
            atr15,
            adx15
        ]):
            return None

        price = close15[-1]

        e20_15 = ema20_15[-1]
        e50_15 = ema50_15[-1]

        e20_1h = ema20_1h[-1]
        e50_1h = ema50_1h[-1]

        current_rsi = rsi15[-1]
        current_atr = atr15[-1]
        current_adx = adx15[-1]

        atr_pct = (
            current_atr
            / price
        )

        if (
            atr_pct < MIN_ATR_PCT
            or atr_pct > MAX_ATR_PCT
        ):
            return None

        volume_avg = (
            sum(volume15[-21:-1])
            / 20
        )

        volume_ratio = (
            volume15[-1]
            / volume_avg
            if volume_avg > 0
            else 0
        )

        funding = BINANCE.get_funding(
            symbol
        )

        last_candle = candles_15m[-1]

        previous_candle = candles_15m[-2]

        bullish_candle = (
            last_candle["close"]
            > last_candle["open"]
            and last_candle["close"]
            > previous_candle["close"]
        )

        bearish_candle = (
            last_candle["close"]
            < last_candle["open"]
            and last_candle["close"]
            < previous_candle["close"]
        )

        long_score = 0.0
        short_score = 0.0

        # 1H trend
        if e20_1h > e50_1h:
            long_score += 2
        elif e20_1h < e50_1h:
            short_score += 2

        # 15M trend
        if e20_15 > e50_15:
            long_score += 2
        elif e20_15 < e50_15:
            short_score += 2

        # Pullback RSI
        if 45 <= current_rsi <= 62:
            long_score += 1

        if 38 <= current_rsi <= 55:
            short_score += 1

        # Candle confirmation
        if bullish_candle:
            long_score += 1

        if bearish_candle:
            short_score += 1

        # Volume
        if volume_ratio >= MIN_VOLUME_RATIO:
            long_score += 1
            short_score += 1

        # Funding extremes
        if funding > 0.0008:
            short_score += 1

        if funding < -0.0008:
            long_score += 1

        # ADX
        if current_adx >= MIN_ADX:
            long_score += 1
            short_score += 1

        # Need clear winner
        if long_score >= MIN_SCORE and (
            long_score > short_score
        ):
            side = "LONG"
            score = long_score

        elif short_score >= MIN_SCORE and (
            short_score > long_score
        ):
            side = "SHORT"
            score = short_score

        else:
            return None

        stop_distance = (
            current_atr
            * ATR_STOP_MULT
        )

        stop_pct = (
            stop_distance
            / price
        )

        stop_pct = max(
            MIN_STOP_PCT,
            min(
                stop_pct,
                MAX_STOP_PCT
            )
        )

        if side == "LONG":

            stop = (
                price
                * (1 - stop_pct)
            )

            tp = (
                price
                * (
                    1
                    + stop_pct
                    * REWARD_R
                )
            )

        else:

            stop = (
                price
                * (1 + stop_pct)
            )

            tp = (
                price
                * (
                    1
                    - stop_pct
                    * REWARD_R
                )
            )

        return {
            "symbol": symbol,
            "side": side,
            "score": score,
            "price": price,
            "stop": stop,
            "tp": tp,
            "stop_pct": stop_pct,
            "atr": current_atr,
            "atr_pct": atr_pct,
            "rsi": current_rsi,
            "adx": current_adx,
            "volume_ratio": volume_ratio,
            "funding": funding,
            "trend_1h": (
                "BULLISH"
                if e20_1h > e50_1h
                else "BEARISH"
            ),
            "trend_15m": (
                "BULLISH"
                if e20_15 > e50_15
                else "BEARISH"
            ),
        }

    except Exception as e:

        logger.exception(
            "Signal error %s: %s",
            symbol,
            e
        )

        return None


# ============================================================
# RISK MANAGER
# ============================================================

def get_equity():

    if not LIVE_TRADING:

        value = get_state(
            "paper_equity"
        )

        if value is None:
            value = PAPER_START_EQUITY
            set_state(
                "paper_equity",
                value
            )

        return float(value)

    return BINANCE.get_equity()


def get_day_key():
    return datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d")


def update_daily_state():

    today = get_day_key()

    saved_day = get_state(
        "day"
    )

    if saved_day != today:

        equity = get_equity()

        set_state(
            "day",
            today
        )

        set_state(
            "day_start_equity",
            equity
        )

        set_state(
            "daily_lock",
            "0"
        )


def daily_locked():

    update_daily_state()

    equity = get_equity()

    start = float(
        get_state(
            "day_start_equity",
            equity
        )
    )

    if start <= 0:
        return False

    drawdown = (
        start - equity
    ) / start

    if drawdown >= MAX_DAILY_DRAWDOWN:

        set_state(
            "daily_lock",
            "1"
        )

        return True

    return (
        get_state(
            "daily_lock",
            "0"
        ) == "1"
    )


def calculate_quantity(
    symbol,
    entry,
    stop
):

    equity = get_equity()

    risk_usdt = (
        equity
        * RISK_PER_TRADE
    )

    stop_distance = abs(
        entry - stop
    )

    if stop_distance <= 0:
        return None

    # Quantity needed to lose approximately
    # RISK_PER_TRADE of equity at the stop.
    quantity = (
        risk_usdt
        / stop_distance
    )

    filters = EXCHANGE_SYMBOLS[
        symbol
    ]

    max_margin = (
        equity
        * MAX_MARGIN_PER_TRADE
    )

    max_notional = (
        max_margin
        * LEVERAGE
    )

    max_quantity = (
        max_notional
        / entry
    )

    quantity = min(
        quantity,
        max_quantity
    )

    quantity = round_qty(
        symbol,
        quantity
    )

    if quantity <= 0:
        return None

    if quantity < filters["min_qty"]:
        return None

    if filters["max_qty"] > 0:
        quantity = min(
            quantity,
            filters["max_qty"]
        )

    notional = (
        quantity
        * entry
    )

    if (
        filters["min_notional"] > 0
        and
        notional
        < filters["min_notional"]
    ):
        return None

    return {
        "qty": quantity,
        "notional": notional,
        "risk_usdt": (
            stop_distance
            * quantity
        ),
    }


def current_margin_used():

    if not LIVE_TRADING:

        total = 0

        for row in db_get_open_trades():

            total += (
                row["notional"]
                / LEVERAGE
            )

        return total

    total = 0

    try:

        positions = (
            BINANCE.get_position_risk()
        )

        for position in positions:

            amt = abs(
                float(
                    position.get(
                        "positionAmt",
                        0
                    )
                )
            )

            entry = float(
                position.get(
                    "entryPrice",
                    0
                )
            )

            if amt > 0 and entry > 0:
                total += (
                    amt
                    * entry
                    / LEVERAGE
                )

    except Exception:
        pass

    return total


def can_open_trade(symbol):

    if BOT_PAUSED:
        return False, "BOT PAUSED"

    if EMERGENCY_STOP:
        return False, "EMERGENCY STOP"

    if daily_locked():
        return False, "DAILY DRAWDOWN LOCK"

    open_trades = (
        db_get_open_trades()
    )

    if len(open_trades) >= MAX_OPEN_POSITIONS:
        return False, "MAX POSITIONS"

    if db_get_open_trade(symbol):
        return False, "SYMBOL ALREADY OPEN"

    global_last = float(
        get_state(
            "last_global_entry",
            "0"
        )
    )

    if (
        global_last > 0
        and
        time.time() - global_last
        <
        GLOBAL_ENTRY_COOLDOWN_MINUTES
        * 60
    ):
        return False, "GLOBAL COOLDOWN"

    last_symbol = float(
        get_state(
            f"last_entry_{symbol}",
            "0"
        )
    )

    if (
        last_symbol > 0
        and
        time.time() - last_symbol
        <
        COOLDOWN_MINUTES
        * 60
    ):
        return False, "SYMBOL COOLDOWN"

    equity = get_equity()

    margin = (
        current_margin_used()
    )

    max_total_margin = (
        equity
        * MAX_TOTAL_MARGIN
    )

    if margin >= max_total_margin:
        return False, "MAX TOTAL MARGIN"

    return True, "OK"


# ============================================================
# LIVE ORDER MANAGEMENT
# ============================================================

def open_live_trade(signal):

    symbol = signal["symbol"]
    side = signal["side"]

    entry_reference = (
        BINANCE.get_price(symbol)
    )

    stop = signal["stop"]
    tp = signal["tp"]

    quantity_data = (
        calculate_quantity(
            symbol,
            entry_reference,
            stop
        )
    )

    if not quantity_data:
        raise RuntimeError(
            f"{symbol}: quantity too small "
            f"or violates exchange filters"
        )

    qty = quantity_data["qty"]

    BINANCE.set_margin_type(
        symbol
    )

    BINANCE.set_leverage(
        symbol,
        LEVERAGE
    )

    order_side = (
        "BUY"
        if side == "LONG"
        else "SELL"
    )

    logger.info(
        "LIVE ENTRY %s %s qty=%s",
        symbol,
        side,
        qty
    )

    entry_order = BINANCE.place_order({
        "symbol": symbol,
        "side": order_side,
        "type": "MARKET",
        "quantity": qty,
        "newOrderRespType": "RESULT",
    })

    time.sleep(0.5)

    actual_entry = (
        get_actual_entry_price(
            symbol
        )
    )

    if actual_entry <= 0:
        actual_entry = (
            float(
                entry_order.get(
                    "avgPrice",
                    entry_reference
                )
            )
        )

    # Recalculate stop / TP from actual entry.
    stop_pct = signal["stop_pct"]

    if side == "LONG":

        stop = (
            actual_entry
            * (1 - stop_pct)
        )

        tp = (
            actual_entry
            * (
                1
                + stop_pct
                * REWARD_R
            )
        )

        sl_side = "SELL"

    else:

        stop = (
            actual_entry
            * (1 + stop_pct)
        )

        tp = (
            actual_entry
            * (
                1
                - stop_pct
                * REWARD_R
            )
        )

        sl_side = "BUY"

    stop_direction = (
        "down"
        if side == "LONG"
        else "up"
    )

    tp_direction = (
        "up"
        if side == "LONG"
        else "down"
    )

    stop = round_price(
        symbol,
        stop,
        stop_direction
    )

    tp = round_price(
        symbol,
        tp,
        tp_direction
    )

    try:

        BINANCE.place_algo_order({
            "symbol": symbol,
            "side": sl_side,
            "type": "STOP_MARKET",
            "triggerPrice": stop,
            "closePosition": "true",
            "workingType": "MARK_PRICE",
            "priceProtect": "false",
            "clientAlgoId":
                f"SNPR_SL_{int(time.time()*1000)}",
        })

    except Exception:

        logger.exception(
            "PROTECTION FAILED: %s",
            symbol
        )

        emergency_close_symbol(
            symbol,
            side
        )

        raise

    notional = (
        qty
        * actual_entry
    )

    risk = abs(
        actual_entry
        - stop
    ) * qty

    trade_id = db_open_trade(
        symbol=symbol,
        side=side,
        mode="LIVE",
        entry=actual_entry,
        stop=stop,
        tp=tp,
        qty=qty,
        notional=notional,
        risk_usdt=risk,
    )

    set_state(
        "last_global_entry",
        time.time()
    )

    set_state(
        f"last_entry_{symbol}",
        time.time()
    )

    return {
        "trade_id": trade_id,
        "entry": actual_entry,
        "stop": stop,
        "tp": tp,
        "qty": qty,
        "notional": notional,
        "risk": risk,
    }


def get_actual_entry_price(symbol):

    positions = (
        BINANCE.get_position_risk()
    )

    for position in positions:

        if position.get(
            "symbol"
        ) != symbol:
            continue

        amount = float(
            position.get(
                "positionAmt",
                0
            )
        )

        if abs(amount) > 0:

            return float(
                position.get(
                    "entryPrice",
                    0
                )
            )

    return 0.0


def emergency_close_symbol(
    symbol,
    side
):

    close_side = (
        "SELL"
        if side == "LONG"
        else "BUY"
    )

    try:

        BINANCE.place_order({
            "symbol": symbol,
            "side": close_side,
            "type": "MARKET",
            "quantity": abs(
                get_position_amount(
                    symbol
                )
            ),
            "reduceOnly": "true",
            "newOrderRespType": "RESULT",
        })

    except Exception as e:

        logger.error(
            "Emergency close failed %s: %s",
            symbol,
            e
        )


def get_position_amount(symbol):

    positions = (
        BINANCE.get_position_risk()
    )

    for p in positions:

        if p.get("symbol") == symbol:
            return float(
                p.get(
                    "positionAmt",
                    0
                )
            )

    return 0.0


# ============================================================
# PAPER TRADING
# ============================================================

def paper_open_trade(signal):

    symbol = signal["symbol"]
    side = signal["side"]

    entry = signal["price"]
    stop = signal["stop"]
    tp = signal["tp"]

    quantity_data = (
        calculate_quantity(
            symbol,
            entry,
            stop
        )
    )

    if not quantity_data:
        raise RuntimeError(
            f"{symbol}: paper quantity invalid"
        )

    qty = quantity_data["qty"]
    notional = (
        qty * entry
    )

    risk = (
        abs(entry - stop)
        * qty
    )

    trade_id = db_open_trade(
        symbol=symbol,
        side=side,
        mode="PAPER",
        entry=entry,
        stop=stop,
        tp=tp,
        qty=qty,
        notional=notional,
        risk_usdt=risk,
    )

    set_state(
        "last_global_entry",
        time.time()
    )

    set_state(
        f"last_entry_{symbol}",
        time.time()
    )

    return {
        "trade_id": trade_id,
        "entry": entry,
        "stop": stop,
        "tp": tp,
        "qty": qty,
        "notional": notional,
        "risk": risk,
    }


def close_paper_trade(
    row,
    exit_price,
    reason
):

    if row["side"] == "LONG":

        pnl = (
            exit_price
            - row["entry"]
        ) * row["qty"]

    else:

        pnl = (
            row["entry"]
            - exit_price
        ) * row["qty"]

    # Approximate trading fee.
    fee_rate = 0.0005

    fees = (
        (
            row["entry"]
            + exit_price
        )
        * row["qty"]
        * fee_rate
    )

    net_pnl = (
        pnl - fees
    )

    current_equity = (
        get_equity()
    )

    new_equity = (
        current_equity
        + net_pnl
    )

    set_state(
        "paper_equity",
        new_equity
    )

    db_close_trade(
        trade_id=row["id"],
        exit_price=exit_price,
        pnl=net_pnl,
        result=reason,
    )

    telegram_send(
        f"🔔 <b>PAPER TRADE CLOSED</b>\n\n"
        f"{row['symbol']} "
        f"{row['side']}\n"
        f"Entry: <code>{row['entry']:.6g}</code>\n"
        f"Exit: <code>{exit_price:.6g}</code>\n"
        f"PnL: <b>{net_pnl:+.2f} USDT</b>\n"
        f"Reason: {reason}\n"
        f"Paper equity: "
        f"<b>{new_equity:.2f} USDT</b>"
    )


def manage_paper_positions():

    rows = db_get_open_trades()

    for row in rows:

        if row["mode"] != "PAPER":
            continue

        try:

            price = BINANCE.get_price(
                row["symbol"]
            )

            if row["side"] == "LONG":

                if price <= row["stop"]:

                    close_paper_trade(
                        row,
                        row["stop"],
                        "STOP LOSS"
                    )

                elif price >= row["tp"]:

                    close_paper_trade(
                        row,
                        row["tp"],
                        "TAKE PROFIT"
                    )

            else:

                if price >= row["stop"]:

                    close_paper_trade(
                        row,
                        row["stop"],
                        "STOP LOSS"
                    )

                elif price <= row["tp"]:

                    close_paper_trade(
                        row,
                        row["tp"],
                        "TAKE PROFIT"
                    )

        except Exception as e:

            logger.error(
                "Paper position error: %s",
                e
            )


# ============================================================
# POSITION MANAGEMENT
# ============================================================

def manage_live_positions():

    if not LIVE_TRADING:
        return

    try:

        positions = (
            BINANCE.get_position_risk()
        )

        live_symbols = set()

        for p in positions:

            symbol = p.get(
                "symbol"
            )

            amount = float(
                p.get(
                    "positionAmt",
                    0
                )
            )

            if abs(amount) <= 0:
                continue

            live_symbols.add(symbol)

            row = db_get_open_trade(
                symbol
            )

            if not row:
                continue

            entry = float(
                p.get(
                    "entryPrice",
                    row["entry"]
                )
            )

            mark = float(
                p.get(
                    "markPrice",
                    entry
                )
            )

            risk_per_unit = abs(
                row["entry"]
                - row["stop"]
            )

            if risk_per_unit <= 0:
                continue

            if row["side"] == "LONG":

                r_multiple = (
                    mark - entry
                ) / risk_per_unit

            else:

                r_multiple = (
                    entry - mark
                ) / risk_per_unit

            # Break-even protection.
            if (
                r_multiple
                >= BE_TRIGGER_R
            ):

                if row["side"] == "LONG":

                    new_stop = (
                        entry
                        * (1 + BE_LOCK_PCT)
                    )

                    new_stop = round_price(
                        row["symbol"],
                        new_stop,
                        "down"
                    )

                else:

                    new_stop = (
                        entry
                        * (1 - BE_LOCK_PCT)
                    )

                    new_stop = round_price(
                        row["symbol"],
                        new_stop,
                        "up"
                    )

                replace_protection(
                    row["symbol"],
                    row["side"],
                    new_stop,
                    row["tp"]
                )

            # ATR trailing.
            if (
                r_multiple
                >= TRAIL_TRIGGER_R
            ):

                raw = BINANCE.get_klines(
                    row["symbol"],
                    "15m",
                    60
                )

                candles = parse_klines(
                    raw
                )

                if len(candles) >= 30:

                    highs = [
                        x["high"]
                        for x in candles
                    ]

                    lows = [
                        x["low"]
                        for x in candles
                    ]

                    closes = [
                        x["close"]
                        for x in candles
                    ]

                    atr_values = atr(
                        highs,
                        lows,
                        closes,
                        14
                    )

                    if atr_values:

                        current_atr = (
                            atr_values[-1]
                        )

                        if row["side"] == "LONG":

                            trail = (
                                mark
                                - current_atr
                                * TRAIL_ATR_MULT
                            )

                            trail = max(
                                trail,
                                entry
                            )

                            trail = round_price(
                                row["symbol"],
                                trail,
                                "down"
                            )

                            if trail > row["stop"]:

                                replace_protection(
                                    row["symbol"],
                                    row["side"],
                                    trail,
                                    row["tp"]
                                )

                        else:

                            trail = (
                                mark
                                + current_atr
                                * TRAIL_ATR_MULT
                            )

                            trail = min(
                                trail,
                                entry
                            )

                            trail = round_price(
                                row["symbol"],
                                trail,
                                "up"
                            )

                            if trail < row["stop"]:

                                replace_protection(
                                    row["symbol"],
                                    row["side"],
                                    trail,
                                    row["tp"]
                                )

        # If database thinks a live position exists,
        # but Binance no longer has it, close the journal row.
        for row in db_get_open_trades():

            if row["mode"] != "LIVE":
                continue

            if row["symbol"] not in live_symbols:

                # We don't invent exact exchange PnL here.
                # Mark as closed externally.
                db_close_trade(
                    row["id"],
                    row["entry"],
                    0,
                    "CLOSED/EXTERNAL"
                )

    except Exception as e:

        logger.exception(
            "Live position manager error: %s",
            e
        )


def replace_protection(
    symbol,
    side,
    stop,
    tp
):

    try:

        # Only cancel OUR protection orders.
        orders = (
            BINANCE.get_open_orders(
                symbol
            )
        )

        for order in orders:

            client_id = order.get(
                "clientOrderId",
                ""
            )

            if client_id.startswith(
                "SNPR_"
            ):

                try:
                    BINANCE.cancel_order(
                        symbol,
                        order["orderId"]
                    )
                except Exception:
                    pass

        if side == "LONG":
            order_side = "SELL"

            stop = round_price(
                symbol,
                stop,
                "down"
            )

            tp = round_price(
                symbol,
                tp,
                "up"
            )

        else:
            order_side = "BUY"

            stop = round_price(
                symbol,
                stop,
                "up"
            )

            tp = round_price(
                symbol,
                tp,
                "down"
            )

        BINANCE.place_algo_order({
            "symbol": symbol,
            "side": "SELL" if order_side in ["BUY", "LONG"] else "BUY",
            "type": "STOP_MARKET",
            "triggerPrice": stop,
            "closePosition": "true",
            "workingType": "MARK_PRICE",
            "priceProtect": "false",
            "clientAlgoId":
                f"SNPR_SL_{int(time.time()*1000)}",
        })

        BINANCE.place_algo_order({
            "symbol": symbol,
            "side": "SELL" if order_side in ["BUY", "LONG"] else "BUY",
            "type": "TAKE_PROFIT_MARKET",
            "triggerPrice": tp,
            "closePosition": "true",
            "workingType": "MARK_PRICE",
            "priceProtect": "false",
            "clientAlgoId":
                f"SNPR_TP_{int(time.time()*1000)}",
        })

    except Exception as e:

        logger.error(
            "Protection replacement failed %s: %s",
            symbol,
            e
        )


# ============================================================
# TELEGRAM ENTRY MESSAGE
# ============================================================

def entry_message(
    signal,
    execution
):

    mode = (
        "🧪 PAPER"
        if not LIVE_TRADING
        else "🔴 LIVE"
    )

    return (
        f"🚨 <b>NEW TRADE</b> {mode}\n\n"
        f"<b>{signal['symbol']}</b> "
        f"{signal['side']}\n\n"

        f"Score: <b>{signal['score']:.1f}</b>\n"
        f"1H trend: <b>{signal['trend_1h']}</b>\n"
        f"15M trend: <b>{signal['trend_15m']}</b>\n"
        f"RSI: <b>{signal['rsi']:.1f}</b>\n"
        f"ADX: <b>{signal['adx']:.1f}</b>\n"
        f"Volume: <b>{signal['volume_ratio']:.2f}x</b>\n"
        f"Funding: <b>{signal['funding']:.5f}%</b>\n"
        f"ATR: <b>{signal['atr_pct']*100:.2f}%</b>\n\n"

        f"Entry: <code>{execution['entry']:.8g}</code>\n"
        f"SL: <code>{execution['stop']:.8g}</code>\n"
        f"TP: <code>{execution['tp']:.8g}</code>\n\n"

        f"Qty: <code>{execution['qty']:.8g}</code>\n"
        f"Notional: <b>{execution['notional']:.2f} USDT</b>\n"
        f"Risk: <b>{execution['risk']:.2f} USDT</b>\n"
    )


# ============================================================
# TRADING ENGINE
# ============================================================

def find_best_signal():

    candidates = []

    for symbol in VALID_SYMBOLS:

        if STOP_EVENT.is_set():
            break

        signal = calculate_signal(
            symbol
        )

        if not signal:
            continue

        candidates.append(
            signal
        )

    if not candidates:
        return None

    candidates.sort(
        key=lambda x: x["score"],
        reverse=True
    )

    return candidates[0]


def execute_signal(signal):

    symbol = signal["symbol"]

    allowed, reason = (
        can_open_trade(symbol)
    )

    if not allowed:

        logger.info(
            "Skip %s: %s",
            symbol,
            reason
        )

        return None

    try:

        if LIVE_TRADING:

            execution = (
                open_live_trade(
                    signal
                )
            )

        else:

            execution = (
                paper_open_trade(
                    signal
                )
            )

        telegram_send(
            entry_message(
                signal,
                execution
            )
        )

        return execution

    except Exception as e:

        logger.exception(
            "Trade execution failed"
        )

        telegram_send(
            f"⚠️ <b>TRADE ERROR</b>\n\n"
            f"{symbol}\n"
            f"<code>{str(e)[:800]}</code>"
        )

        return None


def trading_loop():

    logger.info(
        "Trading engine started. LIVE=%s",
        LIVE_TRADING
    )

    try:

        BINANCE.sync_server_time()

        load_exchange_symbols()

        update_daily_state()

    except Exception as e:

        logger.exception(
            "Startup Binance error"
        )

        telegram_send(
            f"❌ <b>BOT START ERROR</b>\n\n"
            f"<code>{str(e)[:1000]}</code>"
        )

    while not STOP_EVENT.is_set():

        cycle_start = time.time()

        try:

            update_daily_state()

            if LIVE_TRADING:
                manage_live_positions()
            else:
                manage_paper_positions()

            if not BOT_PAUSED and not EMERGENCY_STOP:

                if not daily_locked():

                    signal = (
                        find_best_signal()
                    )

                    if signal:

                        logger.info(
                            "Best signal: %s %s score %.1f",
                            signal["symbol"],
                            signal["side"],
                            signal["score"]
                        )

                        execute_signal(
                            signal
                        )

            elapsed = (
                time.time()
                - cycle_start
            )

            sleep_for = max(
                5,
                SCAN_INTERVAL_SECONDS
                - int(elapsed)
            )

            STOP_EVENT.wait(
                sleep_for
            )

        except Exception as e:

            logger.exception(
                "Main trading loop error"
            )

            telegram_send(
                f"⚠️ <b>ENGINE ERROR</b>\n\n"
                f"<code>{str(e)[:1000]}</code>"
            )

            STOP_EVENT.wait(15)


# ============================================================
# TELEGRAM UI
# ============================================================

def main_keyboard():

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "📊 Status",
                callback_data="status"
            ),
            InlineKeyboardButton(
                "📈 Positions",
                callback_data="positions"
            ),
        ],
        [
            InlineKeyboardButton(
                "🔎 Signals",
                callback_data="signals"
            ),
            InlineKeyboardButton(
                "📋 Stats",
                callback_data="stats"
            ),
        ],
        [
            InlineKeyboardButton(
                "▶️ Resume",
                callback_data="resume"
            ),
            InlineKeyboardButton(
                "⏸ Pause",
                callback_data="pause"
            ),
        ],
        [
            InlineKeyboardButton(
                "🛑 Emergency",
                callback_data="emergency"
            ),
            InlineKeyboardButton(
                "💥 Close All",
                callback_data="close_confirm"
            ),
        ],
    ])


def status_text():

    mode = (
        "🧪 PAPER"
        if not LIVE_TRADING
        else "🔴 LIVE"
    )

    state = (
        "⏸ PAUSED"
        if BOT_PAUSED
        else "▶️ RUNNING"
    )

    emergency = (
        "🚨 YES"
        if EMERGENCY_STOP
        else "NO"
    )

    try:
        equity = get_equity()

        margin = (
            current_margin_used()
        )

        locked = daily_locked()

        return (
            f"🤖 <b>BOT STATUS</b>\n\n"
            f"Mode: <b>{mode}</b>\n"
            f"Engine: <b>{state}</b>\n"
            f"Emergency: <b>{emergency}</b>\n\n"
            f"Equity: <b>{equity:.2f} USDT</b>\n"
            f"Margin used: <b>{margin:.2f} USDT</b>\n"
            f"Daily lock: "
            f"<b>{'YES' if locked else 'NO'}</b>\n\n"
            f"Leverage: <b>{LEVERAGE}x</b>\n"
            f"Risk/trade: "
            f"<b>{RISK_PER_TRADE*100:.2f}%</b>\n"
            f"Max positions: "
            f"<b>{MAX_OPEN_POSITIONS}</b>\n"
        )

    except Exception as e:

        return (
            f"🤖 <b>BOT STATUS</b>\n\n"
            f"Mode: <b>{mode}</b>\n"
            f"Engine: <b>{state}</b>\n"
            f"⚠️ Binance error:\n"
            f"<code>{str(e)[:600]}</code>"
        )


async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not authorized(update):
        await deny(update)
        return

    await update.message.reply_text(
        status_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=main_keyboard()
    )


async def status_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not authorized(update):
        await deny(update)
        return

    await update.message.reply_text(
        status_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=main_keyboard()
    )


async def positions_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not authorized(update):
        await deny(update)
        return

    rows = db_get_open_trades()

    if not rows:

        await update.message.reply_text(
            "📭 No open bot positions."
        )

        return

    lines = [
        "📈 <b>OPEN POSITIONS</b>\n"
    ]

    for row in rows:

        lines.append(
            f"<b>{row['symbol']}</b> "
            f"{row['side']}\n"
            f"Entry: "
            f"<code>{row['entry']:.8g}</code>\n"
            f"SL: "
            f"<code>{row['stop']:.8g}</code>\n"
            f"TP: "
            f"<code>{row['tp']:.8g}</code>\n"
            f"Qty: "
            f"<code>{row['qty']:.8g}</code>\n"
        )

    await update.message.reply_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML
    )


async def stats_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not authorized(update):
        await deny(update)
        return

    stats = db_stats()

    await update.message.reply_text(
        f"📋 <b>BOT STATS</b>\n\n"
        f"Trades: <b>{stats['total']}</b>\n"
        f"Wins: <b>{stats['wins']}</b>\n"
        f"Losses: <b>{stats['losses']}</b>\n"
        f"Win rate: <b>{stats['win_rate']:.1f}%</b>\n\n"
        f"Net PnL: "
        f"<b>{stats['pnl']:+.2f} USDT</b>\n"
        f"Average win: "
        f"<b>{stats['avg_win']:+.2f}</b>\n"
        f"Average loss: "
        f"<b>{stats['avg_loss']:+.2f}</b>",
        parse_mode=ParseMode.HTML
    )


async def signals_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not authorized(update):
        await deny(update)
        return

    await update.message.reply_text(
        "🔎 Scanning market..."
    )

    candidates = []

    for symbol in VALID_SYMBOLS:

        try:

            signal = calculate_signal(
                symbol
            )

            if signal:
                candidates.append(
                    signal
                )

        except Exception:
            continue

    candidates.sort(
        key=lambda x: x["score"],
        reverse=True
    )

    if not candidates:

        await update.message.reply_text(
            "No valid high-score signals right now."
        )

        return

    lines = [
        "🔎 <b>TOP SIGNALS</b>\n"
    ]

    for signal in candidates[:7]:

        lines.append(
            f"{signal['symbol']} "
            f"<b>{signal['side']}</b> "
            f"score "
            f"<b>{signal['score']:.1f}</b>\n"
            f"RSI {signal['rsi']:.1f} | "
            f"ADX {signal['adx']:.1f} | "
            f"Vol {signal['volume_ratio']:.2f}x\n"
        )

    await update.message.reply_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML
    )


async def pause_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    global BOT_PAUSED

    if not authorized(update):
        await deny(update)
        return

    BOT_PAUSED = True

    set_state(
        "bot_paused",
        "1"
    )

    await update.message.reply_text(
        "⏸ <b>BOT PAUSED</b>\n\n"
        "No new trades will be opened.",
        parse_mode=ParseMode.HTML
    )


async def resume_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    global BOT_PAUSED
    global EMERGENCY_STOP

    if not authorized(update):
        await deny(update)
        return

    BOT_PAUSED = False
    EMERGENCY_STOP = False

    set_state(
        "bot_paused",
        "0"
    )

    set_state(
        "emergency_stop",
        "0"
    )

    await update.message.reply_text(
        "▶️ <b>BOT RESUMED</b>",
        parse_mode=ParseMode.HTML
    )


async def mode_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not authorized(update):
        await deny(update)
        return

    mode = (
        "🧪 PAPER"
        if not LIVE_TRADING
        else "🔴 LIVE"
    )

    await update.message.reply_text(
        f"Current mode: <b>{mode}</b>\n\n"
        f"LIVE_TRADING = "
        f"<code>{str(LIVE_TRADING).lower()}</code>",
        parse_mode=ParseMode.HTML
    )


async def risk_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not authorized(update):
        await deny(update)
        return

    await update.message.reply_text(
        f"⚙️ <b>RISK SETTINGS</b>\n\n"
        f"Risk/trade: "
        f"<b>{RISK_PER_TRADE*100:.2f}%</b>\n"
        f"Max margin/trade: "
        f"<b>{MAX_MARGIN_PER_TRADE*100:.1f}%</b>\n"
        f"Max total margin: "
        f"<b>{MAX_TOTAL_MARGIN*100:.1f}%</b>\n"
        f"Max positions: "
        f"<b>{MAX_OPEN_POSITIONS}</b>\n"
        f"Leverage: "
        f"<b>{LEVERAGE}x</b>\n"
        f"Daily DD lock: "
        f"<b>{MAX_DAILY_DRAWDOWN*100:.1f}%</b>",
        parse_mode=ParseMode.HTML
    )


async def health_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not authorized(update):
        await deny(update)
        return

    try:

        BINANCE.get_price(
            "BTCUSDT"
        )

        await update.message.reply_text(
            "🟢 <b>HEALTHY</b>\n\n"
            "Telegram: connected\n"
            "Binance API: connected\n"
            "Trading engine: running",
            parse_mode=ParseMode.HTML
        )

    except Exception as e:

        await update.message.reply_text(
            f"🔴 <b>HEALTH ERROR</b>\n\n"
            f"<code>{str(e)[:800]}</code>",
            parse_mode=ParseMode.HTML
        )


async def callback_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    global BOT_PAUSED
    global EMERGENCY_STOP

    query = update.callback_query

    if str(
        query.message.chat_id
    ) != ALLOWED_CHAT_ID:

        await query.answer(
            "Access denied",
            show_alert=True
        )

        return

    await query.answer()

    action = query.data

    if action == "status":

        await query.edit_message_text(
            status_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard()
        )

    elif action == "positions":

        rows = db_get_open_trades()

        if not rows:
            text = "📭 No open bot positions."

        else:

            parts = [
                "📈 <b>OPEN POSITIONS</b>\n"
            ]

            for row in rows:

                parts.append(
                    f"<b>{row['symbol']}</b> "
                    f"{row['side']}\n"
                    f"Entry: "
                    f"<code>{row['entry']:.8g}</code>\n"
                    f"SL: "
                    f"<code>{row['stop']:.8g}</code>\n"
                    f"TP: "
                    f"<code>{row['tp']:.8g}</code>\n"
                )

            text = "\n".join(parts)

        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard()
        )

    elif action == "stats":

        stats = db_stats()

        await query.edit_message_text(
            f"📋 <b>STATS</b>\n\n"
            f"Trades: {stats['total']}\n"
            f"Wins: {stats['wins']}\n"
            f"Losses: {stats['losses']}\n"
            f"Win rate: "
            f"{stats['win_rate']:.1f}%\n"
            f"PnL: "
            f"<b>{stats['pnl']:+.2f} USDT</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard()
        )

    elif action == "signals":

        await query.edit_message_text(
            "🔎 Scanning...",
            parse_mode=ParseMode.HTML
        )

        candidates = []

        for symbol in VALID_SYMBOLS:

            try:

                signal = calculate_signal(
                    symbol
                )

                if signal:
                    candidates.append(
                        signal
                    )

            except Exception:
                continue

        candidates.sort(
            key=lambda x: x["score"],
            reverse=True
        )

        if not candidates:

            text = (
                "🔎 No high-score signals."
            )

        else:

            lines = [
                "🔎 <b>TOP SIGNALS</b>\n"
            ]

            for signal in candidates[:7]:

                lines.append(
                    f"{signal['symbol']} "
                    f"<b>{signal['side']}</b> "
                    f"{signal['score']:.1f}/10+\n"
                    f"RSI {signal['rsi']:.1f} | "
                    f"ADX {signal['adx']:.1f} | "
                    f"Vol {signal['volume_ratio']:.2f}x\n"
                )

            text = "\n".join(lines)

        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard()
        )

    elif action == "pause":

        BOT_PAUSED = True

        set_state(
            "bot_paused",
            "1"
        )

        await query.edit_message_text(
            "⏸ <b>BOT PAUSED</b>\n\n"
            "No new trades will be opened.",
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard()
        )

    elif action == "resume":

        BOT_PAUSED = False
        EMERGENCY_STOP = False

        set_state(
            "bot_paused",
            "0"
        )

        set_state(
            "emergency_stop",
            "0"
        )

        await query.edit_message_text(
            "▶️ <b>BOT RESUMED</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard()
        )

    elif action == "emergency":

        EMERGENCY_STOP = True
        BOT_PAUSED = True

        set_state(
            "emergency_stop",
            "1"
        )

        set_state(
            "bot_paused",
            "1"
        )

        await query.edit_message_text(
            "🚨 <b>EMERGENCY STOP ACTIVE</b>\n\n"
            "New trades are disabled.\n\n"
            "Existing positions are NOT automatically "
            "closed by this button.\n\n"
            "Use CLOSE ALL if you want to close positions.",
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard()
        )

    elif action == "close_confirm":

        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "❌ YES, CLOSE ALL",
                    callback_data="close_execute"
                ),
                InlineKeyboardButton(
                    "↩️ Cancel",
                    callback_data="cancel"
                ),
            ]
        ])

        await query.edit_message_text(
            "⚠️ <b>ARE YOU SURE?</b>\n\n"
            "This will close all bot positions.",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

    elif action == "close_execute":

        await close_all_bot_positions()

        await query.edit_message_text(
            "💥 <b>CLOSE ALL EXECUTED</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard()
        )

    elif action == "cancel":

        await query.edit_message_text(
            status_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard()
        )


async def close_all_bot_positions():

    global BOT_PAUSED

    BOT_PAUSED = True

    rows = db_get_open_trades()

    if not rows:
        return

    if not LIVE_TRADING:

        for row in rows:

            try:

                price = BINANCE.get_price(
                    row["symbol"]
                )

                close_paper_trade(
                    row,
                    price,
                    "MANUAL CLOSE ALL"
                )

            except Exception as e:

                logger.error(
                    "Paper close error: %s",
                    e
                )

        return

    # LIVE
    for row in rows:

        try:

            amount = (
                get_position_amount(
                    row["symbol"]
                )
            )

            if abs(amount) <= 0:
                continue

            close_side = (
                "SELL"
                if amount > 0
                else "BUY"
            )

            BINANCE.place_order({
                "symbol": row["symbol"],
                "side": close_side,
                "type": "MARKET",
                "quantity": abs(amount),
                "reduceOnly": "true",
                "newOrderRespType": "RESULT",
            })

            try:
                BINANCE.cancel_all_orders(
                    row["symbol"]
                )
            except Exception:
                pass

            db_close_trade(
                row["id"],
                row["entry"],
                0,
                "MANUAL CLOSE ALL"
            )

        except Exception as e:

            logger.error(
                "Live close error %s: %s",
                row["symbol"],
                e
            )


# ============================================================
# TELEGRAM JOBS
# ============================================================

async def heartbeat_job(
    context: ContextTypes.DEFAULT_TYPE
):

    try:

        locked = daily_locked()

        if locked:

            previous = get_state(
                "daily_lock_notified",
                "0"
            )

            if previous != get_day_key():

                telegram_send(
                    "🛑 <b>DAILY RISK LOCK</b>\n\n"
                    f"Daily drawdown reached "
                    f"{MAX_DAILY_DRAWDOWN*100:.1f}%\n"
                    "New trades are disabled for today."
                )

                set_state(
                    "daily_lock_notified",
                    get_day_key()
                )

    except Exception as e:

        logger.error(
            "Heartbeat error: %s",
            e
        )


# ============================================================
# ERROR HANDLER
# ============================================================

async def telegram_error_handler(
    update: object,
    context: ContextTypes.DEFAULT_TYPE
):

    logger.exception(
        "Telegram error",
        exc_info=context.error
    )


# ============================================================
# STARTUP
# ============================================================

async def post_init(
    application: Application
):

    global TELEGRAM_LOOP
    global TELEGRAM_APP
    global TRADING_THREAD
    global BOT_PAUSED
    global EMERGENCY_STOP

    TELEGRAM_LOOP = asyncio.get_running_loop()
    TELEGRAM_APP = application

    BOT_PAUSED = (
        get_state(
            "bot_paused",
            "0"
        ) == "1"
    )

    EMERGENCY_STOP = (
        get_state(
            "emergency_stop",
            "0"
        ) == "1"
    )

    TRADING_THREAD = threading.Thread(
        target=trading_loop,
        daemon=True
    )

    TRADING_THREAD.start()

    application.job_queue.run_repeating(
        heartbeat_job,
        interval=300,
        first=30,
        name="heartbeat"
    )

    telegram_send(
        "🟢 <b>BOT ONLINE</b>\n\n"
        f"Mode: "
        f"<b>{'LIVE' if LIVE_TRADING else 'PAPER'}</b>\n"
        f"Leverage: <b>{LEVERAGE}x</b>\n"
        f"Risk/trade: "
        f"<b>{RISK_PER_TRADE*100:.2f}%</b>\n\n"
        "Use /start to open the control panel."
    )


async def post_shutdown(
    application: Application
):

    STOP_EVENT.set()

    telegram_send(
        "🔴 <b>BOT SHUTDOWN</b>"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    if not BINANCE_API_KEY:
        raise RuntimeError(
            "BINANCE_API_KEY is missing"
        )

    if not BINANCE_SECRET_KEY:
        raise RuntimeError(
            "BINANCE_SECRET_KEY is missing"
        )

    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN is missing"
        )

    if not TELEGRAM_CHAT_ID:
        raise RuntimeError(
            "TELEGRAM_CHAT_ID is missing"
        )

    init_db()

    application = (
        Application.builder()
        .token(
            TELEGRAM_BOT_TOKEN
        )
        .post_init(
            post_init
        )
        .post_shutdown(
            post_shutdown
        )
        .build()
    )

    application.add_handler(
        CommandHandler(
            "start",
            start_command
        )
    )

    application.add_handler(
        CommandHandler(
            "status",
            status_command
        )
    )

    application.add_handler(
        CommandHandler(
            "positions",
            positions_command
        )
    )

    application.add_handler(
        CommandHandler(
            "stats",
            stats_command
        )
    )

    application.add_handler(
        CommandHandler(
            "signals",
            signals_command
        )
    )

    application.add_handler(
        CommandHandler(
            "pause",
            pause_command
        )
    )

    application.add_handler(
        CommandHandler(
            "resume",
            resume_command
        )
    )

    application.add_handler(
        CommandHandler(
            "mode",
            mode_command
        )
    )

    application.add_handler(
        CommandHandler(
            "risk",
            risk_command
        )
    )

    application.add_handler(
        CommandHandler(
            "health",
            health_command
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            callback_handler
        )
    )

    application.add_error_handler(
        telegram_error_handler
    )

    logger.info(
        "Starting Telegram polling..."
    )

    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
