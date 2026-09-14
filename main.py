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
    ):
        gain = gains[i]
        loss = losses[i]

        avg_gain = (
            (avg_gain * (period - 1))
            + gain
        ) / period

        avg_loss = (
            (avg_loss * (period - 1))
            + loss
        ) / period

        if avg_loss == 0:
            result.append(100.0)
        else:
            rs = avg_gain / avg_loss
            result.append(
                100.0
                - (
                    100.0
                    / (1.0 + rs)
                )
            )

    return result


def true_range(high, low, close):
    if not high or not low or not close:
        return []

    result = []

    for i in range(len(close)):
        if i == 0:
            result.append(
                float(high[i])
                - float(low[i])
            )
        else:
            result.append(
                max(
                    float(high[i])
                    - float(low[i]),
                    abs(
                        float(high[i])
                        - float(close[i - 1])
                    ),
                    abs(
                        float(low[i])
                        - float(close[i - 1])
                    ),
                )
            )

    return result


def atr(
    high,
    low,
    close,
    period=14,
):
    tr = true_range(
        high,
        low,
        close,
    )

    if len(tr) < period:
        return []

    value = (
        sum(tr[:period])
        / period
    )

    result = [
        None
    ] * (period - 1)

    result.append(value)

    for i in range(
        period,
        len(tr),
    ):
        value = (
            (
                value
                * (period - 1)
            )
            + tr[i]
        ) / period

        result.append(value)

    return result


def adx(
    high,
    low,
    close,
    period=14,
):
    if len(close) < (
        period * 2
        + 1
    ):
        return []

    tr = []
    plus_dm = []
    minus_dm = []

    for i in range(
        1,
        len(close),
    ):
        current_high = float(
            high[i]
        )
        previous_high = float(
            high[i - 1]
        )

        current_low = float(
            low[i]
        )
        previous_low = float(
            low[i - 1]
        )

        current_close = float(
            close[i]
        )

        previous_close = float(
            close[i - 1]
        )

        tr_value = max(
            current_high
            - current_low,

            abs(
                current_high
                - previous_close
            ),

            abs(
                current_low
                - previous_close
            ),
        )

        up_move = (
            current_high
            - previous_high
        )

        down_move = (
            previous_low
            - current_low
        )

        if (
            up_move > down_move
            and up_move > 0
        ):
            pdm = up_move
        else:
            pdm = 0.0

        if (
            down_move > up_move
            and down_move > 0
        ):
            mdm = down_move
        else:
            mdm = 0.0

        tr.append(tr_value)
        plus_dm.append(pdm)
        minus_dm.append(mdm)

    if len(tr) < period:
        return []

    smoothed_tr = (
        sum(tr[:period])
    )

    smoothed_plus = (
        sum(plus_dm[:period])
    )

    smoothed_minus = (
        sum(minus_dm[:period])
    )

    dx_values = []

    for i in range(
        period,
        len(tr),
    ):
        if i > period:
            smoothed_tr = (
                smoothed_tr
                - (
                    smoothed_tr
                    / period
                )
                + tr[i]
            )

            smoothed_plus = (
                smoothed_plus
                - (
                    smoothed_plus
                    / period
                )
                + plus_dm[i]
            )

            smoothed_minus = (
                smoothed_minus
                - (
                    smoothed_minus
                    / period
                )
                + minus_dm[i]
            )

        if smoothed_tr == 0:
            dx_values.append(0.0)
            continue

        plus_di = (
            100.0
            * smoothed_plus
            / smoothed_tr
        )

        minus_di = (
            100.0
            * smoothed_minus
            / smoothed_tr
        )

        denominator = (
            plus_di
            + minus_di
        )

        if denominator == 0:
            dx = 0.0
        else:
            dx = (
                100.0
                * abs(
                    plus_di
                    - minus_di
                )
                / denominator
            )

        dx_values.append(dx)

    if len(dx_values) < period:
        return []

    adx_values = [
        None
    ] * (
        len(close)
        - len(dx_values)
        - 1
    )

    first_adx = (
        sum(
            dx_values[:period]
        )
        / period
    )

    adx_values.append(
        first_adx
    )

    current_adx = first_adx

    for i in range(
        period,
        len(dx_values),
    ):
        current_adx = (
            (
                current_adx
                * (period - 1)
            )
            + dx_values[i]
        ) / period

        adx_values.append(
            current_adx
        )

    return adx_values


# ============================================================
# DATA HELPERS
# ============================================================

def safe_float(
    value,
    default=0.0,
):
    try:
        return float(value)
    except Exception:
        return default


def now_utc():
    return datetime.now(
        timezone.utc
    )


def now_ts():
    return int(
        time.time()
    )


def today_utc():
    return now_utc().strftime(
        "%Y-%m-%d"
    )


def clamp(
    value,
    minimum,
    maximum,
):
    return max(
        minimum,
        min(
            maximum,
            value,
        ),
    )


def round_down(
    value,
    step,
):
    if step <= 0:
        return value

    return (
        math.floor(
            value / step
            + 1e-12
        )
        * step
    )


def round_to_step(
    value,
    step,
):
    if step <= 0:
        return value

    return (
        round_down(
            value,
            step,
        )
    )


def format_number(
    value,
    decimals=8,
):
    return f"{float(value):.{decimals}f}".rstrip(
        "0"
    ).rstrip(".")


# ============================================================
# DATABASE
# ============================================================

DB_PATH = "bot.db"


def db_connect():
    conn = sqlite3.connect(
        DB_PATH,
        check_same_thread=False,
    )

    conn.row_factory = (
        sqlite3.Row
    )

    return conn


def init_db():
    conn = db_connect()

    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS
        trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            side TEXT,
            entry REAL,
            stop REAL,
            target REAL,
            qty REAL,
            status TEXT,
            pnl REAL DEFAULT 0,
            r_multiple REAL DEFAULT 0,
            opened_at TEXT,
            closed_at TEXT,
            reason TEXT
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS
        state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )

    conn.commit()
    conn.close()


def state_get(
    key,
    default=None,
):
    conn = db_connect()

    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT value
        FROM state
        WHERE key = ?
        """,
        (key,),
    )

    row = cursor.fetchone()

    conn.close()

    if row is None:
        return default

    return row["value"]


def state_set(
    key,
    value,
):
    conn = db_connect()

    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO state (
            key,
            value
        )
        VALUES (?, ?)
        ON CONFLICT(key)
        DO UPDATE SET
            value = excluded.value
        """,
        (
            key,
            str(value),
        ),
    )

    conn.commit()
    conn.close()


def add_trade(
    symbol,
    side,
    entry,
    stop,
    target,
    qty,
    status="OPEN",
):
    conn = db_connect()

    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO trades (
            symbol,
            side,
            entry,
            stop,
            target,
            qty,
            status,
            opened_at
        )
        VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            symbol,
            side,
            entry,
            stop,
            target,
            qty,
            status,
            now_utc().isoformat(),
        ),
    )

    trade_id = cursor.lastrowid

    conn.commit()
    conn.close()

    return trade_id


def close_trade(
    trade_id,
    pnl=0.0,
    r_multiple=0.0,
    reason="UNKNOWN",
):
    conn = db_connect()

    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE trades
        SET
            status = 'CLOSED',
            pnl = ?,
            r_multiple = ?,
            closed_at = ?,
            reason = ?
        WHERE id = ?
        """,
        (
            pnl,
            r_multiple,
            now_utc().isoformat(),
            reason,
            trade_id,
        ),
    )

    conn.commit()
    conn.close()


def get_open_trades():
    conn = db_connect()

    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM trades
        WHERE status = 'OPEN'
        ORDER BY id DESC
        """
    )

    rows = cursor.fetchall()

    conn.close()

    return rows


def get_recent_trades(
    limit=20,
):
    conn = db_connect()

    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM trades
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    )

    rows = cursor.fetchall()

    conn.close()

    return rows


# ============================================================
# PAPER ACCOUNT
# ============================================================

def get_paper_equity():
    value = state_get(
        "paper_equity"
    )

    if value is None:
        state_set(
            "paper_equity",
            PAPER_START_EQUITY,
        )

        return float(
            PAPER_START_EQUITY
        )

    return safe_float(
        value,
        PAPER_START_EQUITY,
    )


def set_paper_equity(
    value,
):
    state_set(
        "paper_equity",
        round(
            float(value),
            8,
        ),
    )


def get_day_start_equity():
    today = today_utc()

    stored_day = state_get(
        "equity_day"
    )

    if stored_day != today:
        equity = get_account_equity()

        state_set(
            "equity_day",
            today,
        )

        state_set(
            "day_start_equity",
            equity,
        )

        return equity

    stored = state_get(
        "day_start_equity"
    )

    if stored is None:
        equity = get_account_equity()

        state_set(
            "day_start_equity",
            equity,
        )

        return equity

    return safe_float(
        stored,
        get_account_equity(),
    )


# ============================================================
# BINANCE API
# ============================================================

SESSION = requests.Session()

EXCHANGE_INFO_CACHE = None
EXCHANGE_INFO_TIME = 0

SYMBOL_RULES = {}


def public_get(
    path,
    params=None,
    timeout=15,
):
    url = (
        BASE_URL
        + path
    )

    response = SESSION.get(
        url,
        params=params or {},
        timeout=timeout,
    )

    response.raise_for_status()

    return response.json()


def sign_params(
    params,
):
    params = dict(
        params or {}
    )

    params[
        "timestamp"
    ] = now_ts() * 1000

    params[
        "recvWindow"
    ] = RECV_WINDOW

    query = urlencode(
        params,
        doseq=True,
    )

    signature = hmac.new(
        BINANCE_SECRET_KEY.encode(),
        query.encode(),
        hashlib.sha256,
    ).hexdigest()

    params[
        "signature"
    ] = signature

    return params


def signed_request(
    method,
    path,
    params=None,
    timeout=15,
):
    if not BINANCE_API_KEY:
        raise RuntimeError(
            "BINANCE_API_KEY is missing"
        )

    if not BINANCE_SECRET_KEY:
        raise RuntimeError(
            "BINANCE_SECRET_KEY is missing"
        )

    headers = {
        "X-MBX-APIKEY":
            BINANCE_API_KEY
    }

    signed = sign_params(
        params
    )

    url = (
        BASE_URL
        + path
    )

    method = method.upper()

    if method == "GET":
        response = SESSION.get(
            url,
            params=signed,
            headers=headers,
            timeout=timeout,
        )

    elif method == "POST":
        response = SESSION.post(
            url,
            params=signed,
            headers=headers,
            timeout=timeout,
        )

    elif method == "DELETE":
        response = SESSION.delete(
            url,
            params=signed,
            headers=headers,
            timeout=timeout,
        )

    else:
        raise ValueError(
            f"Unsupported method: {method}"
        )

    if not response.ok:
        raise RuntimeError(
            f"Binance API "
            f"{response.status_code}: "
            f"{response.text[:500]}"
        )

    return response.json()


def get_klines(
    symbol,
    interval,
    limit=250,
):
    data = public_get(
        "/fapi/v1/klines",
        {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        },
    )

    candles = []

    for row in data:
        candles.append(
            {
                "open_time": int(
                    row[0]
                ),
                "open": safe_float(
                    row[1]
                ),
                "high": safe_float(
                    row[2]
                ),
                "low": safe_float(
                    row[3]
                ),
                "close": safe_float(
                    row[4]
                ),
                "volume": safe_float(
                    row[5]
                ),
                "close_time": int(
                    row[6]
                ),
            }
        )

    return candles


def get_ticker_price(
    symbol,
):
    data = public_get(
        "/fapi/v1/ticker/price",
        {
            "symbol": symbol
        },
    )

    return safe_float(
        data.get("price")
    )


def get_funding_rate(
    symbol,
):
    data = public_get(
        "/fapi/v1/premiumIndex",
        {
            "symbol": symbol
        },
    )

    return safe_float(
        data.get(
            "lastFundingRate"
        )
    )


def get_exchange_info():
    global EXCHANGE_INFO_CACHE
    global EXCHANGE_INFO_TIME

    if (
        EXCHANGE_INFO_CACHE
        and (
            time.time()
            - EXCHANGE_INFO_TIME
            < 3600
        )
    ):
        return (
            EXCHANGE_INFO_CACHE
        )

    data = public_get(
        "/fapi/v1/exchangeInfo"
    )

    EXCHANGE_INFO_CACHE = data
    EXCHANGE_INFO_TIME = (
        time.time()
    )

    SYMBOL_RULES.clear()

    for symbol_info in data.get(
        "symbols",
        [],
    ):
        symbol = symbol_info.get(
            "symbol"
        )

        if not symbol:
            continue

        filters = {}

        for item in symbol_info.get(
            "filters",
            [],
        ):
            filters[
                item.get("filterType")
            ] = item

        SYMBOL_RULES[
            symbol
        ] = {
            "status":
                symbol_info.get(
                    "status"
                ),
            "quoteAsset":
                symbol_info.get(
                    "quoteAsset"
                ),
            "baseAsset":
                symbol_info.get(
                    "baseAsset"
                ),
            "filters":
                filters,
        }

    return data


def get_symbol_rules(
    symbol,
):
    get_exchange_info()

    rules = SYMBOL_RULES.get(
        symbol
    )

    if not rules:
        raise RuntimeError(
            f"No exchange rules "
            f"for {symbol}"
        )

    return rules


def get_symbol_filters(
    symbol,
):
    rules = get_symbol_rules(
        symbol
    )

    return rules[
        "filters"
    ]


def get_step_size(
    symbol,
):
    filters = get_symbol_filters(
        symbol
    )

    lot = filters.get(
        "LOT_SIZE",
        {},
    )

    return safe_float(
        lot.get(
            "stepSize"
        ),
        0.0,
    )


def get_min_qty(
    symbol,
):
    filters = get_symbol_filters(
        symbol
    )

    lot = filters.get(
        "LOT_SIZE",
        {},
    )

    return safe_float(
        lot.get(
            "minQty"
        ),
        0.0,
    )


def get_max_qty(
    symbol,
):
    filters = get_symbol_filters(
        symbol
    )

    lot = filters.get(
        "LOT_SIZE",
        {},
    )

    return safe_float(
        lot.get(
            "maxQty"
        ),
        0.0,
    )


def get_tick_size(
    symbol,
):
    filters = get_symbol_filters(
        symbol
    )

    price_filter = filters.get(
        "PRICE_FILTER",
        {},
    )

    return safe_float(
        price_filter.get(
            "tickSize"
        ),
        0.0,
    )


def get_min_notional(
    symbol,
):
    filters = get_symbol_filters(
        symbol
    )

    for key in (
        "NOTIONAL",
        "MIN_NOTIONAL",
    ):
        item = filters.get(
            key
        )

        if item:
            return safe_float(
                item.get(
                    "minNotional"
                ),
                0.0,
            )

    return 0.0


def normalize_quantity(
    symbol,
    quantity,
):
    step = get_step_size(
        symbol
    )

    minimum = get_min_qty(
        symbol
    )

    maximum = get_max_qty(
        symbol
    )

    if step <= 0:
        return float(
            quantity
        )

    quantity = round_down(
        float(quantity),
        step,
    )

    if minimum > 0:
        quantity = max(
            quantity,
            minimum,
        )

    if maximum > 0:
        quantity = min(
            quantity,
            maximum,
        )

    return quantity


def normalize_price(
    symbol,
    price,
):
    tick = get_tick_size(
        symbol
    )

    if tick <= 0:
        return float(
            price
        )

    return round_to_step(
        float(price),
        tick,
    )


# ============================================================
# ACCOUNT
# ============================================================

def get_account_equity():
    if not LIVE_TRADING:
        return get_paper_equity()

    data = signed_request(
        "GET",
        "/fapi/v2/account",
    )

    return safe_float(
        data.get(
            "totalWalletBalance"
        ),
        0.0,
    )


def get_available_balance():
    if not LIVE_TRADING:
        return get_paper_equity()

    data = signed_request(
        "GET",
        "/fapi/v2/account",
    )

    return safe_float(
        data.get(
            "availableBalance"
        ),
        0.0,
    )


def get_positions():
    if not LIVE_TRADING:
        return []

    return signed_request(
        "GET",
        "/fapi/v2/positionRisk",
    )


def get_open_live_positions():
    positions = get_positions()

    result = []

    for position in positions:
        amount = safe_float(
            position.get(
                "positionAmt"
            )
        )

        if abs(amount) <= 0:
            continue

        result.append(
            position
        )

    return result


def get_live_position(
    symbol,
):
    positions = get_positions()

    for position in positions:
        if (
            position.get(
                "symbol"
            )
            == symbol
        ):
            amount = safe_float(
                position.get(
                    "positionAmt"
                )
            )

            if abs(amount) > 0:
                return position

    return None


def get_position_side(
    position,
):
    amount = safe_float(
        position.get(
            "positionAmt"
        )
    )

    if amount > 0:
        return "LONG"

    if amount < 0:
        return "SHORT"

    return None


# ============================================================
# ORDER HELPERS
# ============================================================

def set_leverage(
    symbol,
    leverage,
):
    return signed_request(
        "POST",
        "/fapi/v1/leverage",
        {
            "symbol": symbol,
            "leverage": int(
                leverage
            ),
        },
    )


def set_isolated_margin(
    symbol,
):
    try:
        return signed_request(
            "POST",
            "/fapi/v1/marginType",
            {
                "symbol": symbol,
                "marginType":
                    "ISOLATED",
            },
        )
    except Exception as exc:
        text = str(exc)

        if (
            "-4046" in text
            or "No need to change"
            in text
        ):
            return {
                "status":
                    "already_isolated"
            }

        raise


def place_market_order(
    symbol,
    side,
    quantity,
):
    return signed_request(
        "POST",
        "/fapi/v1/order",
        {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity":
                format_number(
                    quantity
                ),
            "newOrderRespType":
                "RESULT",
        },
    )


def place_stop_order(
    symbol,
    side,
    stop_price,
):
    return signed_request(
        "POST",
        "/fapi/v1/order",
        {
            "symbol": symbol,
            "side": side,
            "type":
                "STOP_MARKET",
            "stopPrice":
                format_number(
                    stop_price
                ),
            "closePosition":
                "true",
            "workingType":
                "MARK_PRICE",
            "newClientOrderId":
                f"{CLIENT_PREFIX}"
                f"SL_"
                f"{uuid.uuid4().hex[:10]}",
        },
    )


def place_take_profit_order(
    symbol,
    side,
    stop_price,
):
    return signed_request(
        "POST",
        "/fapi/v1/order",
        {
            "symbol": symbol,
            "side": side,
            "type":
                "TAKE_PROFIT_MARKET",
            "stopPrice":
                format_number(
                    stop_price
                ),
            "closePosition":
                "true",
            "workingType":
                "MARK_PRICE",
            "newClientOrderId":
                f"{CLIENT_PREFIX}"
                f"TP_"
                f"{uuid.uuid4().hex[:10]}",
        },
    )


def get_open_orders(
    symbol=None,
):
    params = {}

    if symbol:
        params[
            "symbol"
        ] = symbol

    return signed_request(
        "GET",
        "/fapi/v1/openOrders",
        params,
    )


def cancel_order(
    symbol,
    order_id,
):
    return signed_request(
        "DELETE",
        "/fapi/v1/order",
        {
            "symbol": symbol,
            "orderId": order_id,
        },
    )


def cancel_bot_orders(
    symbol,
):
    orders = get_open_orders(
        symbol
    )

    for order in orders:
        client_id = order.get(
            "clientOrderId",
            "",
        )

        if client_id.startswith(
            CLIENT_PREFIX
        ):
            try:
                cancel_order(
                    symbol,
                    order.get(
                        "orderId"
                    ),
                )
            except Exception:
                pass


# ============================================================
# SIGNAL ENGINE
# ============================================================

def prepare_candles(
    candles,
):
    if not candles:
        return None

    # Remove currently forming candle.
    if len(candles) >= 2:
        candles = candles[:-1]

    if len(candles) < 60:
        return None

    closes = [
        c["close"]
        for c in candles
    ]

    highs = [
        c["high"]
        for c in candles
    ]

    lows = [
        c["low"]
        for c in candles
    ]

    volumes = [
        c["volume"]
        for c in candles
    ]

    return {
        "closes": closes,
        "highs": highs,
        "lows": lows,
        "volumes": volumes,
    }


def average(
    values,
):
    if not values:
        return 0.0

    return (
        sum(values)
        / len(values)
    )


def build_signal(
    symbol,
):
    try:
        candles_15m = get_klines(
            symbol,
            "15m",
            220,
        )

        candles_1h = get_klines(
            symbol,
            "1h",
            220,
        )

        data15 = prepare_candles(
            candles_15m
        )

        data1h = prepare_candles(
            candles_1h
        )

        if not data15 or not data1h:
            return None

        close15 = data15[
            "closes"
        ]

        high15 = data15[
            "highs"
        ]

        low15 = data15[
            "lows"
        ]

        volume15 = data15[
            "volumes"
        ]

        close1h = data1h[
            "closes"
        ]

        ema20_15 = ema(
            close15,
            20,
        )

        ema50_15 = ema(
            close15,
            50,
        )

        ema20_1h = ema(
            close1h,
            20,
        )

        ema50_1h = ema(
            close1h,
            50,
        )

        rsi15 = rsi(
            close15,
            14,
        )

        atr15 = atr(
            high15,
            low15,
            close15,
            14,
        )

        adx15 = adx(
            high15,
            low15,
            close15,
            14,
        )

        if not (
            ema20_15
            and ema50_15
            and ema20_1h
            and ema50_1h
            and rsi15
            and atr15
            and adx15
        ):
            return None

        last_close = close15[-1]

        previous_close = (
            close15[-2]
        )

        last_ema20_15 = (
            ema20_15[-1]
        )

        last_ema50_15 = (
            ema50_15[-1]
        )

        last_ema20_1h = (
            ema20_1h[-1]
        )

        last_ema50_1h = (
            ema50_1h[-1]
        )

        last_rsi = rsi15[-1]

        last_atr = atr15[-1]

        last_adx = adx15[-1]

        if (
            last_atr is None
            or last_adx is None
        ):
            return None

        atr_pct = (
            last_atr
            / last_close
        )

        volume_avg = average(
            volume15[-21:-1]
        )

        if volume_avg <= 0:
            return None

        volume_ratio = (
            volume15[-1]
            / volume_avg
        )

        funding = get_funding_rate(
            symbol
        )

        # Funding returned by Binance
        # is a decimal rate.
        funding_pct = (
            funding * 100.0
        )

        long_score = 0
        short_score = 0

        reasons_long = []
        reasons_short = []

        # 1H trend
        if (
            last_ema20_1h
            > last_ema50_1h
        ):
            long_score += 2
            reasons_long.append(
                "1H uptrend"
            )

        elif (
            last_ema20_1h
            < last_ema50_1h
        ):
            short_score += 2
            reasons_short.append(
                "1H downtrend"
            )

        # 15M trend
        if (
            last_ema20_15
            > last_ema50_15
        ):
            long_score += 2
            reasons_long.append(
                "15M uptrend"
            )

        elif (
            last_ema20_15
            < last_ema50_15
        ):
            short_score += 2
            reasons_short.append(
                "15M downtrend"
            )

        # RSI pullback
        if (
            42
            <= last_rsi
            <= 56
            and last_close
            > previous_close
        ):
            long_score += 2
            reasons_long.append(
                "RSI pullback"
            )

        if (
            44
            <= last_rsi
            <= 58
            and last_close
            < previous_close
        ):
            short_score += 2
            reasons_short.append(
                "RSI pullback"
            )

        # Candle confirmation
        if last_close > previous_close:
            long_score += 1
            reasons_long.append(
                "bullish close"
            )

        elif last_close < previous_close:
            short_score += 1
            reasons_short.append(
                "bearish close"
            )

        # Volume
        if volume_ratio >= 0.85:
            long_score += 1
            short_score += 1

        # Funding extremes
        if funding_pct <= -0.02:
            long_score += 1
            reasons_long.append(
                "negative funding"
            )

        elif funding_pct >= 0.05:
            short_score += 1
            reasons_short.append(
                "high funding"
            )

        # Market condition filters
        if not (
            0.0025
            <= atr_pct
            <= 0.04
        ):
            return None

        if last_adx < 18:
            return None

        if volume_ratio < 0.85:
            return None

        direction = None
        score = 0
        reasons = []

        if (
            long_score
            >= MIN_SCORE
            and long_score
            > short_score
        ):
            direction = "LONG"
            score = long_score
            reasons = reasons_long

        elif (
            short_score
            >= MIN_SCORE
            and short_score
            > long_score
        ):
            direction = "SHORT"
            score = short_score
            reasons = reasons_short

        else:
            return None

        return {
            "symbol": symbol,
            "direction": direction,
            "score": score,
            "price": last_close,
            "atr": last_atr,
            "atr_pct": atr_pct,
            "adx": last_adx,
            "rsi": last_rsi,
            "volume_ratio":
                volume_ratio,
            "funding":
                funding,
            "funding_pct":
                funding_pct,
            "reasons": reasons,
        }

    except Exception as exc:
        log(
            "Signal error "
            f"{symbol}: {exc}"
        )

        return None

# ============================================================
# RISK ENGINE
# ============================================================

def calculate_stop_distance(
    signal,
):
    atr_value = signal[
        "atr"
    ]

    price = signal[
        "price"
    ]

    raw = (
        atr_value
        * ATR_STOP_MULT
    )

    stop_pct = (
        raw / price
    )

    stop_pct = clamp(
        stop_pct,
        MIN_STOP_PCT,
        MAX_STOP_PCT,
    )

    return stop_pct


def calculate_levels(
    direction,
    entry,
    stop_pct,
):
    if direction == "LONG":
        stop = (
            entry
            * (1.0 - stop_pct)
        )

        risk_distance = (
            entry - stop
        )

        target = (
            entry
            + risk_distance
            * REWARD_R
        )

    else:
        stop = (
            entry
            * (1.0 + stop_pct)
        )

        risk_distance = (
            stop - entry
        )

        target = (
            entry
            - risk_distance
            * REWARD_R
        )

    return (
        stop,
        target,
        risk_distance,
    )


def calculate_quantity(
    symbol,
    entry,
    stop_pct,
):
    equity = get_account_equity()

    if equity <= 0:
        return 0.0

    risk_amount = (
        equity
        * RISK_PER_TRADE
    )

    stop_pct = max(
        stop_pct,
        0.0001,
    )

    notional = (
        risk_amount
        / stop_pct
    )

    max_notional = (
        equity
        * MAX_MARGIN_PER_TRADE
        * LEVERAGE
    )

    notional = min(
        notional,
        max_notional,
    )

    quantity = (
        notional
        / entry
    )

    quantity = normalize_quantity(
        symbol,
        quantity,
    )

    min_notional = (
        get_min_notional(
            symbol
        )
    )

    if (
        min_notional > 0
        and quantity * entry
        < min_notional
    ):
        quantity = normalize_quantity(
            symbol,
            (
                min_notional
                / entry
            )
            + get_step_size(
                symbol
            ),
        )

    max_qty = get_max_qty(
        symbol
    )

    if (
        max_qty > 0
        and quantity > max_qty
    ):
        quantity = max_qty

    return quantity


def current_margin_used():
    if not LIVE_TRADING:
        total = 0.0

        for trade in get_open_trades():
            total += (
                abs(
                    safe_float(
                        trade["qty"]
                    )
                )
                * safe_float(
                    trade["entry"]
                )
                / LEVERAGE
            )

        return total

    positions = (
        get_open_live_positions()
    )

    total = 0.0

    for position in positions:
        notional = abs(
            safe_float(
                position.get(
                    "notional"
                )
            )
        )

        total += (
            notional
            / LEVERAGE
        )

    return total


def can_open_new_trade(
    symbol,
):
    if BOT_PAUSED:
        return False, (
            "bot paused"
        )

    if daily_drawdown_hit():
        return False, (
            "daily drawdown limit"
        )

    if global_cooldown_active():
        return False, (
            "global cooldown"
        )

    if symbol_cooldown_active(
        symbol
    ):
        return False, (
            "symbol cooldown"
        )

    if (
        count_open_positions()
        >= MAX_OPEN_POSITIONS
    ):
        return False, (
            "max positions"
        )

    equity = get_account_equity()

    if equity <= 0:
        return False, (
            "no equity"
        )

    margin = (
        current_margin_used()
    )

    max_total_margin = (
        equity
        * MAX_TOTAL_MARGIN
    )

    if (
        margin
        >= max_total_margin
    ):
        return False, (
            "max total margin"
        )

    return True, "ok"


# ============================================================
# COOLDOWNS / RISK LIMITS
# ============================================================

def count_open_positions():
    if not LIVE_TRADING:
        return len(
            get_open_trades()
        )

    return len(
        get_open_live_positions()
    )


def symbol_cooldown_active(
    symbol,
):
    key = (
        f"cooldown_{symbol}"
    )

    value = state_get(
        key
    )

    if not value:
        return False

    last_time = safe_float(
        value,
        0.0,
    )

    elapsed = (
        time.time()
        - last_time
    )

    return (
        elapsed
        < COOLDOWN_MINUTES * 60
    )


def set_symbol_cooldown(
    symbol,
):
    state_set(
        f"cooldown_{symbol}",
        time.time(),
    )


def global_cooldown_active():
    value = state_get(
        "last_entry_time"
    )

    if not value:
        return False

    elapsed = (
        time.time()
        - safe_float(
            value,
            0.0,
        )
    )

    return (
        elapsed
        < GLOBAL_ENTRY_COOLDOWN_MINUTES
        * 60
    )


def set_global_cooldown():
    state_set(
        "last_entry_time",
        time.time(),
    )


def daily_drawdown_hit():
    start_equity = (
        get_day_start_equity()
    )

    current_equity = (
        get_account_equity()
    )

    if start_equity <= 0:
        return True

    drawdown = (
        (
            start_equity
            - current_equity
        )
        / start_equity
    )

    return (
        drawdown
        >= MAX_DAILY_DRAWDOWN
    )

# ============================================================
# PAPER TRADING
# ============================================================

def paper_open_trade(
    signal,
):
    symbol = signal[
        "symbol"
    ]

    direction = signal[
        "direction"
    ]

    entry = signal[
        "price"
    ]

    stop_pct = (
        calculate_stop_distance(
            signal
        )
    )

    stop, target, risk_distance = (
        calculate_levels(
            direction,
            entry,
            stop_pct,
        )
    )

    quantity = (
        calculate_quantity(
            symbol,
            entry,
            stop_pct,
        )
    )

    if quantity <= 0:
        return False

    side = direction

    trade_id = add_trade(
        symbol=symbol,
        side=side,
        entry=entry,
        stop=stop,
        target=target,
        qty=quantity,
        status="OPEN",
    )

    set_global_cooldown()

    set_symbol_cooldown(
        symbol
    )

    telegram_send(
        "🟢 PAPER ENTRY\n"
        f"{symbol}\n"
        f"{direction}\n"
        f"Entry: {entry:.6f}\n"
        f"SL: {stop:.6f}\n"
        f"TP: {target:.6f}\n"
        f"Qty: {quantity}\n"
        f"Score: {signal['score']}\n"
        f"RSI: {signal['rsi']:.1f}\n"
        f"ADX: {signal['adx']:.1f}\n"
        f"Funding: "
        f"{signal['funding_pct']:.4f}%\n"
        f"Reason: "
        f"{', '.join(signal['reasons'])}"
    )

    log(
        f"PAPER ENTRY "
        f"{symbol} "
        f"{direction} "
        f"entry={entry} "
        f"sl={stop} "
        f"tp={target}"
    )

    return True


def paper_close_trade(
    trade,
    exit_price,
    reason,
):
    entry = safe_float(
        trade["entry"]
    )

    stop = safe_float(
        trade["stop"]
    )

    quantity = safe_float(
        trade["qty"]
    )

    side = trade["side"]

    if side == "LONG":
        pnl = (
            exit_price
            - entry
        ) * quantity

        risk = (
            entry
            - stop
        ) * quantity

    else:
        pnl = (
            entry
            - exit_price
        ) * quantity

        risk = (
            stop
            - entry
        ) * quantity

    r_multiple = (
        pnl / risk
        if risk > 0
        else 0.0
    )

    equity = (
        get_paper_equity()
    )

    equity += pnl

    set_paper_equity(
        equity
    )

    close_trade(
        trade["id"],
        pnl=pnl,
        r_multiple=r_multiple,
        reason=reason,
    )

    telegram_send(
        "🔴 PAPER EXIT\n"
        f"{trade['symbol']}\n"
        f"Reason: {reason}\n"
        f"Entry: {entry:.6f}\n"
        f"Exit: {exit_price:.6f}\n"
        f"PnL: {pnl:+.2f} USDT\n"
        f"R: {r_multiple:+.2f}\n"
        f"Equity: {equity:.2f}"
    )

    log(
        f"PAPER EXIT "
        f"{trade['symbol']} "
        f"{reason} "
        f"pnl={pnl:.4f}"
    )


def manage_paper_trades():
    trades = (
        get_open_trades()
    )

    for trade in trades:
        symbol = trade[
            "symbol"
        ]

        price = (
            get_ticker_price(
                symbol
            )
        )

        if price <= 0:
            continue

        entry = safe_float(
            trade["entry"]
        )

        stop = safe_float(
            trade["stop"]
        )

        target = safe_float(
            trade["target"]
        )

        side = trade[
            "side"
        ]

        risk_distance = abs(
            entry - stop
        )

        if risk_distance <= 0:
            continue

        if side == "LONG":
            current_r = (
                price - entry
            ) / risk_distance

            if price <= stop:
                paper_close_trade(
                    trade,
                    stop,
                    "STOP",
                )
                continue

            if price >= target:
                paper_close_trade(
                    trade,
                    target,
                    "TAKE_PROFIT",
                )
                continue

        else:
            current_r = (
                entry - price
            ) / risk_distance

            if price >= stop:
                paper_close_trade(
                    trade,
                    stop,
                    "STOP",
                )
                continue

            if price <= target:
                paper_close_trade(
                    trade,
                    target,
                    "TAKE_PROFIT",
                )
                continue

        # Move stop to protected profit
        # after +1R.
        if (
            current_r
            >= BREAK_EVEN_R
        ):
            if side == "LONG":
                new_stop = max(
                    stop,
                    entry
                    * (
                        1.0
                        + BE_LOCK_PCT
                    ),
                )

            else:
                new_stop = min(
                    stop,
                    entry
                    * (
                        1.0
                        - BE_LOCK_PCT
                    ),
                )

            if (
                new_stop
                != stop
            ):
                conn = db_connect()

                cursor = conn.cursor()

                cursor.execute(
                    """
                    UPDATE trades
                    SET stop = ?
                    WHERE id = ?
                    """,
                    (
                        new_stop,
                        trade["id"],
                    ),
                )

                conn.commit()
                conn.close()

                stop = new_stop

        # ATR trailing after +1.5R.
        if (
            current_r
            >= TRAIL_START_R
        ):
            candles = (
                get_klines(
                    symbol,
                    "15m",
                    80,
                )
            )

            prepared = (
                prepare_candles(
                    candles
                )
            )

            if prepared:
                atr_values = atr(
                    prepared[
                        "highs"
                    ],
                    prepared[
                        "lows"
                    ],
                    prepared[
                        "closes"
                    ],
                    14,
                )

                if (
                    atr_values
                    and atr_values[-1]
                    is not None
                ):
                    trail_atr = (
                        atr_values[-1]
                        * TRAIL_ATR_MULT
                    )

                    if side == "LONG":
                        trail_stop = (
                            price
                            - trail_atr
                        )

                        new_stop = max(
                            stop,
                            trail_stop,
                        )

                    else:
                        trail_stop = (
                            price
                            + trail_atr
                        )

                        new_stop = min(
                            stop,
                            trail_stop,
                        )

                    if (
                        new_stop
                        != stop
                    ):
                        conn = db_connect()

                        cursor = conn.cursor()

                        cursor.execute(
                            """
                            UPDATE trades
                            SET stop = ?
                            WHERE id = ?
                            """,
                            (
                                new_stop,
                                trade["id"],
                            ),
                        )

                        conn.commit()
                        conn.close()

        # Refresh the loop after
        # possible stop movement.

# ============================================================
# LIVE TRADING
# ============================================================

def wait_for_live_position(
    symbol,
    attempts=10,
    delay=0.5,
):
    for _ in range(
        attempts
    ):
        position = (
            get_live_position(
                symbol
            )
        )

        if position:
            entry = safe_float(
                position.get(
                    "entryPrice"
                )
            )

            amount = safe_float(
                position.get(
                    "positionAmt"
                )
            )

            if (
                abs(amount) > 0
                and entry > 0
            ):
                return position

        time.sleep(
            delay
        )

    return None


def live_open_trade(
    signal,
):
    symbol = signal[
        "symbol"
    ]

    direction = signal[
        "direction"
    ]

    entry_reference = signal[
        "price"
    ]

    stop_pct = (
        calculate_stop_distance(
            signal
        )
    )

    quantity = (
        calculate_quantity(
            symbol,
            entry_reference,
            stop_pct,
        )
    )

    if quantity <= 0:
        log(
            f"Invalid quantity "
            f"for {symbol}"
        )

        return False

    try:
        set_isolated_margin(
            symbol
        )

        set_leverage(
            symbol,
            LEVERAGE,
        )

        order_side = (
            "BUY"
            if direction == "LONG"
            else "SELL"
        )

        result = (
            place_market_order(
                symbol,
                order_side,
                quantity,
            )
        )

        position = (
            wait_for_live_position(
                symbol
            )
        )

        if not position:
            raise RuntimeError(
                "Position not detected "
                "after market entry"
            )

        actual_entry = safe_float(
            position.get(
                "entryPrice"
            )
        )

        actual_qty = abs(
            safe_float(
                position.get(
                    "positionAmt"
                )
            )
        )

        if actual_entry <= 0:
            actual_entry = (
                entry_reference
            )

        if actual_qty <= 0:
            actual_qty = quantity

        stop, target, risk_distance = (
            calculate_levels(
                direction,
                actual_entry,
                stop_pct,
            )
        )

        stop = normalize_price(
            symbol,
            stop,
        )

        target = normalize_price(
            symbol,
            target,
        )

        protection_side = (
            "SELL"
            if direction == "LONG"
            else "BUY"
        )

        try:
            place_stop_order(
                symbol,
                protection_side,
                stop,
            )

            place_take_profit_order(
                symbol,
                protection_side,
                target,
            )

        except Exception as protection_error:
            log(
                f"Protection failed "
                f"{symbol}: "
                f"{protection_error}"
            )

            try:
                emergency_close_symbol(
                    symbol
                )
            except Exception:
                pass

            return False

        add_trade(
            symbol=symbol,
            side=direction,
            entry=actual_entry,
            stop=stop,
            target=target,
            qty=actual_qty,
            status="OPEN",
        )

        set_global_cooldown()

        set_symbol_cooldown(
            symbol
        )

        telegram_send(
            "🟢 LIVE ENTRY\n"
            f"{symbol}\n"
            f"{direction}\n"
            f"Entry: "
            f"{actual_entry:.6f}\n"
            f"SL: {stop:.6f}\n"
            f"TP: {target:.6f}\n"
            f"Qty: {actual_qty}\n"
            f"Score: "
            f"{signal['score']}\n"
            f"RSI: "
            f"{signal['rsi']:.1f}\n"
            f"ADX: "
            f"{signal['adx']:.1f}\n"
            f"Funding: "
            f"{signal['funding_pct']:.4f}%"
        )

        log(
            f"LIVE ENTRY "
            f"{symbol} "
            f"{direction} "
            f"entry={actual_entry} "
            f"sl={stop} "
            f"tp={target}"
        )

        return True

    except Exception as exc:
        log(
            f"Live entry error "
            f"{symbol}: {exc}"
        )

        try:
            emergency_close_symbol(
                symbol
            )
        except Exception:
            pass

        return False


def emergency_close_symbol(
    symbol,
):
    position = (
        get_live_position(
            symbol
        )
    )

    if not position:
        return False

    amount = safe_float(
        position.get(
            "positionAmt"
        )
    )

    if amount == 0:
        return False

    side = (
        "SELL"
        if amount > 0
        else "BUY"
    )

    quantity = abs(
        amount
    )

    quantity = normalize_quantity(
        symbol,
        quantity,
    )

    if quantity <= 0:
        return False

    cancel_bot_orders(
        symbol
    )

    place_market_order(
        symbol,
        side,
        quantity,
    )

    return True


def calculate_r_multiple(
    side,
    entry,
    stop,
    price,
):
    risk = abs(
        entry - stop
    )

    if risk <= 0:
        return 0.0

    if side == "LONG":
        return (
            price - entry
        ) / risk

    return (
        entry - price
    ) / risk


def update_trade_stop(
    trade_id,
    new_stop,
):
    conn = db_connect()

    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE trades
        SET stop = ?
        WHERE id = ?
        """,
        (
            new_stop,
            trade_id,
        ),
    )

    conn.commit()
    conn.close()


def replace_live_protection(
    symbol,
    side,
    stop,
    target,
):
    cancel_bot_orders(
        symbol
    )

    protection_side = (
        "SELL"
        if side == "LONG"
        else "BUY"
    )

    place_stop_order(
        symbol,
        protection_side,
        stop,
    )

    place_take_profit_order(
        symbol,
        protection_side,
        target,
    )


def manage_live_positions():
    positions = (
        get_open_live_positions()
    )

    live_symbols = {
        position.get(
            "symbol"
        )
        for position in positions
    }

    # Reconcile positions that
    # disappeared from Binance.
    for trade in get_open_trades():
        symbol = trade[
            "symbol"
        ]

        if symbol not in live_symbols:
            close_trade(
                trade["id"],
                pnl=0.0,
                r_multiple=0.0,
                reason="EXTERNAL",
            )

            set_symbol_cooldown(
                symbol
            )

            cancel_bot_orders(
                symbol
            )

    for position in positions:
        symbol = position.get(
            "symbol"
        )

        amount = safe_float(
            position.get(
                "positionAmt"
            )
        )

        entry = safe_float(
            position.get(
                "entryPrice"
            )
        )

        mark_price = safe_float(
            position.get(
                "markPrice"
            )
        )

        side = get_position_side(
            position
        )

        if (
            not symbol
            or entry <= 0
            or mark_price <= 0
            or side is None
        ):
            continue

        trades = [
            trade
            for trade in get_open_trades()
            if trade["symbol"]
            == symbol
        ]

        if not trades:
            continue

        trade = trades[0]

        stop = safe_float(
            trade["stop"]
        )

        target = safe_float(
            trade["target"]
        )

        r_multiple = (
            calculate_r_multiple(
                side,
                entry,
                stop,
                mark_price,
            )
        )

        # Move to small profit
        # after +1R.
        if (
            r_multiple
            >= BREAK_EVEN_R
        ):
            if side == "LONG":
                new_stop = max(
                    stop,
                    entry
                    * (
                        1
                        + BE_LOCK_PCT
                    ),
                )

            else:
                new_stop = min(
                    stop,
                    entry
                    * (
                        1
                        - BE_LOCK_PCT
                    ),
                )

            new_stop = normalize_price(
                symbol,
                new_stop,
            )

            if (
                (
                    side == "LONG"
                    and new_stop > stop
                )
                or
                (
                    side == "SHORT"
                    and new_stop < stop
                )
            ):
                try:
                    replace_live_protection(
                        symbol,
                        side,
                        new_stop,
                        target,
                    )

                    update_trade_stop(
                        trade["id"],
                        new_stop,
                    )

                    stop = new_stop

                    telegram_send(
                        "🛡️ STOP MOVED\n"
                        f"{symbol}\n"
                        f"{side}\n"
                        f"New SL: "
                        f"{new_stop:.6f}\n"
                        f"R: "
                        f"{r_multiple:.2f}"
                    )

                except Exception as exc:
                    log(
                        f"Stop update "
                        f"error {symbol}: "
                        f"{exc}"
                    )

        # ATR trailing after +1.5R.
        if (
            r_multiple
            >= TRAIL_START_R
        ):
            try:
                candles = (
                    get_klines(
                        symbol,
                        "15m",
                        80,
                    )
                )

                prepared = (
                    prepare_candles(
                        candles
                    )
                )

                if prepared:
                    atr_values = atr(
                        prepared[
                            "highs"
                        ],
                        prepared[
                            "lows"
                        ],
                        prepared[
                            "closes"
                        ],
                        14,
                    )

                    if (
                        atr_values
                        and atr_values[-1]
                        is not None
                    ):
                        trail_distance = (
                            atr_values[-1]
                            * TRAIL_ATR_MULT
                        )

                        if side == "LONG":
                            candidate = (
                                mark_price
                                - trail_distance
                            )

                            new_stop = max(
                                stop,
                                candidate,
                            )

                        else:
                            candidate = (
                                mark_price
                                + trail_distance
                            )

                            new_stop = min(
                                stop,
                                candidate,
                            )

                        new_stop = normalize_price(
                            symbol,
                            new_stop,
                        )

                        should_update = (
                            (
                                side == "LONG"
                                and new_stop
                                > stop
                            )
                            or
                            (
                                side == "SHORT"
                                and new_stop
                                < stop
                            )
                        )

                        if should_update:
                            replace_live_protection(
                                symbol,
                                side,
                                new_stop,
                                target,
                            )

                            update_trade_stop(
                                trade["id"],
                                new_stop,
                            )

                            telegram_send(
                                "📈 TRAILING SL\n"
                                f"{symbol}\n"
                                f"{side}\n"
                                f"New SL: "
                                f"{new_stop:.6f}\n"
                                f"R: "
                                f"{r_multiple:.2f}"
                            )

            except Exception as exc:
                log(
                    f"Trailing error "
                    f"{symbol}: {exc}"
                )


# ============================================================
# SIGNAL SCANNER
# ============================================================

def available_symbols():
    try:
        get_exchange_info()
    except Exception as exc:
        log(
            f"Exchange info error: "
            f"{exc}"
        )

        return []

    result = []

    for symbol in SYMBOLS:
        rules = SYMBOL_RULES.get(
            symbol
        )

        if not rules:
            continue

        if (
            rules.get("status")
            != "TRADING"
        ):
            continue

        if (
            rules.get(
                "quoteAsset"
            )
            != "USDT"
        ):
            continue

        result.append(
            symbol
        )

    return result


def scan_for_best_signal():
    candidates = []

    symbols = (
        available_symbols()
    )

    for symbol in symbols:
        if (
            symbol_cooldown_active(
                symbol
            )
        ):
            continue

        signal = build_signal(
            symbol
        )

        if signal:
            candidates.append(
                signal
            )

    if not candidates:
        return None

    candidates.sort(
        key=lambda item:
            (
                item["score"],
                item["adx"],
                item["volume_ratio"],
            ),
        reverse=True,
    )

    return candidates[0]


# ============================================================
# BOT LOOP
# ============================================================

def process_cycle():
    global BOT_PAUSED

    try:
        if BOT_PAUSED:
            return

        # First manage existing
        # positions.
        if LIVE_TRADING:
            manage_live_positions()
        else:
            manage_paper_trades()

        if daily_drawdown_hit():
            log(
                "Daily drawdown limit "
                "reached. New entries "
                "blocked."
            )
            return

        if count_open_positions() >= (
            MAX_OPEN_POSITIONS
        ):
            return

        if global_cooldown_active():
            return

        signal = (
            scan_for_best_signal()
        )

        if not signal:
            return

        can_trade, reason = (
            can_open_new_trade(
                signal["symbol"]
            )
        )

        if not can_trade:
            log(
                f"Entry blocked "
                f"{signal['symbol']}: "
                f"{reason}"
            )
            return

        if LIVE_TRADING:
            live_open_trade(
                signal
            )
        else:
            paper_open_trade(
                signal
            )

    except Exception as exc:
        log(
            f"Cycle error: {exc}"
        )


def trading_loop():
    log(
        "Trading loop started."
    )

    while True:
        started = time.time()

        try:
            process_cycle()

        except Exception as exc:
            log(
                f"Trading loop error: "
                f"{exc}"
            )

        elapsed = (
            time.time()
            - started
        )

        sleep_for = max(
            5,
            SCAN_INTERVAL
            - elapsed,
        )

        time.sleep(
            sleep_for
        )


# ============================================================
# TELEGRAM
# ============================================================

TELEGRAM_APP = None
TELEGRAM_LOOP = None


def telegram_send(
    text,
):
    global TELEGRAM_APP
    global TELEGRAM_LOOP

    if not TELEGRAM_BOT_TOKEN:
        return

    if not TELEGRAM_CHAT_ID:
        return

    if (
        TELEGRAM_APP is None
        or TELEGRAM_LOOP is None
    ):
        log(
            "Telegram message "
            "skipped: app not ready"
        )
        return

    try:
        future = (
            asyncio.run_coroutine_threadsafe(
                TELEGRAM_APP.bot.send_message(
                    chat_id=TELEGRAM_CHAT_ID,
                    text=text,
                ),
                TELEGRAM_LOOP,
            )
        )

        future.result(
            timeout=15
        )

    except Exception as exc:
        log(
            f"Telegram send error: "
            f"{exc}"
        )


def authorized(
    update,
):
    if not update:
        return False

    user = update.effective_user
    chat = update.effective_chat

    if not user or not chat:
        return False

    return str(
        chat.id
    ) == str(
        TELEGRAM_CHAT_ID
    )


def bot_status_text():
    mode = (
        "LIVE"
        if LIVE_TRADING
        else "PAPER"
    )

    equity = (
        get_account_equity()
    )

    open_count = (
        count_open_positions()
    )

    drawdown_hit = (
        daily_drawdown_hit()
    )

    margin = (
        current_margin_used()
    )

    return (
        "🤖 SNIPER BOT\n"
        "\n"
        f"Mode: {mode}\n"
        f"Equity: {equity:.2f} USDT\n"
        f"Positions: "
        f"{open_count}/"
        f"{MAX_OPEN_POSITIONS}\n"
        f"Margin used: "
        f"{margin:.2f} USDT\n"
        f"Paused: "
        f"{'YES' if BOT_PAUSED else 'NO'}\n"
        f"Daily DD lock: "
        f"{'YES' if drawdown_hit else 'NO'}\n"
        f"Leverage: {LEVERAGE}x\n"
        f"Risk/trade: "
        f"{RISK_PER_TRADE * 100:.2f}%"
    )


def positions_text():
    if not LIVE_TRADING:
        trades = (
            get_open_trades()
        )

        if not trades:
            return (
                "📊 No open paper "
                "positions."
            )

        lines = [
            "📊 PAPER POSITIONS"
        ]

        for trade in trades:
            symbol = trade[
                "symbol"
            ]

            entry = safe_float(
                trade["entry"]
            )

            stop = safe_float(
                trade["stop"]
            )

            target = safe_float(
                trade["target"]
            )

            side = trade[
                "side"
            ]

            price = (
                get_ticker_price(
                    symbol
                )
            )

            r = (
                calculate_r_multiple(
                    side,
                    entry,
                    stop,
                    price,
                )
            )

            lines.append(
                ""
                f"{symbol} "
                f"{side}\n"
                f"Entry: "
                f"{entry:.6f}\n"
                f"Price: "
                f"{price:.6f}\n"
                f"SL: "
                f"{stop:.6f}\n"
                f"TP: "
                f"{target:.6f}\n"
                f"R: {r:.2f}"
            )

        return "\n".join(
            lines
        )

    positions = (
        get_open_live_positions()
    )

    if not positions:
        return (
            "📊 No open live "
            "positions."
        )

    lines = [
        "📊 LIVE POSITIONS"
    ]

    for position in positions:
        symbol = position.get(
            "symbol"
        )

        side = get_position_side(
            position
        )

        amount = abs(
            safe_float(
                position.get(
                    "positionAmt"
                )
            )
        )

        entry = safe_float(
            position.get(
                "entryPrice"
            )
        )

        mark = safe_float(
            position.get(
                "markPrice"
            )
        )

        pnl = safe_float(
            position.get(
                "unRealizedProfit"
            )
        )

        lines.append(
            ""
            f"{symbol} "
            f"{side}\n"
            f"Qty: {amount}\n"
            f"Entry: {entry:.6f}\n"
            f"Mark: {mark:.6f}\n"
            f"uPnL: "
            f"{pnl:+.2f} USDT"
        )

    return "\n".join(
        lines
    )


def stats_text():
    trades = (
        get_recent_trades(
            1000
        )
    )

    closed = [
        trade
        for trade in trades
        if trade["status"]
        == "CLOSED"
    ]

    if not closed:
        return (
            "📈 No closed trades yet."
        )

    wins = [
        trade
        for trade in closed
        if safe_float(
            trade["pnl"]
        ) > 0
    ]

    losses = [
        trade
        for trade in closed
        if safe_float(
            trade["pnl"]
        ) < 0
    ]

    pnl = sum(
        safe_float(
            trade["pnl"]
        )
        for trade in closed
    )

    avg_r = (
        average(
            [
                safe_float(
                    trade[
                        "r_multiple"
                    ]
                )
                for trade in closed
            ]
        )
    )

    win_rate = (
        len(wins)
        / len(closed)
        * 100
    )

    return (
        "📈 STATS\n"
        "\n"
        f"Trades: "
        f"{len(closed)}\n"
        f"Wins: "
        f"{len(wins)}\n"
        f"Losses: "
        f"{len(losses)}\n"
        f"Win rate: "
        f"{win_rate:.1f}%\n"
        f"Net PnL: "
        f"{pnl:+.2f} USDT\n"
        f"Average R: "
        f"{avg_r:+.2f}"
    )


def signals_text():
    symbols = (
        available_symbols()
    )

    if not symbols:
        return (
            "No symbols available."
        )

    candidates = []

    for symbol in symbols:
        if (
            symbol_cooldown_active(
                symbol
            )
        ):
            continue

        signal = build_signal(
            symbol
        )

        if signal:
            candidates.append(
                signal
            )

    candidates.sort(
        key=lambda item:
            (
                item["score"],
                item["adx"],
            ),
        reverse=True,
    )

    if not candidates:
        return (
            "🔎 No valid signals."
        )

    lines = [
        "🔎 TOP SIGNALS"
    ]

    for signal in candidates[:8]:
        lines.append(
            ""
            f"{signal['symbol']} "
            f"{signal['direction']}\n"
            f"Score: "
            f"{signal['score']}\n"
            f"RSI: "
            f"{signal['rsi']:.1f}\n"
            f"ADX: "
            f"{signal['adx']:.1f}\n"
            f"ATR: "
            f"{signal['atr_pct'] * 100:.2f}%\n"
            f"Volume: "
            f"{signal['volume_ratio']:.2f}x\n"
            f"Funding: "
            f"{signal['funding_pct']:.4f}%"
        )

    return "\n".join(
        lines
    )


def health_text():
    try:
        get_exchange_info()

        exchange_ok = True

    except Exception as exc:
        exchange_ok = False

        log(
            f"Health exchange "
            f"error: {exc}"
        )

    telegram_ok = (
        bool(
            TELEGRAM_BOT_TOKEN
        )
        and bool(
            TELEGRAM_CHAT_ID
        )
    )

    return (
        "🩺 HEALTH\n"
        "\n"
        f"Exchange: "
        f"{'OK' if exchange_ok else 'ERROR'}\n"
        f"Telegram config: "
        f"{'OK' if telegram_ok else 'ERROR'}\n"
        f"Mode: "
        f"{'LIVE' if LIVE_TRADING else 'PAPER'}\n"
        f"Paused: "
        f"{'YES' if BOT_PAUSED else 'NO'}"

    # ============================================================
# TELEGRAM BUTTONS
# ============================================================

async def telegram_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query is None:
        return

    user = update.effective_user
    if user is None or not is_authorized_chat(update):
        await query.answer("Access denied.", show_alert=True)
        return

    await query.answer()

    data = query.data or ""

    if data == "status":
        await query.edit_message_text(
            status_text(),
            reply_markup=telegram_keyboard()
        )

    elif data == "positions":
        await query.edit_message_text(
            positions_text(),
            reply_markup=telegram_keyboard()
        )

    elif data == "stats":
        await query.edit_message_text(
            stats_text(),
            reply_markup=telegram_keyboard()
        )

    elif data == "signals":
        text = signals_text()
        await query.edit_message_text(
            text,
            reply_markup=telegram_keyboard()
        )

    elif data == "pause":
        global BOT_PAUSED
        BOT_PAUSED = True
        save_state("bot_paused", "1")

        await query.edit_message_text(
            "⏸ BOT PAUSED\n\nNew entries are disabled.",
            reply_markup=telegram_keyboard()
        )

    elif data == "resume":
        global BOT_PAUSED
        BOT_PAUSED = False
        save_state("bot_paused", "0")

        await query.edit_message_text(
            "▶️ BOT RESUMED\n\nNew entries are enabled.",
            reply_markup=telegram_keyboard()
        )

    elif data == "emergency":
        global BOT_PAUSED
        BOT_PAUSED = True
        save_state("bot_paused", "1")

        await query.edit_message_text(
            "🚨 EMERGENCY STOP\n\n"
            "New entries disabled.\n"
            "Existing positions were NOT closed.",
            reply_markup=telegram_keyboard()
        )

    elif data == "close_confirm":
        await query.edit_message_text(
            "⚠️ CLOSE ALL POSITIONS?\n\n"
            "This will close all bot-managed positions.",
            reply_markup=close_confirm_keyboard()
        )

    elif data == "close_cancel":
        await query.edit_message_text(
            status_text(),
            reply_markup=telegram_keyboard()
        )

    elif data == "close_all":
        result = close_all_bot_positions()

        await query.edit_message_text(
            result,
            reply_markup=telegram_keyboard()
        )


# ============================================================
# TELEGRAM KEYBOARD
# ============================================================

def telegram_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Status", callback_data="status"),
            InlineKeyboardButton("📈 Positions", callback_data="positions"),
        ],
        [
            InlineKeyboardButton("📡 Signals", callback_data="signals"),
            InlineKeyboardButton("💰 Stats", callback_data="stats"),
        ],
        [
            InlineKeyboardButton("⏸ Pause", callback_data="pause"),
            InlineKeyboardButton("▶️ Resume", callback_data="resume"),
        ],
        [
            InlineKeyboardButton("🚨 Emergency", callback_data="emergency"),
        ],
        [
            InlineKeyboardButton("🔴 Close All", callback_data="close_confirm"),
        ],
    ])


def close_confirm_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "⚠️ YES, CLOSE ALL",
                callback_data="close_all"
            ),
            InlineKeyboardButton(
                "❌ CANCEL",
                callback_data="close_cancel"
            ),
        ]
    ])


# ============================================================
# TELEGRAM COMMANDS
# ============================================================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized_chat(update):
        return

    text = (
        "🤖 BINANCE SNIPER BOT\n\n"
        "Bot is online.\n\n"
        f"Mode: {'LIVE' if LIVE_TRADING else 'PAPER'}\n"
        f"Leverage: {LEVERAGE}x\n"
        f"Risk/trade: {RISK_PER_TRADE * 100:.2f}%\n"
        f"Max positions: {MAX_OPEN_POSITIONS}\n\n"
        "Use the buttons below or commands:\n"
        "/status\n"
        "/positions\n"
        "/stats\n"
        "/signals\n"
        "/pause\n"
        "/resume\n"
        "/mode\n"
        "/risk\n"
        "/health"
    )

    await update.message.reply_text(
        text,
        reply_markup=telegram_keyboard()
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized_chat(update):
        return

    await update.message.reply_text(
        status_text(),
        reply_markup=telegram_keyboard()
    )


async def cmd_positions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized_chat(update):
        return

    await update.message.reply_text(
        positions_text(),
        reply_markup=telegram_keyboard()
    )


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized_chat(update):
        return

    await update.message.reply_text(
        stats_text(),
        reply_markup=telegram_keyboard()
    )


async def cmd_signals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized_chat(update):
        return

    await update.message.reply_text(
        signals_text(),
        reply_markup=telegram_keyboard()
    )


async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global BOT_PAUSED

    if not is_authorized_chat(update):
        return

    BOT_PAUSED = True
    save_state("bot_paused", "1")

    await update.message.reply_text(
        "⏸ BOT PAUSED\n\nNew entries are disabled.",
        reply_markup=telegram_keyboard()
    )


async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global BOT_PAUSED

    if not is_authorized_chat(update):
        return

    BOT_PAUSED = False
    save_state("bot_paused", "0")

    await update.message.reply_text(
        "▶️ BOT RESUMED\n\nNew entries are enabled.",
        reply_markup=telegram_keyboard()
    )


async def cmd_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized_chat(update):
        return

    mode = "LIVE TRADING" if LIVE_TRADING else "PAPER MODE"

    await update.message.reply_text(
        f"⚙️ CURRENT MODE\n\n{mode}\n\n"
        f"Leverage: {LEVERAGE}x\n"
        f"Risk/trade: {RISK_PER_TRADE * 100:.2f}%\n"
        f"Max positions: {MAX_OPEN_POSITIONS}",
        reply_markup=telegram_keyboard()
    )


async def cmd_risk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized_chat(update):
        return

    await update.message.reply_text(
        "🛡 RISK SETTINGS\n\n"
        f"Risk per trade: {RISK_PER_TRADE * 100:.2f}%\n"
        f"Max margin/trade: {MAX_MARGIN_PER_TRADE * 100:.1f}%\n"
        f"Max total margin: {MAX_TOTAL_MARGIN * 100:.1f}%\n"
        f"Max daily drawdown: {MAX_DAILY_DRAWDOWN * 100:.1f}%\n"
        f"Cooldown: {COOLDOWN_MINUTES} min\n"
        f"Global cooldown: {GLOBAL_ENTRY_COOLDOWN_MINUTES} min\n"
        f"Max positions: {MAX_OPEN_POSITIONS}",
        reply_markup=telegram_keyboard()
    )


async def cmd_health(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized_chat(update):
        return

    try:
        ping_binance()

        await update.message.reply_text(
            "🟢 SYSTEM HEALTH\n\n"
            "Binance API: OK\n"
            "Telegram: OK\n"
            "Database: OK\n"
            f"Mode: {'LIVE' if LIVE_TRADING else 'PAPER'}\n"
            f"Paused: {'YES' if BOT_PAUSED else 'NO'}",
            reply_markup=telegram_keyboard()
        )

    except Exception as e:
        await update.message.reply_text(
            f"🔴 SYSTEM HEALTH\n\n"
            f"Binance API ERROR:\n{e}",
            reply_markup=telegram_keyboard()
        )


# ============================================================
# TELEGRAM APPLICATION
# ============================================================

def build_telegram_app():
    if not TELEGRAM_BOT_TOKEN:
        log.warning("TELEGRAM_BOT_TOKEN is not configured.")
        return None

    application = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .build()
    )

    application.add_handler(
        CallbackQueryHandler(
            telegram_buttons
        )
    )

    application.add_handler(
        CommandHandler(
            "start",
            cmd_start
        )
    )

    application.add_handler(
        CommandHandler(
            "status",
            cmd_status
        )
    )

    application.add_handler(
        CommandHandler(
            "positions",
            cmd_positions
        )
    )

    application.add_handler(
        CommandHandler(
            "stats",
            cmd_stats
        )
    )

    application.add_handler(
        CommandHandler(
            "signals",
            cmd_signals
        )
    )

    application.add_handler(
        CommandHandler(
            "pause",
            cmd_pause
        )
    )

    application.add_handler(
        CommandHandler(
            "resume",
            cmd_resume
        )
    )

    application.add_handler(
        CommandHandler(
            "mode",
            cmd_mode
        )
    )

    application.add_handler(
        CommandHandler(
            "risk",
            cmd_risk
        )
    )

    application.add_handler(
        CommandHandler(
            "health",
            cmd_health
        )
    )

    return application


# ============================================================
# TELEGRAM MAIN
# ============================================================

async def telegram_main():
    application = build_telegram_app()

    if application is None:
        log.warning(
            "Telegram disabled because TELEGRAM_BOT_TOKEN is missing."
        )
        return

    await application.initialize()
    await application.start()

    if application.updater is not None:
        await application.updater.start_polling()

    log.info("Telegram bot started.")

    try:
        while True:
            await asyncio.sleep(3600)

    finally:
        if application.updater is not None:
            await application.updater.stop()

        await application.stop()
        await application.shutdown()


# ============================================================
# STARTUP
# ============================================================

def startup_checks():
    log.info("==========================================")
    log.info("BINANCE SNIPER BOT STARTING")
    log.info("==========================================")

    log.info(
        "Mode: %s",
        "LIVE TRADING" if LIVE_TRADING else "PAPER MODE"
    )

    log.info("Leverage: %sx", LEVERAGE)
    log.info("Risk per trade: %.2f%%", RISK_PER_TRADE * 100)
    log.info("Max positions: %s", MAX_OPEN_POSITIONS)

    init_db()

    if LIVE_TRADING:
        if not BINANCE_API_KEY or not BINANCE_SECRET_KEY:
            raise RuntimeError(
                "LIVE_TRADING=true but Binance API keys are missing."
            )

        ping_binance()

        log.info("Binance API connection: OK")

    else:
        log.info(
            "Paper equity: $%.2f",
            get_paper_equity()
        )

    if TELEGRAM_BOT_TOKEN:
        log.info("Telegram: configured")
    else:
        log.warning("Telegram: NOT configured")

    log.info("Startup checks complete.")


# ============================================================
# MAIN
# ============================================================

def main():
    startup_checks()

    trading_thread = threading.Thread(
        target=bot_loop,
        name="TradingThread",
        daemon=True
    )

    trading_thread.start()

    if TELEGRAM_BOT_TOKEN:
        asyncio.run(
            telegram_main()
        )

    else:
        while True:
            time.sleep(60)


if __name__ == "__main__":
    main()
