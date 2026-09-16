import asyncio
import hashlib
import hmac
import math
import os
import sqlite3
import time
import urllib.parse
from typing import Dict, List, Optional

import aiohttp
import numpy as np

# ==========================================
# CONFIGURATION & ENVIRONMENT VARIABLES
# ==========================================
API_KEY = os.getenv("BINANCE_API_KEY", "")
API_SECRET = os.getenv("BINANCE_SECRET_KEY", "")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
LIVE_TRADING = os.getenv("LIVE_TRADING", "false").lower() == "true"

BASE_URL = "https://fapi.binance.com"

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", 
    "ADAUSDT", "AVAXUSDT", "DOGEUSDT", "LINKUSDT", "NEARUSDT",
    "DOTUSDT", "MATICUSDT", "PEPEUSDT", "INJUSDT", "TIAUSDT"
]

RISK_PER_TRADE = 0.005        # 0.5% риска на сделку от Equity
MAX_OPEN_POSITIONS = 3         # Максимум 3 одновременных позиции
CORR_THRESHOLD = 0.75          # Порог корреляции активов (Пирсон)
PAPER_TRADING_BALANCE = 10000.0 # Виртуальный баланс для тестов

# ==========================================
# TELEGRAM NOTIFIER
# ==========================================
async def send_telegram(message: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    print(f"[Telegram Alert Error]: Status {resp.status}")
    except Exception as e:
        print(f"[Telegram Exception]: {e}")

# ==========================================
# ASYNC BINANCE API CLIENT
# ==========================================
class AsyncBinanceClient:
    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.session: Optional[aiohttp.ClientSession] = None

    async def init_session(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(headers={"X-MBX-APIKEY": self.api_key})

    async def close_session(self):
        if self.session and not self.session.closed:
            await self.session.close()

    def _sign(self, params: dict) -> dict:
        params["timestamp"] = int(time.time() * 1000)
        query_string = urllib.parse.urlencode(params)
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
        params["signature"] = signature
        return params

    async def get_klines(self, symbol: str, interval: str, limit: int = 200) -> np.ndarray:
        await self.init_session()
        url = f"{BASE_URL}/fapi/v1/klines"
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        async with self.session.get(url, params=params) as resp:
            data = await resp.json()
            return np.array([[float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in data])

    async def get_account_equity(self) -> float:
        if not LIVE_TRADING:
            return PAPER_TRADING_BALANCE
        await self.init_session()
        url = f"{BASE_URL}/fapi/v2/account"
        params = self._sign({})
        async with self.session.get(url, params=params) as resp:
            data = await resp.json()
            return float(data.get("totalMarginBalance", PAPER_TRADING_BALANCE))

    async def place_market_order(self, symbol: str, side: str, quantity: float) -> dict:
        if not LIVE_TRADING:
            return {"status": "FILLED", "avgPrice": 0.0, "fills": []}
        
        await self.init_session()
        url = f"{BASE_URL}/fapi/v1/order"
        params = self._sign({
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": quantity,
            "newOrderRespType": "RESULT"
        })
        async with self.session.post(url, data=params) as resp:
            return await resp.json()

    async def place_algo_order(self, symbol: str, side: str, order_type: str, stop_price: float, quantity: float = 0, reduce_only: bool = True):
        if not LIVE_TRADING:
            return {"status": "NEW"}
            
        await self.init_session()
        url = f"{BASE_URL}/fapi/v1/order"
        params = self._sign({
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "stopPrice": round(stop_price, 4),
            "closePosition": "true" if quantity == 0 else "false",
            "quantity": round(quantity, 4) if quantity > 0 else None,
            "reduceOnly": "true" if reduce_only else "false"
        })
        params = {k: v for k, v in params.items() if v is not None}
        async with self.session.post(url, data=params) as resp:
            return await resp.json()

# ==========================================
# VECTORIZED INDICATOR ENGINE
# ==========================================
class Indicators:
    @staticmethod
    def ema(series: np.ndarray, period: int) -> np.ndarray:
        alpha = 2 / (period + 1)
        res = np.zeros_like(series)
        res[0] = series[0]
        for i in range(1, len(series)):
            res[i] = alpha * series[i] + (1 - alpha) * res[i - 1]
        return res

    @staticmethod
    def rsi(closes: np.ndarray, period: int = 14) -> float:
        deltas = np.diff(closes)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    @staticmethod
    def atr(klines: np.ndarray, period: int = 14) -> float:
        highs, lows, closes = klines[:, 1], klines[:, 2], klines[:, 3]
        tr = np.maximum(highs[1:] - lows[1:], 
             np.maximum(np.abs(highs[1:] - closes[:-1]), 
                        np.abs(lows[1:] - closes[:-1])))
        return float(np.mean(tr[-period:]))

    @staticmethod
    def adx(klines: np.ndarray, period: int = 14) -> float:
        highs, lows, closes = klines[:, 1], klines[:, 2], klines[:, 3]
        up_move = highs[1:] - highs[:-1]
        down_move = lows[:-1] - lows[1:]
        
        plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
        
        tr = np.maximum(highs[1:] - lows[1:], 
             np.maximum(np.abs(highs[1:] - closes[:-1]), 
                        np.abs(lows[1:] - closes[:-1])))
        
        atr_val = np.mean(tr[-period:])
        if atr_val == 0:
            return 0.0
            
        plus_di = 100 * (np.mean(plus_dm[-period:]) / atr_val)
        minus_di = 100 * (np.mean(minus_dm[-period:]) / atr_val)
        
        dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di + 1e-8)
        return float(dx)

# ==========================================
# MACRO BTC REGIME & CORRELATION
# ==========================================
async def get_btc_market_regime(client: AsyncBinanceClient) -> str:
    btc_1h = await client.get_klines("BTCUSDT", "1h", limit=210)
    closes = btc_1h[:, 3]
    sma200 = np.mean(closes[-200:])
    current_price = closes[-1]
    adx_val = Indicators.adx(btc_1h, period=14)

    if adx_val < 20.0:
        return "RANGE"
    elif current_price > sma200 and adx_val >= 22.0:
        return "BULL"
    elif current_price < sma200 and adx_val >= 22.0:
        return "BEAR"
    return "NEUTRAL"

def calculate_correlation(klines_a: np.ndarray, klines_b: np.ndarray) -> float:
    closes_a, closes_b = klines_a[:, 3], klines_b[:, 3]
    min_len = min(len(closes_a), len(closes_b))
    returns_a = np.diff(np.log(closes_a[-min_len:]))
    returns_b = np.diff(np.log(closes_b[-min_len:]))
    matrix = np.corrcoef(returns_a, returns_b)
    return float(matrix[0, 1])

# ==========================================
# DATABASE MANAGER (WAL MODE)
# ==========================================
def init_db():
    conn = sqlite3.connect("sniper_trading.db")
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            symbol TEXT PRIMARY KEY,
            side TEXT,
            entry_price REAL,
            quantity REAL,
            sl_price REAL,
            tp1_price REAL,
            atr REAL,
            tp1_hit INTEGER DEFAULT 0,
            timestamp INTEGER
        )
    """)
    conn.commit()
    conn.close()

# ==========================================
# MAIN TRADING ENGINE & TRACKER
# ==========================================
class SniperBot:
    def __init__(self):
        self.client = AsyncBinanceClient(API_KEY, API_SECRET)
        self.semaphore = asyncio.Semaphore(5) # Защита от лимитов API (Max 5 параллельно)
        init_db()

    async def parse_execution_price(self, order_response: dict, fallback_price: float) -> float:
        fills = order_response.get("fills", [])
        if fills:
            total_qty = sum(float(f["qty"]) for f in fills)
            total_cost = sum(float(f["price"]) * float(f["qty"]) for f in fills)
            if total_qty > 0:
                return total_cost / total_qty
        return float(order_response.get("avgPrice", fallback_price))

    async def process_symbol_signal(self, symbol: str, btc_regime: str, cache: dict) -> Optional[dict]:
        async with self.semaphore:
            klines_15m = await self.client.get_klines(symbol, "15m", limit=100)
            klines_1h = await self.client.get_klines(symbol, "1h", limit=100)
            cache[symbol] = klines_15m

            closes_15m, closes_1h = klines_15m[:, 3], klines_1h[:, 3]
            
            ema20_1h = Indicators.ema(closes_1h, 20)[-1]
            ema50_1h = Indicators.ema(closes_1h, 50)[-1]
            ema20_15m = Indicators.ema(closes_15m, 20)[-1]
            ema50_15m = Indicators.ema(closes_15m, 50)[-1]
            
            rsi15 = Indicators.rsi(closes_15m, 14)
            atr15 = Indicators.atr(klines_15m, 14)
            atr_pct = atr15 / closes_15m[-1]

            if not (0.0035 <= atr_pct <= 0.035):
                return None

            score = 0
            direction = None

            if btc_regime == "BULL":
                if ema20_1h > ema50_1h: score += 2
                if ema20_15m > ema50_15m: score += 2
                if 42 <= rsi15 <= 60: score += 2
                if closes_15m[-1] > closes_15m[-2]: score += 1
                direction = "BUY"
            elif btc_regime == "BEAR":
                if ema20_1h < ema50_1h: score += 2
                if ema20_15m < ema50_15m: score += 2
                if 40 <= rsi15 <= 58: score += 2
                if closes_15m[-1] < closes_15m[-2]: score += 1
                direction = "SELL"

            if score >= 6 and direction:
                return {
                    "symbol": symbol, "direction": direction,
                    "score": score, "atr": atr15, "price": closes_15m[-1]
                }
            return None

    async def execute_trade(self, signal: dict, equity: float, active_positions: List[str], cache: dict):
        symbol, side, price, atr = signal["symbol"], signal["direction"], signal["price"], signal["atr"]

        for active_sym in active_positions:
            if active_sym in cache and symbol in cache:
                if calculate_correlation(cache[symbol], cache[active_sym]) > CORR_THRESHOLD:
                    return

        stop_dist = atr * 1.5
        risk_amount = equity * RISK_PER_TRADE
        qty = round(risk_amount / stop_dist, 3)
        if qty <= 0: return

        order_res = await self.client.place_market_order(symbol, side, qty)
        actual_entry = await self.parse_execution_price(order_res, price) if LIVE_TRADING else price

        if side == "BUY":
            sl_price = actual_entry - stop_dist
            tp1_price = actual_entry + (stop_dist * 2.0)
            opp_side = "SELL"
        else:
            sl_price = actual_entry + stop_dist
            tp1_price = actual_entry - (stop_dist * 2.0)
            opp_side = "BUY"

        await self.client.place_algo_order(symbol, opp_side, "STOP_MARKET", sl_price)

        conn = sqlite3.connect("sniper_trading.db")
        c = conn.cursor()
        c.execute("""
            INSERT OR REPLACE INTO positions 
            (symbol, side, entry_price, quantity, sl_price, tp1_price, atr, tp1_hit, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
        """, (symbol, side, actual_entry, qty, sl_price, tp1_price, atr, int(time.time())))
        conn.commit()
        conn.close()

        msg = (f"🎯 <b>NEW ORDER EXECUTED</b>\n"
               f"Symbol: {symbol} ({side})\n"
               f"Entry: {actual_entry:.4f}\n"
               f"SL: {sl_price:.4f} | TP1: {tp1_price:.4f}\n"
               f"Mode: {'REAL' if LIVE_TRADING else 'PAPER'}")
        await send_telegram(msg)

    async def track_positions(self):
        """Фоновый трекинг: 50% TP, перевод в БУ и ATR Trailing"""
        while True:
            try:
                conn = sqlite3.connect("sniper_trading.db")
                c = conn.cursor()
                c.execute("SELECT symbol, side, entry_price, quantity, sl_price, tp1_price, atr, tp1_hit FROM positions")
                rows = c.fetchall()
                conn.close()

                for row in rows:
                    sym, side, entry, qty, sl, tp1, atr, tp1_hit = row
                    klines = await self.client.get_klines(sym, "15m", limit=2)
                    current_price = klines[-1, 3]

                    # 1. Сплит-фиксация 50% и перевод в безубыток
                    if tp1_hit == 0:
                        hit_tp1 = (side == "BUY" and current_price >= tp1) or (side == "SELL" and current_price <= tp1)
                        if hit_tp1:
                            close_qty = round(qty / 2, 3)
                            opp_side = "SELL" if side == "BUY" else "BUY"
                            
                            await self.client.place_market_order(sym, opp_side, close_qty)
                            new_sl = entry * 1.001 if side == "BUY" else entry * 0.999
                            await self.client.place_algo_order(sym, opp_side, "STOP_MARKET", new_sl)

                            conn = sqlite3.connect("sniper_trading.db")
                            c = conn.cursor()
                            c.execute("UPDATE positions SET tp1_hit = 1, sl_price = ?, quantity = ? WHERE symbol = ?", 
                                      (new_sl, qty - close_qty, sym))
                            conn.commit()
                            conn.close()
                            
                            await send_telegram(f"✅ <b>TP1 HIT (50% Closed)</b>\nSymbol: {sym}\nSL moved to Break-Even: {new_sl:.4f}")

                    # 2. Динамический ATR Trailing Stop для остатка (3.0 x ATR)
                    elif tp1_hit == 1:
                        opp_side = "SELL" if side == "BUY" else "BUY"
                        if side == "BUY":
                            potential_sl = current_price - (atr * 3.0)
                            if potential_sl > sl:
                                await self.client.place_algo_order(sym, opp_side, "STOP_MARKET", potential_sl)
                                conn = sqlite3.connect("sniper_trading.db")
                                conn.cursor().execute("UPDATE positions SET sl_price = ? WHERE symbol = ?", (potential_sl, sym))
                                conn.commit()
                                conn.close()
                        else:
                            potential_sl = current_price + (atr * 3.0)
                            if potential_sl < sl:
                                await self.client.place_algo_order(sym, opp_side, "STOP_MARKET", potential_sl)
                                conn = sqlite3.connect("sniper_trading.db")
                                conn.cursor().execute("UPDATE positions SET sl_price = ? WHERE symbol = ?", (potential_sl, sym))
                                conn.commit()
                                conn.close()

            except Exception as e:
                print(f"[Tracker Error]: {e}")

            await asyncio.sleep(10)

    async def run(self):
        await send_telegram("🚀 <b>SNIPER Pro Bot Started</b>\nMode: " + ("LIVE" if LIVE_TRADING else "PAPER"))
        asyncio.create_task(self.track_positions())

        while True:
            try:
                btc_regime = await get_btc_market_regime(self.client)
                
                if btc_regime != "RANGE":
                    cache = {}
                    tasks = [self.process_symbol_signal(sym, btc_regime, cache) for sym in SYMBOLS]
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    for res in results:
                        if isinstance(res, aiohttp.ClientResponseError) and res.status == 429:
                            await send_telegram("⚠️ <b>API Limit Warning (429)! Cooldown 60s...</b>")
                            await asyncio.sleep(60)
                            break

                    valid_signals = [s for s in results if isinstance(s, dict)]
                    valid_signals.sort(key=lambda x: x["score"], reverse=True)

                    conn = sqlite3.connect("sniper_trading.db")
                    open_syms = [r[0] for r in conn.cursor().execute("SELECT symbol FROM positions").fetchall()]
                    conn.close()

                    equity = await self.client.get_account_equity()

                    for sig in valid_signals:
                        if len(open_syms) >= MAX_OPEN_POSITIONS:
                            break
                        if sig["symbol"] not in open_syms:
                            await self.execute_trade(sig, equity, open_syms, cache)
                            open_syms.append(sig["symbol"])

            except Exception as e:
                print(f"[Loop Error]: {e}")
            
            await asyncio.sleep(15)

if __name__ == "__main__":
    bot = SniperBot()
    asyncio.run(bot.run())
