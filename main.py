import os
import time
import requests
import pandas as pd
import numpy as np
from threading import Thread
from flask import Flask
from binance.client import Client
from binance.exceptions import BinanceAPIException

app = Flask(__name__)

BINANCE_API_KEY = os.environ.get('BINANCE_API_KEY')
BINANCE_SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

TIMEFRAME = Client.KLINE_INTERVAL_15MINUTE
LEVERAGE = 5  
MAX_ACTIVE_POSITIONS = 3  # Разрешаем до 3 параллельных сделок
TARGET_USDT = 25.0        # Объем позиции в USDT

binance_client = None
if BINANCE_API_KEY and BINANCE_SECRET_KEY:
    try:
        binance_client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)
        print("✅ Binance API подключен.")
    except Exception as e:
        print(f"❌ Ошибка Binance API: {e}")

def send_telegram(text):
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        try:
            requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}, timeout=8)
        except Exception as e:
            print(f"❌ Ошибка TG: {e}")

def get_top_symbols(limit=80):
    """Автозагрузка топ-пар по объему со фьючерсов Binance"""
    try:
        tickers = binance_client.futures_ticker()
        # Сортируем по суточному объему торгов в USDT
        usdt_tickers = [t for t in tickers if t['symbol'].endswith('USDT') and 'USDC' not in t['symbol']]
        usdt_tickers = sorted(usdt_tickers, key=lambda x: float(x['quoteVolume']), reverse=True)
        
        top_symbols = [t['symbol'] for t in usdt_tickers[:limit]]
        return top_symbols
    except Exception as e:
        print(f"⚠️ Ошибка загрузки топ пар: {e}")
        # Резервный список на случай сбоя запроса
        return ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT', 'DOGEUSDT', 'ADAUSDT', 'AVAXUSDT']

def get_exchange_rules():
    """Динамическое получение точности цен и минимальных лотов для всех пар"""
    rules = {}
    try:
        exchange_info = binance_client.futures_exchange_info()
        for s in exchange_info['symbols']:
            symbol = s['symbol']
            price_precision = s['pricePrecision']
            qty_precision = s['quantityPrecision']
            
            min_qty = 1.0
            for f in s['filters']:
                if f['filterType'] == 'LOT_SIZE':
                    min_qty = float(f['minQty'])
                    
            rules[symbol] = {
                'price_dec': price_precision,
                'qty_dec': qty_precision,
                'min_qty': min_qty
            }
    except Exception as e:
        print(f"⚠️ Ошибка получения правил биржи: {e}")
    return rules

SYMBOL_RULES = {}

def format_price(symbol, price):
    dec = SYMBOL_RULES.get(symbol, {}).get('price_dec', 2)
    return round(float(price), dec)

def calculate_safe_qty(symbol, current_price):
    rules = SYMBOL_RULES.get(symbol, {'qty_dec': 2, 'min_qty': 1.0})
    raw_qty = TARGET_USDT / current_price
    final_qty = max(raw_qty, rules['min_qty'])
    dec = rules['qty_dec']
    if dec == 0:
        return float(int(final_qty))
    return round(final_qty, dec)

def clean_leftover_orders(symbol):
    try:
        binance_client.futures_cancel_all_open_orders(symbol=symbol)
    except Exception as e:
        print(f"⚠️ Ошибка чистки ордеров {symbol}: {e}")

def get_actual_entry_price(symbol):
    try:
        positions = binance_client.futures_position_information(symbol=symbol)
        for p in positions:
            if p['symbol'] == symbol:
                amt = float(p['positionAmt'])
                if amt != 0:
                    return float(p['entryPrice']), amt
    except Exception as e:
        print(f"⚠️ Ошибка получения цены входа {symbol}: {e}")
    return None, 0

def get_klines_df(symbol):
    klines = binance_client.futures_klines(symbol=symbol, interval=TIMEFRAME, limit=150)
    df = pd.DataFrame(klines, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'
    ])
    df['open'] = df['open'].astype(float)
    df['close'] = df['close'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['volume'] = df['volume'].astype(float)

    # Индикаторы для стратегии «Снайпер-Импульс»
    df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
    df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
    df['ema200'] = df['close'].ewm(span=50, adjust=False).mean() # Ускоренная для 15m
    df['vol_sma20'] = df['volume'].rolling(window=20).mean()

    # Размер тела свечи и ATR для стопов
    df['body_size'] = abs(df['close'] - df['open'])
    df['avg_body'] = df['body_size'].rolling(window=10).mean()

    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['low'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    df['atr'] = np.max(ranges, axis=1).rolling(14).mean()

    return df

def get_open_positions():
    active = {}
    try:
        positions = binance_client.futures_position_information()
        for p in positions:
            amt = float(p['positionAmt'])
            if amt != 0:
                active[p['symbol']] = amt
    except Exception as e:
        print(f"⚠️ Ошибка получения позиций: {e}")
    return active

def execute_smart_trade(symbol, action, est_price, atr):
    qty = calculate_safe_qty(symbol, est_price)
    
    try:
        clean_leftover_orders(symbol)

        try:
            binance_client.futures_change_margin_type(symbol=symbol, marginType='ISOLATED')
        except BinanceAPIException:
            pass

        binance_client.futures_change_leverage(symbol=symbol, leverage=LEVERAGE)

        # 1. Вход по рынку
        binance_client.futures_create_order(
            symbol=symbol, side=action, type='MARKET', quantity=qty
        )

        time.sleep(1)

        real_entry, _ = get_actual_entry_price(symbol)
        if not real_entry or real_entry == 0:
            real_entry = est_price

        # Короткий стоп под основание импульса (~1.5-2%)
        sl_dist = atr * 1.5
        tp_dist = atr * 3.0

        if action == 'BUY':
            sl_price = format_price(symbol, real_entry - sl_dist)
            tp_price = format_price(symbol, real_entry + tp_dist)
            side_close = 'SELL'
        else:
            sl_price = format_price(symbol, real_entry + sl_dist)
            tp_price = format_price(symbol, real_entry - tp_dist)
            side_close = 'BUY'

        # 2. Защитный Стоп-Лосс
        binance_client.futures_create_order(
            symbol=symbol, side=side_close, type='STOP_MARKET', stopPrice=sl_price, closePosition=True
        )

        # 3. Тейк-Профит
        binance_client.futures_create_order(
            symbol=symbol, side=side_close, type='TAKE_PROFIT_MARKET', stopPrice=tp_price, closePosition=True
        )

        msg = (
            f"🎯 *СНАЙПЕР-ИМПУЛЬС: ВХОД* (`{action}` | {LEVERAGE}x)\n"
            f"• Монета: `{symbol}` (15m)\n"
            f"• Объем: `{qty}`\n"
            f"• Вход: `~{real_entry}`\n"
            f"• Stop-Loss: `{sl_price}`\n"
            f"• Take-Profit: `{tp_price}`"
        )
        send_telegram(msg)

    except Exception as err:
        print(f"❌ Ошибка входа по {symbol}: {err}")

def manage_trailing_stops(current_positions):
    """Динамический трейлинг: перенос в безубыток при хорошем движении"""
    for symbol, amt in current_positions.items():
        try:
            entry_price, position_amt = get_actual_entry_price(symbol)
            if not entry_price:
                continue
            
            ticker = binance_client.futures_symbol_ticker(symbol=symbol)
            current_price = float(ticker['price'])
            
            # Проверяем процент движения в нашу сторону
            is_long = position_amt > 0
            if is_long:
                profit_pct = (current_price - entry_price) / entry_price * 100
                # Если ушли в плюс на 2.5% и более — подтягиваем стоп в безубыток или выше
                if profit_pct >= 2.5:
                    clean_leftover_orders(symbol)
                    new_sl = format_price(symbol, entry_price * 1.002) # Чуть выше входа
                    binance_client.futures_create_order(
                        symbol=symbol, side='SELL', type='STOP_MARKET', stopPrice=new_sl, closePosition=True
                    )
                    send_telegram(f"🛡 *Трелинг-стоп активирован* для `{symbol}`. Стоп перенесен в безубыток (+0.2%).")
            else:
                profit_pct = (entry_price - current_price) / entry_price * 100
                if profit_pct >= 2.5:
                    clean_leftover_orders(symbol)
                    new_sl = format_price(symbol, entry_price * 0.998)
                    binance_client.futures_create_order(
                        symbol=symbol, side='BUY', type='STOP_MARKET', stopPrice=new_sl, closePosition=True
                    )
                    send_telegram(f"🛡 *Трелинг-стоп активирован* для `{symbol}`. Стоп перенесен в безубыток.")
        except Exception as e:
            print(f"⚠️ Ошибка трейлинга для {symbol}: {e}")

def bot_loop():
    time.sleep(5)
    global SYMBOL_RULES
    
    if binance_client:
        SYMBOL_RULES = get_exchange_rules()
        send_telegram("🚀 *Снайпер-Бот запущен!* Загружено топ-80 пар, активирован сканер импульсов и трейлинг.")
    
    known_positions = {}

    while True:
        try:
            if binance_client:
                current_positions = get_open_positions()

                # Управление трейлингом для открытых позиций
                if current_positions:
                    manage_trailing_stops(current_positions)

                # Отслеживание закрытия позиций
                for sym in list(known_positions.keys()):
                    if sym not in current_positions:
                        clean_leftover_orders(sym)
                        send_telegram(f"🏁 Сделка по `{sym}` закрыта. Слот освобожден.")
                        del known_positions[sym]

                for sym, amt in current_positions.items():
                    known_positions[sym] = amt

                total_active = len(current_positions)

                # Автозагружаем актуальный топ-80 пар на каждом круге
                active_symbols = get_top_symbols(limit=80)

                for symbol in active_symbols:
                    if symbol in current_positions or total_active >= MAX_ACTIVE_POSITIONS:
                        continue

                    df = get_klines_df(symbol)
                    if len(df) < 30:
                        continue

                    c2 = df.iloc[-2] # Закрытая свеча
                    c3 = df.iloc[-3] # Предыдущая свеча

                    # Логика фильтра «Снайпер-Импульс» (15m)
                    trend_up = c2['close'] > c2['ema200']
                    trend_down = c2['close'] < c2['ema200']

                    # Пересечение EMA + импульсное тело свечи больше среднего + объем выше нормы
                    cross_up = (c3['ema9'] <= c3['ema21']) and (c2['ema9'] > c2['ema21'])
                    cross_down = (c3['ema9'] >= c3['ema21']) and (c2['ema9'] < c2['ema21'])
                    
                    impulse_ok = c2['body_size'] > (c2['avg_body'] * 1.2)
                    vol_ok = c2['volume'] > (c2['vol_sma20'] * 1.3)

                    if cross_up and trend_up and impulse_ok and vol_ok:
                        execute_smart_trade(symbol, 'BUY', c2['close'], c2['atr'])
                        total_active += 1
                        time.sleep(1)

                    elif cross_down and trend_down and impulse_ok and vol_ok:
                        execute_smart_trade(symbol, 'SELL', c2['close'], c2['atr'])
                        total_active += 1
                        time.sleep(1)

        except Exception as e:
            print(f"❌ Ошибка главного цикла: {e}")

        # Пауза между сканированиями рынка
        time.sleep(60)

Thread(target=bot_loop, daemon=True).start()

@app.route('/')
def home():
    return "🤖 Sniper-Impulse Bot Active.", 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
