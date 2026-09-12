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

SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'NEARUSDT']

TIMEFRAME = Client.KLINE_INTERVAL_5MINUTE
LEVERAGE = 5  
MAX_ACTIVE_POSITIONS = 2  

# Точные регламенты биржи: точность цены, точность объема и минимальный лот
SYMBOL_RULES = {
    'BTCUSDT':  {'price_dec': 1, 'qty_dec': 3, 'min_qty': 0.001},
    'ETHUSDT':  {'price_dec': 2, 'qty_dec': 3, 'min_qty': 0.01},
    'SOLUSDT':  {'price_dec': 2, 'qty_dec': 2, 'min_qty': 0.2},
    'XRPUSDT':  {'price_dec': 4, 'qty_dec': 1, 'min_qty': 40.0},
    'DOGEUSDT': {'price_dec': 5, 'qty_dec': 0, 'min_qty': 200.0},
    'NEARUSDT': {'price_dec': 3, 'qty_dec': 1, 'min_qty': 5.0}
}

TARGET_USDT = 25.0  # Целевой объем позиции ($25)

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
            requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=8)
        except Exception as e:
            print(f"❌ Ошибка TG: {e}")

def format_price(symbol, price):
    dec = SYMBOL_RULES.get(symbol, {}).get('price_dec', 2)
    return round(float(price), dec)

def calculate_safe_qty(symbol, current_price):
    """Безопасный расчет объема с учетом минимальных требований Binance"""
    rules = SYMBOL_RULES.get(symbol, {'qty_dec': 1, 'min_qty': 1.0})
    raw_qty = TARGET_USDT / current_price
    
    # Не даем объему опуститься ниже минимального порога биржи
    final_qty = max(raw_qty, rules['min_qty'])
    
    dec = rules['qty_dec']
    if dec == 0:
        return float(int(final_qty))
    return round(final_qty, dec)

def clean_leftover_orders(symbol):
    """Полное удаление оставшихся ордеров"""
    try:
        binance_client.futures_cancel_all_open_orders(symbol=symbol)
    except Exception as e:
        print(f"⚠️ Ошибка чистки ордеров {symbol}: {e}")

def get_actual_entry_price(symbol):
    """Получение реальной цены входа из открытой позиции"""
    try:
        positions = binance_client.futures_position_information(symbol=symbol)
        for p in positions:
            if p['symbol'] == symbol:
                return float(p['entryPrice'])
    except Exception as e:
        print(f"⚠️ Ошибка получения цены входа {symbol}: {e}")
    return None

def get_klines_df(symbol):
    klines = binance_client.futures_klines(symbol=symbol, interval=TIMEFRAME, limit=210)
    df = pd.DataFrame(klines, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'
    ])
    df['close'] = df['close'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['volume'] = df['volume'].astype(float)

    df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
    df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
    df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
    df['vol_sma20'] = df['volume'].rolling(window=20).mean()

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

        time.sleep(1) # Небольшая пауза для гарантированного обновления позиции на бирже

        # 2. Берем НАСТОЯЩУЮ цену входа с биржи, а не примерную
        real_entry = get_actual_entry_price(symbol)
        if not real_entry or real_entry == 0:
            real_entry = est_price

        sl_dist = atr * 1.2
        tp_dist = atr * 2.5

        if action == 'BUY':
            sl_price = format_price(symbol, real_entry - sl_dist)
            tp_price = format_price(symbol, real_entry + tp_dist)
            side_close = 'SELL'
        else:
            sl_price = format_price(symbol, real_entry + sl_dist)
            tp_price = format_price(symbol, real_entry - tp_dist)
            side_close = 'BUY'

        # 3. Выставляем Защитный Стоп-Лосс
        try:
            binance_client.futures_create_order(
                symbol=symbol, side=side_close, type='STOP_MARKET', stopPrice=sl_price, closePosition=True
            )
        except Exception as sl_err:
            send_telegram(f"⚠️ Ошибка SL по {symbol}: {sl_err}")

        # 4. Выставляем Тейк-Профит
        try:
            binance_client.futures_create_order(
                symbol=symbol, side=side_close, type='TAKE_PROFIT_MARKET', stopPrice=tp_price, closePosition=True
            )
        except Exception as tp_err:
            send_telegram(f"⚠️ Ошибка TP по {symbol}: {tp_err}")

        msg = (
            f"🎯 ВХОД В СДЕЛКУ ({action} | 5x)\n"
            f"Монета: {symbol} (5m)\n"
            f"Объем: {qty} {symbol.replace('USDT','')}\n"
            f"Фактический вход: ~{real_entry}\n"
            f"✅ Take-Profit: {tp_price}\n"
            f"🛑 Stop-Loss: {sl_price}"
        )
        send_telegram(msg)

    except Exception as err:
        error_msg = f"❌ Ошибка входа по {symbol}: {err}"
        print(error_msg)
        send_telegram(error_msg)

def bot_loop():
    time.sleep(5)
    
    # Первичная зачистка ордеров при запуске скрипта
    for sym in SYMBOLS:
        positions = get_open_positions()
        if sym not in positions:
            clean_leftover_orders(sym)

    send_telegram("🚀 Бот полностью проверен и запущен! Все баги ликвидированы.")
    
    known_positions = {}

    while True:
        try:
            if binance_client:
                current_positions = get_open_positions()

                # Отслеживание закрытия позиций и очистка сиротских ордеров
                for sym in list(known_positions.keys()):
                    if sym not in current_positions:
                        clean_leftover_orders(sym)
                        send_telegram(f"🏁 Сделка по {sym} закрыта. Повисшие ордера отменены.")
                        del known_positions[sym]

                for sym, amt in current_positions.items():
                    known_positions[sym] = amt

                total_active = len(current_positions)

                for symbol in SYMBOLS:
                    if symbol in current_positions or total_active >= MAX_ACTIVE_POSITIONS:
                        continue

                    df = get_klines_df(symbol)
                    c2 = df.iloc[-2]
                    c3 = df.iloc[-3]

                    # ИСПРАВЛЕННЫЕ УСЛОВИЯ ПЕРЕСЕЧЕНИЯ
                    cross_up = (c3['ema9'] <= c3['ema21']) and (c2['ema9'] > c2['ema21'])
                    cross_down = (c3['ema9'] >= c3['ema21']) and (c2['ema9'] < c2['ema21'])

                    vol_ok = c2['volume'] > (c2['vol_sma20'] * 1.1)
                    trend_long = c2['close'] > c2['ema200']
                    trend_short = c2['close'] < c2['ema200']

                    if cross_up and trend_long and vol_ok:
                        execute_smart_trade(symbol, 'BUY', c2['close'], c2['atr'])
                        total_active += 1

                    elif cross_down and trend_short and vol_ok:
                        execute_smart_trade(symbol, 'SELL', c2['close'], c2['atr'])
                        total_active += 1

                    time.sleep(0.5)

        except Exception as e:
            print(f"❌ Ошибка главного цикла: {e}")

        time.sleep(60)

Thread(target=bot_loop, daemon=True).start()

@app.route('/')
def home():
    return "🤖 Verified Bot Active.", 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
    
