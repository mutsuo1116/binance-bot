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

# Топ ликвидных монет для 5m трейдинга
SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'NEARUSDT']

TIMEFRAME = Client.KLINE_INTERVAL_5MINUTE
LEVERAGE = 5  
MAX_ACTIVE_POSITIONS = 2  # Не более 2 сделок одновременно

# Позиции ~$25 на монету (залог ~$5)
QUANTITIES = {
    'BTCUSDT': 0.0004,
    'ETHUSDT': 0.01,
    'SOLUSDT': 0.18,
    'XRPUSDT': 45.0,
    'DOGEUSDT': 250.0,
    'NEARUSDT': 5.5
}

binance_client = None
if BINANCE_API_KEY and BINANCE_SECRET_KEY:
    try:
        binance_client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)
        print("✅ Binance API подключен.")
    except Exception as e:
        print(f"❌ Ошибка Binance API: {e}")

def send_telegram(text):
    """Надежная отправка в Telegram"""
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        try:
            res = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=8)
            if res.status_code != 200:
                print(f"⚠️ Ошибка TG HTTP: {res.text}")
        except Exception as e:
            print(f"❌ Ошибка отправки TG: {e}")

def get_klines_df(symbol):
    """Загрузка свечей и расчет EMA200, EMA9, EMA21, Vol_SMA, ATR"""
    klines = binance_client.futures_klines(symbol=symbol, interval=TIMEFRAME, limit=210)
    df = pd.DataFrame(klines, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'
    ])
    df['close'] = df['close'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['volume'] = df['volume'].astype(float)

    # Индикаторы
    df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
    df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
    df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
    df['vol_sma20'] = df['volume'].rolling(window=20).mean()

    # ATR
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['low'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    df['atr'] = np.max(ranges, axis=1).rolling(14).mean()

    return df

def get_open_positions():
    """Проверка открытых сделок"""
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

def execute_smart_trade(symbol, action, entry_price, atr):
    """Исполнение ордера с защищенной отправкой в TG"""
    qty = QUANTITIES.get(symbol, 1.0)
    
    try:
        try:
            binance_client.futures_change_margin_type(symbol=symbol, marginType='ISOLATED')
        except BinanceAPIException:
            pass

        binance_client.futures_change_leverage(symbol=symbol, leverage=LEVERAGE)

        # 1. Основной ордер
        binance_client.futures_create_order(
            symbol=symbol, side=action, type='MARKET', quantity=qty
        )

        # Дистанции TP (2.5x ATR) / SL (1.2x ATR)
        sl_dist = atr * 1.2
        tp_dist = atr * 2.5

        if action == 'BUY':
            sl_price = round(entry_price - sl_dist, 4)
            tp_price = round(entry_price + tp_dist, 4)
            side_close = 'SELL'
        else:
            sl_price = round(entry_price + sl_dist, 4)
            tp_price = round(entry_price - tp_dist, 4)
            side_close = 'BUY'

        # 2. Стоп и Тейк
        binance_client.futures_create_order(
            symbol=symbol, side=side_close, type='STOP_MARKET', stopPrice=sl_price, closePosition=True
        )
        binance_client.futures_create_order(
            symbol=symbol, side=side_close, type='TAKE_PROFIT_MARKET', stopPrice=tp_price, closePosition=True
        )

        msg = (
            f"🎯 УМНЫЙ ВХОД ({action} | 5x)\n"
            f"Монета: {symbol} (5m)\n"
            f"Объем: {qty}\n"
            f"Вход: ~{entry_price}\n"
            f"✅ Take-Profit: {tp_price}\n"
            f"🛑 Stop-Loss: {sl_price}"
        )
        send_telegram(msg)

    except Exception as err:
        error_msg = f"❌ Ошибка открытия сделки по {symbol}: {err}"
        print(error_msg)
        send_telegram(error_msg)

def bot_loop():
    """Разумный цикл проверки сигналов"""
    time.sleep(5)
    send_telegram("🤖 Умный трендовый бот запущен (5m + EMA200)! Проверка связи OK.")
    
    while True:
        try:
            if binance_client:
                open_positions = get_open_positions()
                total_active = len(open_positions)

                for symbol in SYMBOLS:
                    if symbol in open_positions or total_active >= MAX_ACTIVE_POSITIONS:
                        continue

                    df = get_klines_df(symbol)
                    c2 = df.iloc[-2]  # Закрытая свеча
                    c3 = df.iloc[-3]  # Предпоследняя

                    # 1. Сигнал пересечения EMA9 и EMA21
                    cross_up = (c3['ema9'] <= c3['ema21']) and (c2['ema9'] > c2['ema21'])
                    cross_down = (c3['ema9'] >= c3['ema21']) and (c2['ema9'] < c2['ema21'])

                    # 2. Фильтр тренда (EMA200) и объема
                    vol_ok = c2['volume'] > (c2['vol_sma20'] * 1.1)
                    trend_long = c2['close'] > c2['ema200']
                    trend_short = c2['close'] < c2['ema200']

                    if cross_up and trend_long and vol_ok:
                        print(f"🟢 Сигнал LONG: {symbol}")
                        execute_smart_trade(symbol, 'BUY', c2['close'], c2['atr'])
                        open_positions[symbol] = 1
                        total_active += 1

                    elif cross_down and trend_short and vol_ok:
                        print(f"🔴 Сигнал SHORT: {symbol}")
                        execute_smart_trade(symbol, 'SELL', c2['close'], c2['atr'])
                        open_positions[symbol] = -1
                        total_active += 1

                    time.sleep(0.5)

        except Exception as e:
            print(f"❌ Ошибка в главном цикле: {e}")

        time.sleep(60)

Thread(target=bot_loop, daemon=True).start()

@app.route('/')
def home():
    return "🤖 Smart Trend Bot Active.", 200

@app.route('/test')
def test_tg():
    send_telegram("🔔 Проверка Telegram: Бот на связи!")
    return "OK", 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
