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

# Переменные окружения из Railway
BINANCE_API_KEY = os.environ.get('BINANCE_API_KEY')
BINANCE_SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

SYMBOLS = [
    'BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 
    'XRPUSDT', 'ADAUSDT', 'DOGEUSDT', 'AVAXUSDT', 
    'NEARUSDT', 'LINKUSDT'
]

TIMEFRAME = Client.KLINE_INTERVAL_3MINUTE
LEVERAGE = 5  # Плечо 5x
MAX_ACTIVE_POSITIONS = 3  # Максимум 3 сделки

# ИСПРАВЛЕННЫЕ ЛОТЫ: Полноценные позиции ~$50 на монету (Залог ~$10)
QUANTITIES = {
    'BTCUSDT': 0.001,   # ~$60
    'ETHUSDT': 0.02,    # ~$50
    'SOLUSDT': 0.35,    # ~$49
    'BNBUSDT': 0.09,    # ~$49
    'XRPUSDT': 90.0,    # ~$49
    'ADAUSDT': 140.0,   # ~$49
    'DOGEUSDT': 500.0,  # ~$50
    'AVAXUSDT': 2.0,    # ~$50
    'NEARUSDT': 11.0,   # ~$49
    'LINKUSDT': 4.5     # ~$49
}

binance_client = None
if BINANCE_API_KEY and BINANCE_SECRET_KEY:
    try:
        binance_client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)
        print("✅ Binance API успешно подключен.")
    except Exception as e:
        print(f"❌ Ошибка подключения Binance: {e}")

def send_telegram(text):
    """Отправка сообщений в Telegram"""
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        try:
            requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=5)
        except Exception as e:
            print(f"❌ Ошибка Telegram: {e}")

def get_klines_df(symbol):
    """Расчет скальперских индикаторов"""
    klines = binance_client.futures_klines(symbol=symbol, interval=TIMEFRAME, limit=100)
    df = pd.DataFrame(klines, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'
    ])
    df['close'] = df['close'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['volume'] = df['volume'].astype(float)

    # Скользящие средние EMA 7 и EMA 21
    df['ema7'] = df['close'].ewm(span=7, adjust=False).mean()
    df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()

    # RSI 14
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))

    # ATR 14
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['low'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    df['atr'] = true_range.rolling(14).mean()

    return df

def get_open_positions_dict():
    """Проверка активных сделок"""
    active_dict = {}
    try:
        positions = binance_client.futures_position_information()
        for p in positions:
            amt = float(p['positionAmt'])
            if amt != 0:
                active_dict[p['symbol']] = amt
    except Exception as e:
        print(f"⚠️ Ошибка получения позиций: {e}")
    return active_dict

def execute_scalp_trade(symbol, action, entry_price, atr):
    """Исполнение скальп-ордера на ~$50"""
    qty = QUANTITIES.get(symbol, 1.0)
    try:
        binance_client.futures_change_margin_type(symbol=symbol, marginType='ISOLATED')
    except BinanceAPIException:
        pass

    binance_client.futures_change_leverage(symbol=symbol, leverage=LEVERAGE)

    # Вход в сделку
    binance_client.futures_create_order(
        symbol=symbol, side=action, type='MARKET', quantity=qty
    )

    # Скальперские стоп и тейк
    sl_dist = atr * 1.0
    tp_dist = atr * 1.8

    if action == 'BUY':
        sl_price = round(entry_price - sl_dist, 4)
        tp_price = round(entry_price + tp_dist, 4)
        side_close = 'SELL'
    else:
        sl_price = round(entry_price + sl_dist, 4)
        tp_price = round(entry_price - tp_dist, 4)
        side_close = 'BUY'

    binance_client.futures_create_order(
        symbol=symbol, side=side_close, type='STOP_MARKET', stopPrice=sl_price, closePosition=True
    )
    binance_client.futures_create_order(
        symbol=symbol, side=side_close, type='TAKE_PROFIT_MARKET', stopPrice=tp_price, closePosition=True
    )

    send_telegram(
        f"⚡ СКАЛЬП-СДЕЛКА ({action} | 5x)\n"
        f"Монета: {symbol} (3m)\n"
        f"Объем: {qty} {symbol.replace('USDT','')}\n"
        f"Вход: ~{entry_price}\n"
        f"🎯 Take-Profit: {tp_price}\n"
        f"🛡️ Stop-Loss: {sl_price}"
    )

def scalper_loop():
    """Главный скальперский цикл"""
    time.sleep(5)
    print("⚡ СКАЛЬПИНГ-БОТ ЗАПУЩЕН (Позиции ~$50)! Сканирование...")
    
    while True:
        try:
            if binance_client:
                print(f"\n⚡ [{time.strftime('%H:%M:%S')}] Сканирование микро-трендов...")
                open_positions = get_open_positions_dict()
                total_active = len(open_positions)
                print(f"💼 Активных позиций: {total_active}/{MAX_ACTIVE_POSITIONS}")

                for symbol in SYMBOLS:
                    try:
                        if symbol in open_positions:
                            continue

                        if total_active >= MAX_ACTIVE_POSITIONS:
                            break

                        df = get_klines_df(symbol)
                        c2 = df.iloc[-2]
                        c3 = df.iloc[-3]

                        cross_up = (c3['ema7'] <= c3['ema21']) and (c2['ema7'] > c2['ema21'])
                        cross_down = (c3['ema7'] >= c3['ema21']) and (c2['ema7'] < c2['ema21'])

                        long_cond = cross_up and (38 < c2['rsi'] < 68)
                        short_cond = cross_down and (32 < c2['rsi'] < 62)

                        if long_cond:
                            print(f"  🟢 {symbol}: ИМПУЛЬС ВВЕРХ (LONG)! Открываем сделку на ~$50...")
                            execute_scalp_trade(symbol, 'BUY', c2['close'], c2['atr'])
                            open_positions[symbol] = 1
                            total_active += 1
                        elif short_cond:
                            print(f"  🔴 {symbol}: ИМПУЛЬС ВНИЗ (SHORT)! Открываем сделку на ~$50...")
                            execute_scalp_trade(symbol, 'SELL', c2['close'], c2['atr'])
                            open_positions[symbol] = -1
                            total_active += 1

                        time.sleep(0.3)

                    except Exception as coin_err:
                        print(f"  ❌ Ошибка по {symbol}: {coin_err}")

        except Exception as e:
            print(f"❌ Ошибка в цикле: {e}")

        time.sleep(45)

Thread(target=scalper_loop, daemon=True).start()

@app.route('/')
def home():
    return "⚡ Скальпинг-бот активен.", 200

@app.route('/test')
def test_tg():
    send_telegram("⚡ ТЕСТ: Бот работает!")
    return "OK", 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
