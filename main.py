import os
import time
import requests
import pandas as pd
import numpy as np
from threading import Thread
from flask import Flask, jsonify
from binance.client import Client
from binance.exceptions import BinanceAPIException

app = Flask(__name__)

# Загрузка конфигурации из Environment Variables
BINANCE_API_KEY = os.environ.get('BINANCE_API_KEY')
BINANCE_SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

# Настройки стратегии
SYMBOL = 'BTCUSDT'
TIMEFRAME = Client.KLINE_INTERVAL_15MINUTE  # Официальный таймфрейм 15 минут
LEVERAGE = 3
QUANTITY = 0.001  # Размер позиции в BTC

# Инициализация клиента Binance
binance_client = None
if BINANCE_API_KEY and BINANCE_SECRET_KEY:
    try:
        binance_client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)
    except Exception as e:
        print(f"Ошибка инициализации Binance: {e}")

def send_telegram(text):
    """Отправка сообщений в Telegram"""
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        try:
            requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=5)
        except Exception as e:
            print(f"Ошибка Telegram: {e}")

def get_klines_df():
    """Загрузка исторических свечей и расчет всех 4 факторов"""
    klines = binance_client.futures_klines(symbol=SYMBOL, interval=TIMEFRAME, limit=250)
    df = pd.DataFrame(klines, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'
    ])
    df['close'] = df['close'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['volume'] = df['volume'].astype(float)

    # 1. Скользящие средние (EMA)
    df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
    df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
    df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()

    # 2. Индекс относительной силы (RSI)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))

    # 3. Средний истинный диапазон (ATR)
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    df['atr'] = true_range.rolling(14).mean()

    # 4. Объемная скользящая
    df['vol_ma'] = df['volume'].rolling(20).mean()

    return df

def execute_trade(action, entry_price, atr):
    """Исполнение ордера с динамическими SL/TP по волатильности ATR"""
    try:
        binance_client.futures_change_margin_type(symbol=SYMBOL, marginType='ISOLATED')
    except BinanceAPIException as e:
        if e.code != -4046:
            pass

    binance_client.futures_change_leverage(symbol=SYMBOL, leverage=LEVERAGE)

    # Рыночный ордер на вход
    order = binance_client.futures_create_order(
        symbol=SYMBOL, side=action, type='MARKET', quantity=QUANTITY
    )

    # Динамический расчет SL (1.5x ATR) и TP (3.0x ATR)
    sl_dist = atr * 1.5
    tp_dist = atr * 3.0

    if action == 'BUY':
        sl_price = round(entry_price - sl_dist, 1)
        tp_price = round(entry_price + tp_dist, 1)
        side_close = 'SELL'
    else:
        sl_price = round(entry_price + sl_dist, 1)
        tp_price = round(entry_price - tp_dist, 1)
        side_close = 'BUY'

    # Выставление Стоп-Лосса и Тейк-Профита на бирже
    binance_client.futures_create_order(
        symbol=SYMBOL, side=side_close, type='STOP_MARKET', stopPrice=sl_price, closePosition=True
    )
    binance_client.futures_create_order(
        symbol=SYMBOL, side=side_close, type='TAKE_PROFIT_MARKET', stopPrice=tp_price, closePosition=True
    )

    send_telegram(
        f"🤖 АВТО-СДЕЛКА ОТКРЫТА ({action})\n"
        f"Пара: {SYMBOL}\n"
        f"Вход: ~{entry_price}\n"
        f"Стоп-Лосс (1.5x ATR): {sl_price}\n"
        f"Тейк-Профит (3.0x ATR): {tp_price}"
    )

def market_analyzer_loop():
    """Фоновый цикл проверки рынка каждые 15 минут"""
    while True:
        try:
            if binance_client:
                df = get_klines_df()
                last = df.iloc[-2]      # Последняя закрытая свеча
                prev = df.iloc[-3]      # Предпоследняя закрытая свеча

                # Проверка открытых позиций
                positions = binance_client.futures_position_information(symbol=SYMBOL)
                has_position = False
                for p in positions:
                    if p['symbol'] == SYMBOL and float(p['positionAmt']) != 0:
                        has_position = True
                        break

                if not has_position:
                    # Условия LONG: Тренд бычий + Пересечение EMA9/21 вверх + RSI < 68 + Объем выше среднего
                    long_cond = (
                        (last['close'] > last['ema200']) and
                        (prev['ema9'] <= prev['ema21']) and (last['ema9'] > last['ema21']) and
                        (last['rsi'] < 68) and
                        (last['volume'] > last['vol_ma'])
                    )

                    # Условия SHORT: Тренд медвежий + Пересечение EMA9/21 вниз + RSI > 32 + Объем выше среднего
                    short_cond = (
                        (last['close'] < last['ema200']) and
                        (prev['ema9'] >= prev['ema21']) and (last['ema9'] < last['ema21']) and
                        (last['rsi'] > 32) and
                        (last['volume'] > last['vol_ma'])
                    )

                    if long_cond:
                        execute_trade('BUY', last['close'], last['atr'])
                    elif short_cond:
                        execute_trade('SELL', last['close'], last['atr'])

        except Exception as e:
            print(f"Ошибка в цикле анализа рынка: {e}")

        time.sleep(900)  # Пауза 15 минут (900 секунд)

# Запуск аналитического потока
Thread(target=market_analyzer_loop, daemon=True).start()

@app.route('/')
def home():
    return "🤖 Многофакторный алгоритмический бот активен и ведет анализ рынка.", 200

@app.route('/test')
def test_tg():
    send_telegram("🛡️ ТЕСТ СВЯЗИ: Автономная аналитическая система готова к работе.")
    return "OK", 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
