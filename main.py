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

# Конфигурация из переменных окружения (ENV в Railway)
BINANCE_API_KEY = os.environ.get('BINANCE_API_KEY')
BINANCE_SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

# Топовый пул ликвидных альткоинов
SYMBOLS = [
    'BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 
    'XRPUSDT', 'ADAUSDT', 'DOGEUSDT', 'AVAXUSDT', 
    'NEARUSDT', 'LINKUSDT'
]

# Оптимальный таймфрейм для высокой скорости и точности: 5 минут
TIMEFRAME = Client.KLINE_INTERVAL_5MINUTE
LEVERAGE = 5  # Оптимальное плечо 5x
MAX_ACTIVE_POSITIONS = 3  # Максимум 3 активные сделки одновременно для защиты баланса

# Сбалансированные увеличенные лоты под плечо 5x (~$45–$50 позиции на монету)
QUANTITIES = {
    'BTCUSDT': 0.0012,
    'ETHUSDT': 0.015,
    'SOLUSDT': 0.08,
    'BNBUSDT': 0.03,
    'XRPUSDT': 15.0,
    'ADAUSDT': 25.0,
    'DOGEUSDT': 50.0,
    'AVAXUSDT': 0.5,
    'NEARUSDT': 1.2,
    'LINKUSDT': 0.35
}

binance_client = None
if BINANCE_API_KEY and BINANCE_SECRET_KEY:
    try:
        binance_client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)
        print("✅ Binance API успешно подключен.")
    except Exception as e:
        print(f"❌ Ошибка подключения Binance: {e}")

def send_telegram(text):
    """Мгновенное отправление уведомлений в Telegram"""
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        try:
            requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=5)
        except Exception as e:
            print(f"❌ Ошибка Telegram: {e}")

def get_klines_df(symbol):
    """Загрузка свечей и расчет профессиональных индикаторов"""
    klines = binance_client.futures_klines(symbol=symbol, interval=TIMEFRAME, limit=200)
    df = pd.DataFrame(klines, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'
    ])
    df['close'] = df['close'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['volume'] = df['volume'].astype(float)

    # Скользящие средние (EMA)
    df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
    df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
    df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()

    # Индекс относительной силы (RSI 14)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))

    # Волатильность (ATR 14)
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['low'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    df['atr'] = true_range.rolling(14).mean()
    df['atr_ma'] = df['atr'].rolling(20).mean()

    # Объем (Volume MA)
    df['vol_ma'] = df['volume'].rolling(20).mean()

    return df

def count_total_open_positions():
    """Подсчет активных позиций портфеля"""
    active_count = 0
    try:
        positions = binance_client.futures_position_information()
        for p in positions:
            if float(p['positionAmt']) != 0 and p['symbol'] in SYMBOLS:
                active_count += 1
    except Exception as e:
        print(f"⚠️ Ошибка проверки позиций: {e}")
    return active_count

def execute_trade(symbol, action, entry_price, atr):
    """Исполнение ордера с плечом 5x и соотношением 1 к 3"""
    qty = QUANTITIES.get(symbol, 1.0)
    try:
        # Устанавливаем изолированную маржу
        binance_client.futures_change_margin_type(symbol=symbol, marginType='ISOLATED')
    except BinanceAPIException as e:
        if e.code != -4046:  # Если маржа уже изолированная — игнорируем ошибку
            pass

    # Выставляем плечо 5x
    binance_client.futures_change_leverage(symbol=symbol, leverage=LEVERAGE)

    # Рыночный вход
    binance_client.futures_create_order(
        symbol=symbol, side=action, type='MARKET', quantity=qty
    )

    # Расчет стоп-лосса (1.2 ATR) и тейк-профита (3.6 ATR) -> строго 1:3
    sl_dist = atr * 1.2
    tp_dist = atr * 3.6

    if action == 'BUY':
        sl_price = round(entry_price - sl_dist, 4)
        tp_price = round(entry_price + tp_dist, 4)
        side_close = 'SELL'
    else:
        sl_price = round(entry_price + sl_dist, 4)
        tp_price = round(entry_price - tp_dist, 4)
        side_close = 'BUY'

    # Установка Stop Loss и Take Profit на бирже
    binance_client.futures_create_order(
        symbol=symbol, side=side_close, type='STOP_MARKET', stopPrice=sl_price, closePosition=True
    )
    binance_client.futures_create_order(
        symbol=symbol, side=side_close, type='TAKE_PROFIT_MARKET', stopPrice=tp_price, closePosition=True
    )

    # Уведомление в Telegram
    send_telegram(
        f"🚀 ПРОФИ-СДЕЛКА (Плечо 5x | {action})\n"
        f"Монета: {symbol} (ТФ: 5м)\n"
        f"Цена входа: ~{entry_price}\n"
        f"Соотношение Риск/Прибыль: 1 к 3\n"
        f"🎯 Take-Profit: {tp_price}\n"
        f"🛡️ Stop-Loss: {sl_price}"
    )

def market_analyzer_loop():
    """Высокоскоростной цикл сканирования рынка"""
    time.sleep(5)
    print("🚀 Профессиональный торговый комплекс запущен (5m, Плечо 5x)!")
    
    while True:
        try:
            if binance_client:
                print(f"\n[{time.strftime('%H:%M:%S')}] 🔍 Сканирование 10 пар...")
                
                # Поводырь рынка (BTC)
                btc_df = get_klines_df('BTCUSDT')
                btc_last = btc_df.iloc[-2]
                btc_bullish = btc_last['close'] > btc_last['ema200']
                
                total_active = count_total_open_positions()

                for symbol in SYMBOLS:
                    try:
                        # Проверяем наличие уже открытой сделки по монете
                        positions = binance_client.futures_position_information(symbol=symbol)
                        has_pos = any(float(p['positionAmt']) != 0 for p in positions if p['symbol'] == symbol)

                        if has_pos or total_active >= MAX_ACTIVE_POSITIONS:
                            continue

                        df = get_klines_df(symbol)
                        last = df.iloc[-2]
                        prev = df.iloc[-3]

                        # Защита от резких аномальных сквизов
                        if last['atr'] > (last['atr_ma'] * 3.0):
                            continue

                        # Точные профессиональные условия входа
                        long_cond = (
                            btc_bullish and
                            (last['close'] > last['ema200']) and
                            (prev['ema9'] <= prev['ema21']) and (last['ema9'] > last['ema21']) and
                            (last['rsi'] < 68) and
                            (last['volume'] > last['vol_ma'])
                        )

                        short_cond = (
                            (not btc_bullish) and
                            (last['close'] < last['ema200']) and
                            (prev['ema9'] >= prev['ema21']) and (last['ema9'] < last['ema21']) and
                            (last['rsi'] > 32) and
                            (last['volume'] > last['vol_ma'])
                        )

                        if long_cond:
                            print(f"  🟢 {symbol}: Сигнал LONG (5м)! Входим...")
                            execute_trade(symbol, 'BUY', last['close'], last['atr'])
                            total_active += 1
                        elif short_cond:
                            print(f"  🔴 {symbol}: Сигнал SHORT (5м)! Входим...")
                            execute_trade(symbol, 'SELL', last['close'], last['atr'])
                            total_active += 1

                        time.sleep(1) # Короткая пауза между монетами

                    except Exception as coin_err:
                        print(f"  ❌ Ошибка по {symbol}: {coin_err}")

        except Exception as e:
            print(f"❌ Ошибка в главном цикле: {e}")

        # Пауза между кругами сканирования (2 минуты)
        time.sleep(120)

# Запуск сканера в фоновом потоке
Thread(target=market_analyzer_loop, daemon=True).start()

@app.route('/')
def home():
    return "🤖 Профессиональный торговый бот работает (5m, Плечо 5x).", 200

@app.route('/test')
def test_tg():
    send_telegram("🛡️ ТЕСТ: Профи-комплекс (5x) успешно подключен!")
    return "OK", 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
