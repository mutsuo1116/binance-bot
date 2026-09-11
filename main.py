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

# Пул топ-10 монет
SYMBOLS = [
    'BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 
    'XRPUSDT', 'ADAUSDT', 'DOGEUSDT', 'AVAXUSDT', 
    'NEARUSDT', 'LINKUSDT'
]

TIMEFRAME = Client.KLINE_INTERVAL_5MINUTE
LEVERAGE = 5  # Плечо 5x
MAX_ACTIVE_POSITIONS = 3  # Максимум 3 активные сделки

# Лоты под плечо 5x (~$45–$50 позиция на монету)
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
    """Отправка сообщений в Telegram"""
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        try:
            requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=5)
        except Exception as e:
            print(f"❌ Ошибка Telegram: {e}")

def get_klines_df(symbol):
    """Загрузка свечей и расчет индикаторов"""
    klines = binance_client.futures_klines(symbol=symbol, interval=TIMEFRAME, limit=200)
    df = pd.DataFrame(klines, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'
    ])
    df['close'] = df['close'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['volume'] = df['volume'].astype(float)

    # Индикаторы EMA
    df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
    df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
    df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()

    # RSI 14
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))

    # ATR 14 и Volume MA
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['low'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    df['atr'] = true_range.rolling(14).mean()
    df['atr_ma'] = df['atr'].rolling(20).mean()
    df['vol_ma'] = df['volume'].rolling(20).mean()

    return df

def get_open_positions_dict():
    """Сбор всех открытых позиций за один вызов"""
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

def execute_trade(symbol, action, entry_price, atr):
    """Открытие сделки с плечом 5x и RR 1:3"""
    qty = QUANTITIES.get(symbol, 1.0)
    try:
        binance_client.futures_change_margin_type(symbol=symbol, marginType='ISOLATED')
    except BinanceAPIException:
        pass

    binance_client.futures_change_leverage(symbol=symbol, leverage=LEVERAGE)

    # Рыночный ордер
    binance_client.futures_create_order(
        symbol=symbol, side=action, type='MARKET', quantity=qty
    )

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

    binance_client.futures_create_order(
        symbol=symbol, side=side_close, type='STOP_MARKET', stopPrice=sl_price, closePosition=True
    )
    binance_client.futures_create_order(
        symbol=symbol, side=side_close, type='TAKE_PROFIT_MARKET', stopPrice=tp_price, closePosition=True
    )

    send_telegram(
        f"🚀 СДЕЛКА ОТКРЫТА (Плечо 5x | {action})\n"
        f"Монета: {symbol}\n"
        f"Вход: ~{entry_price}\n"
        f"🎯 Take-Profit: {tp_price}\n"
        f"🛡️ Stop-Loss: {sl_price}"
    )

def market_analyzer_loop():
    """Главный цикл анализа с детальным логированием"""
    time.sleep(5)
    print("🚀 Сканер запущен (5m, 5x, с детальными логами)!")
    
    while True:
        try:
            if binance_client:
                print(f"\n==========================================")
                print(f"[{time.strftime('%H:%M:%S')}] 🔍 Сканирование рынка...")
                
                # Анализируем BTC
                btc_df = get_klines_df('BTCUSDT')
                btc_last = btc_df.iloc[-2]
                btc_bullish = btc_last['close'] > btc_last['ema200']
                btc_status = "Бычий 🟢" if btc_bullish else "Медвежий 🔴"
                
                open_positions = get_open_positions_dict()
                total_active = len(open_positions)
                
                print(f"📊 Тренд BTC: {btc_status} (Close: {btc_last['close']:.1f} | EMA200: {btc_last['ema200']:.1f})")
                print(f"💼 Открытых сделок: {total_active}/{MAX_ACTIVE_POSITIONS}")

                for symbol in SYMBOLS:
                    try:
                        if symbol in open_positions:
                            print(f"  📌 {symbol}: Позиция уже открыта. Пропуск.")
                            continue

                        if total_active >= MAX_ACTIVE_POSITIONS:
                            print(f"  ⏸️ {symbol}: Достигнут лимит активных сделок ({MAX_ACTIVE_POSITIONS}).")
                            continue

                        df = get_klines_df(symbol)
                        c2 = df.iloc[-2] # Прошлая закрытая свеча
                        c3 = df.iloc[-3] # Свеча перед ней
                        c4 = df.iloc[-4]

                        # Детекция свежего пересечения EMA9 и EMA21 (за последние 2 свечи)
                        cross_up = (c3['ema9'] <= c3['ema21'] and c2['ema9'] > c2['ema21']) or \
                                   (c4['ema9'] <= c4['ema21'] and c3['ema9'] > c3['ema21'])
                                   
                        cross_down = (c3['ema9'] >= c3['ema21'] and c2['ema9'] < c2['ema21']) or \
                                     (c4['ema9'] >= c4['ema21'] and c3['ema9'] < c3['ema21'])

                        vol_ok = c2['volume'] > c2['vol_ma']
                        storm = c2['atr'] > (c2['atr_ma'] * 3.0)

                        long_cond = btc_bullish and (c2['close'] > c2['ema200']) and cross_up and (c2['rsi'] < 68) and vol_ok and not storm
                        short_cond = (not btc_bullish) and (c2['close'] < c2['ema200']) and cross_down and (c2['rsi'] > 32) and vol_ok and not storm

                        if long_cond:
                            print(f"  🟢 {symbol}: НАЙДЕН СИГНАЛ LONG! Входим...")
                            execute_trade(symbol, 'BUY', c2['close'], c2['atr'])
                            open_positions[symbol] = 1
                            total_active += 1
                        elif short_cond:
                            print(f"  🔴 {symbol}: НАЙДЕН СИГНАЛ SHORT! Входим...")
                            execute_trade(symbol, 'SELL', c2['close'], c2['atr'])
                            open_positions[symbol] = -1
                            total_active += 1
                        else:
                            # Лог с причиной, почему сделку пропускаем
                            reason = []
                            if btc_bullish and c2['close'] <= c2['ema200']: reason.append("Ниже EMA200")
                            elif not btc_bullish and c2['close'] >= c2['ema200']: reason.append("Выше EMA200")
                            if not (cross_up or cross_down): reason.append("Нет свежего перекреста EMA")
                            if not vol_ok: reason.append("Малый объем")
                            if storm: reason.append("Шторм волатильности")
                            
                            print(f"  🔎 {symbol}: Ожидание ({', '.join(reason)}) | RSI: {c2['rsi']:.1f}")

                        time.sleep(0.5)

                    except Exception as coin_err:
                        print(f"  ❌ Ошибка по {symbol}: {coin_err}")

        except Exception as e:
            print(f"❌ Ошибка в главном цикле: {e}")

        time.sleep(120)

Thread(target=market_analyzer_loop, daemon=True).start()

@app.route('/')
def home():
    return "🤖 Торговый бот активен.", 200

@app.route('/test')
def test_tg():
    send_telegram("🛡️ ТЕСТ: Телеграм-уведомления работают отлично!")
    return "OK", 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
