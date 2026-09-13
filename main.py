import os
import time
import requests
import hmac
import hashlib
from urllib.parse import urlencode
from threading import Thread
from flask import Flask

app = Flask(__name__)

# ==================== НАСТРОЙКИ API И TELEGRAM ====================
API_KEY = os.environ.get('BINANCE_API_KEY', 'ТВОЙ_API_KEY')
API_SECRET = os.environ.get('BINANCE_SECRET_KEY', 'ТВОЙ_SECRET_KEY')
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', 'ТВОЙ_TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', 'ТВОЙ_CHAT_ID')

BASE_URL = "https://fapi.binance.com"

# ==================== РИСК-МЕНЕДЖМЕНТ И ПАРАМЕТРЫ ====================
LEVERAGE = 5              # Кредитное плечо
TARGET_USDT = 50.0        # Объем позиции в USDT (маржа ~10 USDT с плечом 5x)

# Урезанный и жесткий список из 30 топовых и ликвидных пар
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", 
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT", 
    "MATICUSDT", "UNIUSDT", "ATOMUSDT", "LTCUSDT", "ETCUSDT", 
    "NEARUSDT", "APTUSDT", "FTMUSDT", "ARBUSDT", "OPUSDT", 
    "INJUSDT", "SUIUSDT", "RNDRUSDT", "TIAUSDT", "SEIUSDT", 
    "IMXUSDT", "RENDERUSDT", "PEPEUSDT", "SHIBUSDT", "WIFUSDT"
]

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
        requests.post(url, data=payload, timeout=5)
    except Exception as e:
        print(f"Ошибка отправки в Telegram: {e}")

def get_signature(query_string):
    return hmac.new(API_SECRET.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

def send_signed_request(http_method, url_path, payload={}):
    query_string = urlencode(payload)
    if query_string:
        query_string = f"{query_string}&timestamp={int(time.time() * 1000)}"
    else:
        query_string = f"timestamp={int(time.time() * 1000)}"
    
    signature = get_signature(query_string)
    url = f"{BASE_URL}{url_path}?{query_string}&signature={signature}"
    headers = {"X-MBX-APIKEY": API_KEY}
    
    try:
        if http_method == "GET":
            response = requests.get(url, headers=headers, timeout=10)
        elif http_method == "POST":
            response = requests.post(url, headers=headers, timeout=10)
        elif http_method == "DELETE":
            response = requests.delete(url, headers=headers, timeout=10)
        return response.json()
    except Exception as e:
        print(f"Ошибка запроса {url_path}: {e}")
        return None

def set_leverage(symbol):
    send_signed_request("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": LEVERAGE})

def format_price(symbol, price):
    if "BTC" in symbol:
        return round(price, 1)
    elif "ETH" in symbol:
        return round(price, 2)
    else:
        return round(price, 4)

def get_actual_entry_price(symbol):
    positions = send_signed_request("GET", "/fapi/v2/positionRisk", {"symbol": symbol})
    if positions and isinstance(positions, list):
        for p in positions:
            if float(p['positionAmt']) != 0:
                return float(p['entryPrice']), float(p['positionAmt'])
    return None, 0.0

def clean_leftover_orders(symbol):
    orders = send_signed_request("GET", "/fapi/v1/openOrders", {"symbol": symbol})
    if orders and isinstance(orders, list):
        for o in orders:
            send_signed_request("DELETE", "/fapi/v1/order", {"symbol": symbol, "orderId": o['orderId']})

def get_historical_klines(symbol, interval="15m", limit=50):
    url = f"{BASE_URL}/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        if isinstance(data, list):
            return [float(x[4]) for x in data]
    except:
        pass
    return []

def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    gains, losses = 0.0, 0.0
    for i in range(1, period + 1):
        diff = prices[-i] - prices[-i-1]
        if diff >= 0:
            gains += diff
        else:
            losses -= diff
    avg_gain = gains / period
    avg_loss = losses / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

def check_strict_signal(symbol):
    closes = get_historical_klines(symbol, "15m", 50)
    if len(closes) < 30:
        return None
    
    current_price = closes[-1]
    ema_20 = sum(closes[-20:]) / 20
    ema_50 = sum(closes[-50:]) / 50
    rsi = calculate_rsi(closes, 14)

    if current_price > ema_20 > ema_50 and rsi < 35:
        return "BUY"
    if current_price < ema_20 < ema_50 and rsi > 65:
        return "SELL"
    return None

def manage_trailing_stops():
    account_info = send_signed_request("GET", "/fapi/v2/account")
    if not account_info or 'positions' not in account_info:
        return
        
    for p in account_info['positions']:
        symbol = p['symbol']
        position_amt = float(p['positionAmt'])
        if position_amt == 0:
            continue
            
        try:
            entry_price, _ = get_actual_entry_price(symbol)
            if not entry_price:
                continue
                
            ticker = requests.get(f"{BASE_URL}/fapi/v1/ticker/price?symbol={symbol}", timeout=5).json()
            current_price = float(ticker['price'])
            
            is_long = position_amt > 0
            if is_long:
                profit_pct = (current_price - entry_price) / entry_price * 100
                if profit_pct >= 2.0:
                    clean_leftover_orders(symbol)
                    new_sl = format_price(symbol, entry_price * 1.002)
                    new_tp = format_price(symbol, entry_price * 1.04)
                    
                    send_signed_request("POST", "/fapi/v1/order", {
                        "symbol": symbol, "side": "SELL", "type": "STOP_MARKET", "stopPrice": new_sl, "closePosition": "true"
                    })
                    send_signed_request("POST", "/fapi/v1/order", {
                        "symbol": symbol, "side": "SELL", "type": "TAKE_PROFIT_MARKET", "stopPrice": new_tp, "closePosition": "true"
                    })
                    send_telegram(f"🛡 *Трейлинг:* `{symbol}` переведен в безубыток (+4%).")
            else:
                profit_pct = (entry_price - current_price) / entry_price * 100
                if profit_pct >= 2.0:
                    clean_leftover_orders(symbol)
                    new_sl = format_price(symbol, entry_price * 0.998)
                    new_tp = format_price(symbol, entry_price * 0.96)
                    
                    send_signed_request("POST", "/fapi/v1/order", {
                        "symbol": symbol, "side": "BUY", "type": "STOP_MARKET", "stopPrice": new_sl, "closePosition": "true"
                    })
                    send_signed_request("POST", "/fapi/v1/order", {
                        "symbol": symbol, "side": "BUY", "type": "TAKE_PROFIT_MARKET", "stopPrice": new_tp, "closePosition": "true"
                    })
                    send_telegram(f"🛡 *Трейлинг:* `{symbol}` переведен в безубыток (-4%).")
        except Exception as e:
            print(f"Ошибка трейлинга {symbol}: {e}")

def bot_loop():
    time.sleep(5)
    send_telegram("🚀 *Бот запущен на Flask-сервере!* Режим: Жёсткий топ-30 + RSI + защита ТП.")
    while True:
        try:
            manage_trailing_stops()
            
            for symbol in SYMBOLS:
                signal = check_strict_signal(symbol)
                if signal:
                    _, current_amt = get_actual_entry_price(symbol)
                    if current_amt != 0:
                        continue
                        
                    set_leverage(symbol)
                    ticker = requests.get(f"{BASE_URL}/fapi/v1/ticker/price?symbol={symbol}", timeout=5).json()
                    price = float(ticker['price'])
                    
                    qty = round((TARGET_USDT * LEVERAGE) / price, 3)
                    
                    order_res = send_signed_request("POST", "/fapi/v1/order", {
                        "symbol": symbol, "side": signal, "type": "MARKET", "quantity": qty
                    })
                    
                    if order_res and 'orderId' in order_res:
                        if signal == "BUY":
                            sl_price = format_price(symbol, price * 0.985)
                            tp_price = format_price(symbol, price * 1.03)
                            sl_side, tp_side = "SELL", "SELL"
                        else:
                            sl_price = format_price(symbol, price * 1.015)
                            tp_price = format_price(symbol, price * 0.97)
                            sl_side, tp_side = "BUY", "BUY"
                            
                        send_signed_request("POST", "/fapi/v1/order", {
                            "symbol": symbol, "side": sl_side, "type": "STOP_MARKET", "stopPrice": sl_price, "closePosition": "true"
                        })
                        send_signed_request("POST", "/fapi/v1/order", {
                            "symbol": symbol, "side": tp_side, "type": "TAKE_PROFIT_MARKET", "stopPrice": tp_price, "closePosition": "true"
                        })
                        
                        send_telegram(f"⚡ *Жёсткий вход:* `{symbol}` | *{signal}* | Цена: `{price}`")
                        
            time.sleep(60)
        except Exception as e:
            print(f"Главный цикл ошибки: {e}")
            time.sleep(10)

# Запускаем торговый цикл в отдельном потоке, чтобы Flask работал как веб-сервер для Railway
Thread(target=bot_loop, daemon=True).start()

@app.route('/')
def home():
    return "🤖 Strict Top-30 Sniper Bot is Active.", 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
    
