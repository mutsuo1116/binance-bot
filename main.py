import os
import requests
from flask import Flask, request, jsonify
from binance.client import Client
from binance.exceptions import BinanceAPIException

app = Flask(__name__)

BINANCE_API_KEY = os.environ.get('BINANCE_API_KEY')
BINANCE_SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

binance_client = None
if BINANCE_API_KEY and BINANCE_SECRET_KEY:
    try:
        binance_client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)
    except Exception as e:
        print(f"Ошибка инициализации Binance API: {e}")

def send_telegram(text):
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
        try:
            requests.post(url, json=payload, timeout=5)
        except Exception as e:
            print(f"Ошибка отправки в Telegram: {e}")

@app.route('/')
def home():
    return "OK", 200

@app.route('/test')
def test_tg():
    send_telegram("🛡️ ТЕСТ СВЯЗИ: Бот готов к работе с процентами SL/TP.")
    return "OK", 200

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json(force=True, silent=True) or {}
    
    action = str(data.get('action', '')).upper()
    symbol = str(data.get('symbol', 'BTCUSDT')).upper()
    raw_qty = data.get('quantity')
    raw_leverage = data.get('leverage', 3)
    sl_pct = data.get('sl_pct')  # Процент Стоп-Лосса (например, 1.5)
    tp_pct = data.get('tp_pct')  # Процент Тейк-Профита (например, 3.0)

    if not action or action not in ['BUY', 'SELL']:
        return jsonify({"status": "error", "message": "Параметр action должен быть BUY или SELL"}), 400

    if raw_qty is None:
        return jsonify({"status": "error", "message": "Не указано quantity"}), 400

    try:
        quantity = float(raw_qty)
    except (ValueError, TypeError):
        return jsonify({"status": "error", "message": "Некорректный числовой формат quantity"}), 400

    try:
        leverage = max(1, min(int(raw_leverage), 5))
    except (ValueError, TypeError):
        leverage = 3

    if not binance_client:
        send_telegram("⚠️ Ошибка: Ключи Binance API не найдены в настройках Render!")
        return jsonify({"status": "error", "message": "Binance client missing"}), 500

    try:
        # 1. Установка изолированной маржи
        try:
            binance_client.futures_change_margin_type(symbol=symbol, marginType='ISOLATED')
        except BinanceAPIException as e:
            if e.code != -4046:
                print(f"Маржа: {e.message}")

        # 2. Установка плеча
        binance_client.futures_change_leverage(symbol=symbol, leverage=leverage)

        # 3. Выполнение рыночного ордера
        order = binance_client.futures_create_order(
            symbol=symbol,
            side=action,
            type='MARKET',
            quantity=quantity
        )

        # Текущая цена для расчета SL/TP
        ticker = binance_client.futures_symbol_ticker(symbol=symbol)
        entry_price = float(ticker['price'])

        sl_info = "Без SL"
        tp_info = "Без TP"

        # 4. Расчет и выставление Стоп-Лосса (%)
        if sl_pct is not None:
            try:
                sl_percent = float(sl_pct)
                if action == 'BUY':
                    sl_price = entry_price * (1 - sl_percent / 100)
                else:
                    sl_price = entry_price * (1 + sl_percent / 100)
                
                sl_price = round(sl_price, 1 if 'BTC' in symbol else 2)
                sl_side = 'SELL' if action == 'BUY' else 'BUY'

                binance_client.futures_create_order(
                    symbol=symbol,
                    side=sl_side,
                    type='STOP_MARKET',
                    stopPrice=sl_price,
                    closePosition=True
                )
                sl_info = f"{sl_price} (-{sl_percent}%)"
            except Exception as sl_err:
                sl_info = f"Ошибка SL: {sl_err}"

        # 5. Расчет и выставление Тейк-Профита (%)
        if tp_pct is not None:
            try:
                tp_percent = float(tp_pct)
                if action == 'BUY':
                    tp_price = entry_price * (1 + tp_percent / 100)
                else:
                    tp_price = entry_price * (1 - tp_percent / 100)
                
                tp_price = round(tp_price, 1 if 'BTC' in symbol else 2)
                tp_side = 'SELL' if action == 'BUY' else 'BUY'

                binance_client.futures_create_order(
                    symbol=symbol,
                    side=tp_side,
                    type='TAKE_PROFIT_MARKET',
                    stopPrice=tp_price,
                    closePosition=True
                )
                tp_info = f"{tp_price} (+{tp_percent}%)"
            except Exception as tp_err:
                tp_info = f"Ошибка TP: {tp_err}"

        # Отправка отчета в Telegram
        send_telegram(
            f"🛡️ СДЕЛКА ОТКРЫТА\n"
            f"Направление: {action}\n"
            f"Пара: {symbol}\n"
            f"Цена входа: ~{entry_price}\n"
            f"Объем: {quantity}\n"
            f"Плечо: {leverage}x (Isolated)\n"
            f"Стоп-Лосс: {sl_info}\n"
            f"Тейк-Профит: {tp_info}"
        )

        return jsonify({"status": "success", "order": order}), 200

    except Exception as e:
        error_msg = str(e)
        send_telegram(f"❌ ОШИБКА ИСПОЛНЕНИЯ ОРДЕРА\nПара: {symbol}\nПричина: {error_msg}")
        return jsonify({"status": "error", "message": error_msg}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)                            
