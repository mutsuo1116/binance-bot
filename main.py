import os
import requests
from flask import Flask, request, jsonify
from binance.client import Client

app = Flask(__name__)

# Загрузка переменных окружения из Render
BINANCE_API_KEY = os.environ.get('BINANCE_API_KEY')
BINANCE_SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

# Инициализация клиента Binance
binance_client = None
if BINANCE_API_KEY and BINANCE_SECRET_KEY:
    try:
        binance_client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)
    except Exception as e:
        print(f"Ошибка инициализации Binance: {e}")

def send_telegram(text):
    """Отправка уведомления в Telegram"""
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
        try:
            requests.post(url, json=payload, timeout=5)
        except Exception as e:
            print(f"Ошибка отправки в ТГ: {e}")

@app.route('/')
def home():
    """Эндпоинт для работы cron-job.org"""
    return "OK", 200

@app.route('/test')
def test_tg():
    """Проверка работы Telegram-уведомлений"""
    send_telegram("🔔 **Тест связи!** Бот успешно подключен к Telegram.")
    return "Тестовое сообщение отправлено в Telegram!", 200

@app.route('/webhook', methods=['POST'])
def webhook():
    """Прием сигналов от TradingView и исполнение ордеров"""
    data = request.get_json(force=True, silent=True) or {}
    
    action = data.get('action')           # BUY или SELL
    symbol = data.get('symbol', 'BTCUSDT') # Торговая пара
    quantity = data.get('quantity')       # Количество монеты (например 0.001)
    amount_usdt = data.get('amount_usdt') # Или сумма в USDT (например 10)

    if not action:
        return jsonify({"status": "error", "message": "Не указан action (BUY или SELL)"}), 400

    if not binance_client:
        send_telegram("⚠️ **Внимание**: Сигнал получен, но API-ключи Binance не настроены на Render!")
        return jsonify({"status": "error", "message": "Binance API keys missing"}), 500

    try:
        # ПОКУПКА (BUY)
        if action.upper() == "BUY":
            if amount_usdt:
                # Покупка на конкретную сумму USDT (например, на 10$)
                order = binance_client.order_market_buy(symbol=symbol, quoteOrderQty=amount_usdt)
                send_telegram(f"🟢 **ПОКУПКА (SPOT)**\nПара: `{symbol}`\nСумма: `{amount_usdt} USDT`")
            elif quantity:
                # Покупка по точной сумме монет
                order = binance_client.order_market_buy(symbol=symbol, quantity=quantity)
                send_telegram(f"🟢 **ПОКУПКА (SPOT)**\nПара: `{symbol}`\nКоличество: `{quantity}`")
            else:
                return jsonify({"status": "error", "message": "Укажите quantity или amount_usdt"}), 400

        # ПРОДАЖА (SELL)
        elif action.upper() == "SELL":
            if quantity:
                order = binance_client.order_market_sell(symbol=symbol, quantity=quantity)
                send_telegram(f"🔴 **ПРОДАЖА (SPOT)**\nПара: `{symbol}`\nКоличество: `{quantity}`")
            else:
                return jsonify({"status": "error", "message": "Для продажи укажите quantity"}), 400
        else:
            return jsonify({"status": "error", "message": "Неверный action (только BUY или SELL)"}), 400

        return jsonify({"status": "success", "order": order}), 200

    except Exception as e:
        error_msg = str(e)
        send_telegram(f"❌ **ОШИБКА СДЕЛКИ на Binance**\nПара: `{symbol}`\nПричина: `{error_msg}`")
        return jsonify({"status": "error", "message": error_msg}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
