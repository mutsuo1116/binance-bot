import os
import requests
from flask import Flask, request, jsonify
from binance.client import Client
from binance.exceptions import BinanceAPIException

app = Flask(__name__)

# Загрузка конфигурации из Environment Variables
BINANCE_API_KEY = os.environ.get('BINANCE_API_KEY')
BINANCE_SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

# Инициализация API клиента
binance_client = None
if BINANCE_API_KEY and BINANCE_SECRET_KEY:
    try:
        binance_client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)
    except Exception as e:
        print(f"Ошибка инициализации Binance API: {e}")

def send_telegram(text):
    """Надежная отправка уведомлений без сбоев форматирования"""
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
        try:
            requests.post(url, json=payload, timeout=5)
        except Exception as e:
            print(f"Ошибка отправки в Telegram: {e}")

@app.route('/')
def home():
    """Эндпоинт для поддержания активности через cron-job"""
    return "OK", 200

@app.route('/test')
def test_tg():
    """Тест работы уведомлений"""
    send_telegram("🛡️ ТЕСТ СВЯЗИ: Бот готов к безопасной торговле на Binance Futures.")
    return "OK", 200

@app.route('/webhook', methods=['POST'])
def webhook():
    """Обработка вебхуков TradingView и исполнение фьючерсных ордеров"""
    data = request.get_json(force=True, silent=True) or {}
    
    action = str(data.get('action', '')).upper()
    symbol = str(data.get('symbol', 'BTCUSDT')).upper()
    raw_qty = data.get('quantity')
    raw_leverage = data.get('leverage', 3)
    stop_loss_price = data.get('sl')

    # Валидация базовых входящих данных
    if not action or action not in ['BUY', 'SELL']:
        return jsonify({"status": "error", "message": "Параметр action должен быть BUY или SELL"}), 400

    if raw_qty is None:
        return jsonify({"status": "error", "message": "Не указано quantity"}), 400

    try:
        quantity = float(raw_qty)
    except (ValueError, TypeError):
        return jsonify({"status": "error", "message": "Некорректный числовой формат quantity"}), 400

    # Безопасное ограничение плеча (от 1x до 5x)
    try:
        leverage = max(1, min(int(raw_leverage), 5))
    except (ValueError, TypeError):
        leverage = 3

    if not binance_client:
        send_telegram("⚠️ Ошибка: Ключи Binance API не найдены в настройках Render!")
        return jsonify({"status": "error", "message": "Binance client missing"}), 500

    try:
        # 1. Установка изолированной маржи для защиты баланса
        try:
            binance_client.futures_change_margin_type(symbol=symbol, marginType='ISOLATED')
        except BinanceAPIException as e:
            if e.code != -4046:  # Ошибка -4046 означает, что ISOLATED уже включена
                print(f"Маржа: {e.message}")

        # 2. Установка выбранного размера плеча
        binance_client.futures_change_leverage(symbol=symbol, leverage=leverage)

        # 3. Выполнение рыночного ордера (BUY / SELL)
        order = binance_client.futures_create_order(
            symbol=symbol,
            side=action,
            type='MARKET',
            quantity=quantity
        )

                # 4. Выставление защитного Стоп-Лосса (SL)
        sl_info = "Без SL"
        if stop_loss_price:
            try:
                sl_price = float(stop_loss_price)
                sl_side = 'SELL' if action == 'BUY' else 'BUY'
                binance_client.futures_create_order(
                    symbol=symbol,
                    side=sl_side,
                    type='STOP_MARKET',
                    stopPrice=sl_price,
                    closePosition=True
                )
                sl_info = f"{sl_price}"
            except Exception as sl_err:
                sl_info = f"Ошибка SL: {sl_err}"

        # 5. Выставление Тейк-Профита (TP)
        tp_info = "Без TP"
        take_profit_price = data.get('tp')
        if take_profit_price:
            try:
                tp_price = float(take_profit_price)
                tp_side = 'SELL' if action == 'BUY' else 'BUY'
                binance_client.futures_create_order(
                    symbol=symbol,
                    side=tp_side,
                    type='TAKE_PROFIT_MARKET',
                    stopPrice=tp_price,
                    closePosition=True
                )
                tp_info = f"{tp_price}"
            except Exception as tp_err:
                tp_info = f"Ошибка TP: {tp_err}"

        # Отправка отчета в Telegram
        send_telegram(
            f"🛡️ СДЕЛКА ОТКРЫТА\n"
            f"Направление: {action}\n"
            f"Пара: {symbol}\n"
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
