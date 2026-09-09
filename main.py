import os
import time
import hmac
import hashlib
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

# Ключи API загружаются из переменных окружения (Environment Variables)
BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY", "")
BINANCE_SECRET_KEY = os.environ.get("BINANCE_SECRET_KEY", "")
BINANCE_BASE_URL = "https://api.binance.com"  # Для Spot API

@app.route('/', methods=['GET'])
def health_check():
    """Проверка работы бота"""
    return jsonify({
        "status": "online",
        "message": "Binance Bot Server is running!"
    }), 200

@app.route('/webhook', methods=['POST'])
def webhook():
    """Эндпоинт для приема вебхуков (например, от TradingView или сигналов)"""
    data = request.json or {}
    print("Получен сигнал:", data)
    
    # Здесь добавляется логика обработки сигнала и выставления ордеров
    symbol = data.get("symbol", "BTCUSDT")
    side = data.get("side", "BUY")
    
    return jsonify({
        "status": "success",
        "processed": True,
        "symbol": symbol,
        "side": side
    }), 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
