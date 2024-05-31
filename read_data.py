import websocket
import json
from datetime import datetime

def on_message(ws, message):
    data = json.loads(message)
    if 'data' in data:
        for trade in data['data']:
            transformed_trade = {
                "price": trade['p'],
                "currency": trade['s'],
                "ts": datetime.utcfromtimestamp(trade['t'] / 1000).strftime('%Y-%m-%dT%H:%M:%S'),
                "volume": trade['v']
            }
            print(json.dumps(transformed_trade))

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

# def on_open(ws):
#     ws.send('{"type":"subscribe","symbol":"AAPL"}')
#     ws.send('{"type":"subscribe","symbol":"AMZN"}')
#     ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')
#     ws.send('{"type":"subscribe","symbol":"IC MARKETS:1"}')

def run_websocket():
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://ws.finnhub.io?token=cm239g9r01qvesfhfk70cm239g9r01qvesfhfk7g",
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)
    #ws.on_open = on_open
    ws.run_forever()

if __name__ == "__main__":
    run_websocket()
