# import jwt  # PyJWT
# import uuid
import websocket  # websocket-client
from kafka import KafkaProducer  # kafka-python
from kafka.errors import KafkaError
from json import dumps

global producer
producer = KafkaProducer(acks=0,  # 0: Producer will not wait for any acknowledgment from the server.
                         client_id='test_producer',  # a name for this client
                         compression_type='gzip',
                         # my kafka server IP
                         bootstrap_servers=['3.35.168.158:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8')
                         )


def on_message(ws, message):
    # do something
    data = message.decode('utf-8')
    producer.send('test', value=data)
    producer.flush()
    print(data)


def on_connect(ws):
    print("connected!")
    # Request after connection
    ws.send('[{"ticket":"test"},{"type":"ticker","codes":["KRW-BTC"]}]')


def on_error(ws, err):
    print(err)


def on_close(ws, status_code, msg):
    print("closed!")


ws_app = websocket.WebSocketApp("wss://api.upbit.com/websocket/v1",
                                # header=headers,
                                on_message=on_message,
                                on_open=on_connect,
                                on_error=on_error,
                                on_close=on_close
                                )

print(on_message)

ws_app.run_forever()
