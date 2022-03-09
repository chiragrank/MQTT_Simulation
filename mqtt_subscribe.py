# python3.6

import random

from paho.mqtt import client as mqtt_client


def connect_mqtt(client_id, broker, port) -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client, topic_list):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")

    for topic in topic_list:
        client.subscribe(topic)
    client.on_message = on_message


def run():
    broker = "127.0.0.1"
    port = 1883
    topic_list = ["python/mqtt", "java/mqtt"]
    client_id = f"python-mqtt-{random.randint(0, 100)}"

    client = connect_mqtt(client_id, broker, port)
    subscribe(client, topic_list)
    client.loop_forever()


if __name__ == "__main__":
    run()
