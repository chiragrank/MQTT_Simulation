# python 3.6

import random
import time

from paho.mqtt import client as mqtt_client


def connect_mqtt(client_id, broker, port):
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish(client, topic_list):
    msg_count = 0
    while True:
        for idx, topic in enumerate(topic_list):
            time.sleep(1)
            msg = f"messages: {idx}"
            result = client.publish(topic, msg)
            status = result[0]
            if status == 0:
                print(f"Send `{msg}` to topic `{topic}`")
            else:
                print(f"Failed to send message to topic {topic}")
            msg_count += 1


def run():
    broker = "127.0.0.1"
    port = 1883
    topic_list = ["python/mqtt", "java/mqtt"]
    client_id = f"python-mqtt-{random.randint(0, 1000)}"
    client = connect_mqtt(client_id, broker, port)
    client.loop_start()
    publish(client, topic_list)


if __name__ == "__main__":
    run()
