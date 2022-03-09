# python 3.6

import json
import random
import time

from paho.mqtt import client as mqtt_client


class RawMessage(object):
    def __init__(self, key, payload=None, jsondata=None):
        self.key = key
        self.payload = payload
        if jsondata:
            self.payload = json.dumps(jsondata).encode("utf-8")

    @property
    def jsondata(self):
        return json.loads(self.payload.decode("utf-8"))

    def __repr__(self):
        return f"RawMessage(key='{self.key}',payload={self.payload})"


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


def publish(client, topic_msg_list, sleep_time):
    # msg_count = 0
    # while True:
    for idx, msg in enumerate(topic_msg_list):
        time.sleep(sleep_time)
        try:
            topic = msg["topic"]
        except:
            print("No topic found")
            topic = "error/absent_topic"
        result = client.publish(topic, json.dumps(msg).encode("utf-8"))
        status = result[0]
        if status == 0:
            print(f"Send `{msg}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")


def run():
    sleep_time = 0.5
    broker = "127.0.0.1"
    port = 1883
    client_id = f"python-mqtt-{random.randint(0, 1000)}"

    filename = "study-3_spiral-3_pilot_NotHSRData_TrialMessages_Trial-T000448_Team-TM000074_Member-na_CondBtwn-ASI-UAZ-TA1_CondWin-na_Vers-1.metadata"
    with open(filename, encoding="utf8") as f:
        data_list = f.readlines()
    topic_msg_list = [json.loads(l) for l in data_list]

    print(f"Len of the list if {len(topic_msg_list)}")
    print(f"First message is {topic_msg_list[1]}")
    print(f"type :{type(topic_msg_list[0])}")
    print(f"msg_keys: {topic_msg_list[1].keys()}")
    client = connect_mqtt(client_id, broker, port)
    client.loop_start()
    publish(client, topic_msg_list, sleep_time)


if __name__ == "__main__":
    run()
