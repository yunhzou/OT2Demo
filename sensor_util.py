import json
import secrets
import threading
from time import time
from queue import Queue, Empty

import paho.mqtt.client as paho

# Configuration
course_id = "absurd-gazelle"
username = "sgbaird"
password = "D.Pq5gYtejYbU#L"
host = "248cc294c37642359297f75b7b023374.s2.eu.hivemq.cloud"
as7341_topic = f"{course_id}/as7341"
session_id = secrets.token_hex(4)

# Save session ID for autograding
with open("session_id.txt", "w") as f:
    f.write(session_id)

def get_client_and_queue(subscribe_topic, host, username, password=None, port=8883, tls=True):
    client = paho.Client()
    queue = Queue()
    connected_event = threading.Event()

    def on_message(client, userdata, msg):
        queue.put(json.loads(msg.payload))

    def on_connect(client, userdata, flags, rc):
        client.subscribe(subscribe_topic, qos=2)
        connected_event.set()

    client.on_connect = on_connect
    client.on_message = on_message

    if tls:
        client.tls_set(tls_version=paho.ssl.PROTOCOL_TLS_CLIENT)  # type: ignore
    client.username_pw_set(username, password)
    client.connect(host, port)
    connected_event.wait(timeout=10.0)
    
    return client, queue

def get_color_sensor_data(client, queue, queue_timeout=30, function_timeout=300):
    command_topic = 'absurd-gazelle/neopixel'
    payload = '{"command": {"R": 0.2, "G": 0.5, "B": 0.3}, "experiment_id": "target", "session_id": "aaf818f3"}'
    client.publish(command_topic, payload, qos=2) # this act as a trigger
    client.loop_start()
    t0 = time()
    
    while time() - t0 <= function_timeout:
        try:
            results = queue.get(True,timeout=queue_timeout)
            print(results)
            if isinstance(results, dict):
                client.loop_stop()
                return results["sensor_data"]
        except Empty:
            raise TimeoutError(f"Sensor data retrieval timed out ({queue_timeout} seconds)")

    raise TimeoutError(f"Function timed out ({function_timeout} seconds)")

# Set up MQTT client and subscribe to the topic
mqtt_client, queue = get_client_and_queue(as7341_topic, host, username, password)

# Retrieve and print sensor data
sensor_data = get_color_sensor_data(mqtt_client, queue)
print(f"Sensor data: {sensor_data}")
