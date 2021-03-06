#!/Users/rfrei/Documents/Development/Virtual/virt2/bin/python3

import time
import datetime
import json
import redis
import random
import paho.mqtt.client as mqtt

# define event callbacks
def on_connect(client, userdata, flags, rc):
    print("[on_connect] CONNACK received with code: %d" % (rc))

def on_subscribe(client, userdata, mid, granted_qos):
    print("[on_subscribe] Subscribed to: " + str(mid) + ", QOS: " + str(granted_qos))

def on_message(client, userdata, message):
    print("[on_message] Topic: " + message.topic + ", QOS: " + str(message.qos) + ", Payload: " + str(message.payload))

def on_publish(client, userdata, mid):
    print("[on_publish] Message ID: " + str(mid))

def on_disconnect(client, userdata, rc):
	if rc != 0:
		print("Unexpected Disconnect")
		mqttc.loop_stop(force = False)

# instantiate MQTT client
mqttc = mqtt.Client(client_id = "", clean_session = True, userdata = None, protocol = mqtt.MQTTv311)

# set callbacks
mqttc.on_connect = on_connect
mqttc.on_publish = on_publish
mqttc.on_subscribe = on_subscribe
mqttc.on_message = on_message
mqttc.on_disconnect = on_disconnect

# connect to MQTT broker (raspberry pi)
mqttc.connect("localhost", 1883, keepalive = 60, bind_address = "")
mqttc.loop_start() # start background thread

# subscribe to topic
mqttc.subscribe("ishnalaIOT", qos = 0)

# connect to redis server (localhost)
r = redis.StrictRedis(host = 'localhost', port = 6379, db = 0)

run = True
while run:

    # generate random temperature between 0 and 15 (60 - 75)
    temp = 60 + random.randrange(16)

    # generate timestamp
    ts_id = str(random.randrange(1001)).zfill(4)
    timestamp = time.strftime("sensor_01_%Y_%m_%d_%H_%M_%S_", time.localtime())
    timestamp = "".join((timestamp, ts_id))

    # construct JSON message as string
    msg1 = json.dumps({
        timestamp: {
            'type': 'temperature',
            'location': 'study',
            'sensorValue': temp,
    }})
    print("Original String: " + msg1 + "\n")

    # push the data to redis_list
    r.lpush('redis_list', msg1)
    r.ltrim('redis_list', 0, 99)
    redis_list = [json.loads(list_item.decode('utf-8')) for list_item in r.lrange('redis_list', 0, -1)]
    print("Redis List 1: " + str(redis_list) + "\n")

    #(rc, mid) = mqttc.publish("ishnalaIOT", '{"preload_sensor_01": ' + str(redis_list) + '}', qos = 0)

    time.sleep(5)
