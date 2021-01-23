import paho.mqtt.client as mqtt
import time
import paho.mqtt.publish as publish

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))

client = mqtt.Client(client_id='SimonNielsen')
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set('mqtt', password='mqtt')

client.connect("192.168.0.114", 1883, 60)

client.publish("admin/config_changes", '{"interval":600}', qos=0, retain=False)

time.sleep(10)
client.disconnect()