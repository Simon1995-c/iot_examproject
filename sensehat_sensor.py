from sense_hat import SenseHat
import paho.mqtt.client as mqtt
import time
import ast

class Interval:
	def __init__(self, time):
		self.time_step = time

	def get_time(self):
		return self.time_step
	
	def set_time(self, time):
		self.time_step = time


ip = '192.168.0.114'
port = 1883
address = 0x48
topic = "wifi/sensor/sense_hat_sensor"
interval = Interval(10)
sense = SenseHat()	

def on_connect(client, userdata, flags, rc):
	print("Connected with result code "+str(rc))

def on_message(client, userdata, msg):
	byte_str = msg.payload[2:]
	dict_str = byte_str.decode('UTF-8')
	data = ast.literal_eval(dict_str)
	print(msg.topic+ " "+str(byte_str))
	time_step = int(data['interval'])
	interval.set_time(time_step)

def while_connected(client, interval, dry_value, wet_value):
    try:
        while True:
            moist = sense.get_humidity()
            temp = sense.get_temperature()
            press = sense.get_pressure()

            message = {'moisture':moist, 'temperature':temp, 'pressure':press}
            client.publish(topic, str(message), qos=0, retain=False)
            time_step = interval.get_time()
            print("Interval: %s" % time_step)
            time.sleep(time_step)
    except:
        print("Disconnecting")
        client.disconnect()

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set('mqtt', password='mqtt')

client.connect(ip, port, 60)
client.subscribe('admin/config_changes')
client.loop_start()
#dry, wet = calibrate()
while_connected(client, interval, 0, 256)
