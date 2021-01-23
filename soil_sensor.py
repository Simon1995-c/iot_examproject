import paho.mqtt.client as mqtt
import smbus2
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
time_step = 10
topic = "wifi/sensor/plant_sensor_2"
interval = Interval(10)
PCF8591 = smbus2.SMBus(1)
PCF8591.write_byte(address, 0x03)

def calibrate_loop(boundary):
	calibrating = True
	value = PCF8591.read_byte(address)
	while calibrating:
		for i in range(3):
			new_value = PCF8591.read_byte(address)
			if (value != new_value):
				value = new_value
				calibrating = True
				break
			else:
				calibrating = False
			time.sleep(5)
	
	print("Calibration of %s done: %s" % (boundary, value))
	return value
		
def calibrate():
	print("Calibrate dry environment; press enter when ready")
	raw_input()
	dry_value = calibrate_loop("Dryness")
	print("Calibrate wet environment; press enter when ready")
	raw_input()
	max_value = calibrate_loop("Wetness")
	return dry_value, max_value
	
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
	factor = float(wet_value - dry_value) / 100
	try:
		while True:
			soil_moist_8bit = PCF8591.read_byte(address)
			soil_moist = float(soil_moist_8bit - dry_value) / factor 
			print(soil_moist)

			message = {'moisture':soil_moist}
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
while_connected(client, interval, 0, 256)
