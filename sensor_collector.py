import paho.mqtt.client as mqtt
import mysql.connector as mariadb
from models import Sensor
import time
import ast

class SensorList:
    def __init__(self):
        self.list = []

    def get_items(self):
        return self.list

    def append_item(self, topic):
        self.list.append(topic)

mariadb_connection = mariadb.connect(user='web', password='raspberry', database='PlantHubDB')
cursor = mariadb_connection.cursor()
sensors = SensorList()

def closeDBConnection():
    cursor.close()
    mariadb_connection.close()
    return

def get_sensor(name):
    mariadb_connection = mariadb.connect(user='web', password='raspberry', database='PlantHubDB')
    cursor = mariadb_connection.cursor()
    data = []
    cursor.execute("SELECT ID FROM Sensor WHERE Name='{}'".format(name))
    data = cursor.fetchall()

    entry = data[0][0]

    closeDBConnection()
    return entry

def add_measure(sensor_id, moist, temp, pres):
    mariadb_connection = mariadb.connect(user='web', password='raspberry', database='PlantHubDB')
    cursor = mariadb_connection.cursor()
    cursor.execute("INSERT INTO Measurement (SensorId, Moisture, Temperature, Pressure) VALUES ({},{},{},{})".format(sensor_id, moist, temp, pres))
    mariadb_connection.commit()

    closeDBConnection()
    return

def add_sensor(name, type):
    mariadb_connection = mariadb.connect(user='web', password='raspberry', database='PlantHubDB')
    cursor = mariadb_connection.cursor()
    cursor.execute("INSERT INTO Sensor (Name, Type) VALUES ('{}', '{}')".format(name, type))
    mariadb_connection.commit()

    closeDBConnection()
    return

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    #Ignoring the bridge data
    if ('zigbee2mqtt/bridge' in str(msg.topic)):
        return
    byte_str = msg.payload[2:]
    dict_str = byte_str.decode('UTF-8')
    data = ast.literal_eval(dict_str)
    print(msg.topic+ " "+str(byte_str))

    name = str(msg.topic).replace("wifi/sensor/", " ").replace("zigbee2mqtt/", " ")
    name = name[1:]
    try:
        sensor_id = get_sensor(name)
    except:
        if ('wifi' in str(msg.topic)):
            sensor_type = 'Wifi'
        elif ('zigbee' in str(msg.topic)):
            sensor_type = 'Zigbee'
        else:
            sensor_type = 'Unknown'
        add_sensor(name, sensor_type)

    sensor_id = get_sensor(name)
    print(sensor_id)

    moisture = 0
    try:
        moisture = data['moisture']
    except:
        try:
            moisture = data['humidity']
        except:
            pass

    temperature = 0
    try:
        temperature = data['temperature']
    except:
        pass

    pressure = 0
    try:
        pressure = data['pressure']
    except:
        pass

    print(pressure, temperature, moisture)
    add_measure(int(sensor_id), moisture, temperature, pressure)

client = mqtt.Client(client_id='sensor_collector')
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set('mqtt', password='mqtt')

client.connect("localhost", 1883, 60)

client.subscribe("wifi/sensor/#")
client.subscribe("zigbee2mqtt/#")
client.loop_forever()
