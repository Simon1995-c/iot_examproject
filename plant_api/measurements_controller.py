from models import Measurement
import mysql.connector as mariadb

## CREATE A DB WITH MARIADB ##
mariadb_connection = mariadb.connect(user='web', password='raspberry', database='PlantHubDB')
cursor = mariadb_connection.cursor()

def closeConnection():
    cursor.close()
    mariadb_connection.close()
    return

def getTasks(amount):
    mariadb_connection = mariadb.connect(user='web', password='raspberry', database='PlantHubDB')
    cursor = mariadb_connection.cursor()
    all_data = []
    cursor.execute("SELECT * FROM Measurement")
    all_entries = cursor.fetchall()

    for row in all_entries:
        entry = Measurement(row[0], row[1], row[2], row[3], row[4], row[5])
        all_data.append(entry.data)

    closeConnection()
    return all_data

def getTask(task_id):
    mariadb_connection = mariadb.connect(user='web', password='raspberry', database='PlantHubDB')
    cursor = mariadb_connection.cursor()
    cursor.execute("SELECT * FROM Measurement WHERE ID={}".format(task_id))
    entry = cursor.fetchall()

    data = Measurement(entry[0][0], entry[0][1], entry[0][2], entry[0][3], entry[0][4], entry[0][5])

    closeConnection()
    return data.data

def getLatestTask(sensor_id):
    mariadb_connection = mariadb.connect(user='web', password='raspberry', database='PlantHubDB')
    cursor = mariadb_connection.cursor()
    cursor.execute("SELECT * FROM Measurement WHERE Date=( SELECT MAX(Date) FROM Measurement WHERE SensorId={} )".format(sensor_id))
    entry = cursor.fetchall()

    n = len(entry) - 1
    data = Measurement(entry[n][0], entry[n][1], entry[n][2], entry[n][3], entry[n][4], entry[n][5])

    closeConnection()
    return data.data