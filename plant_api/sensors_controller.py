from models import Sensor
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
    cursor.execute("SELECT * FROM Sensor")
    all_entries = cursor.fetchall()

    for row in all_entries:
        entry = Sensor(row[0], row[1], row[2])
        all_data.append(entry.data)

    closeConnection()
    return all_data

def getTask(task_id):
    mariadb_connection = mariadb.connect(user='web', password='raspberry', database='PlantHubDB')
    cursor = mariadb_connection.cursor()
    cursor.execute("SELECT * FROM Sensor WHERE ID={}".format(task_id))
    entry = cursor.fetchall()

    data = Sensor(entry[0][0], entry[0][1], entry[0][2])

    closeConnection()
    return data.data
 