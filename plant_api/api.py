#!/user/bin/env python
#-*- coding: utf-8 -*-

import sensors_controller as sensors
import measurements_controller as measurements
import models
import subprocess
import sys
from flask import Flask, jsonify, abort, request
from flask_cors import CORS
import json

app = Flask(__name__)
CORS(app)

def notify():
 p = subprocess.Popen([sys.executable, 'notify.py'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

@app.route('/api/<string:table_name>', methods=['GET'])
def get_tasks(table_name):
  if table_name == "sensors":
    notify()
    return json.dumps(sensors.getTasks(1), indent=4, sort_keys=True, default=str)
  elif table_name == "measurements":
    notify()
    return json.dumps(measurements.getTasks(1), indent=4, sort_keys=True, default=str)
  return jsonify({"Error": "Not found"})

@app.route('/api/<string:table_name>/<int:task_id>', methods=['GET'])
def get_task(table_name, task_id):
  tasks = []
  if table_name == 'sensors':
    notify()
    task = sensors.getTask(task_id)
  elif table_name == 'measurements':
    notify()
    task = measurements.getTask(task_id)
  else:
    abort(404)

  tasks.append(task)

  return json.dumps(tasks[0], indent=4, sort_keys=True, default=str)

@app.route('/api/<string:table_name>/<int:task_id>/latest', methods=['GET'])
def get_latest_task(table_name, task_id):
  tasks = []
  if table_name == 'sensors':
    notify()
    return jsonify({"Error": "Not found"})
  elif table_name == 'measurements':
    notify()
    task = measurements.getLatestTask(task_id)
  else:
    abort(404)

  tasks.append(task)

  return json.dumps(tasks[0], indent=4, sort_keys=True, default=str)
   

if __name__ == '__main__':
  app.run(host='0.0.0.0', port=7777, debug=True)

