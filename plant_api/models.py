#!/usr/bin/env python
# -*- coding: utf-8 -*-

class Sensor:
    def __init__(self, ID, Name, Type):
        self.data = {
            'ID' : ID,
            'Name' : Name,
            'Type' : Type,
        }
    def __getitem__(self, key):
        return self.data[key]

class Measurement:
    def __init__(self, ID, SensorId, Date, Moisture, Temperature, Pressure):
        self.data = {
            'ID' : ID,
            'SensorId' : SensorId,
            'Date' : Date,
            'Moisture' : Moisture,
            'Temperature' : Temperature,
            'Pressure': Pressure
        }
        
    def __getitem__(self, key):
        return self.data[key]