import socket
import asyncio
import re
from hbmqtt.mqtt.packet import (
    MQTTFixedHeader, MQTTPacket, CONNECT, CONNACK, SUBSCRIBE, SUBSCRIBE, UNSUBSCRIBE, UNSUBACK, PUBLISH, DISCONNECT, PINGREQ, PINGRESP
)
from hbmqtt.mqtt import packet_class
import hbmqtt.mqtt.connect as MQTTConnect
import hbmqtt.mqtt.connack as MQTTConnack
import hbmqtt.mqtt.subscribe as MQTTSubscribe
import hbmqtt.mqtt.unsubscribe as MQTTUnsubscribe
from hbmqtt.mqtt.pingresp import PingRespPacket
from hbmqtt.mqtt.publish import PublishPacket
import namegenerator as name
from collections import deque

class Session:

    def __init__(self):
        self._packet_id = 0
        self.remote_address = None
        self.remote_port = None
        self.client_id = None
        self.username = None
        self.password = None
        self.keep_alive = 0
        self.writer = None
        self.reader = None

    def generate_client_id(self):
        return name.gen()
    
    def get_next_packet_id(self):
        self._packet_id += 1
        if self._packet_id > 65535:
            self._packet_id = 1
        
        return self._packet_id

    @asyncio.coroutine
    def publish(self, topic, payload):
        packet_id = self.get_next_packet_id()
        packet = PublishPacket.build(topic, payload, packet_id, qos=0, retain=False, dup_flag=None)
        yield from packet.to_stream(self.writer)

class Broker:

    def __init__(self):
        self._sessions = dict()
        self._ip = '0.0.0.0'
        self._port = 1883
        self._username = 'mqtt'
        self._password = 'mqtt'
        self._subscriptions = dict()
        self._loop = asyncio.get_event_loop()
        self._broadcast_queue = asyncio.Queue(loop=self._loop)
    
    @asyncio.coroutine
    def start(self):
        broadcast_task = asyncio.ensure_future(self.broadcast_loop(), loop=self._loop)
        try:
            server = yield from asyncio.start_server(self.client_connected, host=self._ip, port=self._port, loop=self._loop)
        except:
            print('Client disconnected externally')


    @asyncio.coroutine
    def client_connected(self, reader, writer):

        #handle connection and create session to add to the list of sessions
        #read connection packet from the client

        connect = yield from MQTTConnect.ConnectPacket.from_stream(reader)
        if (connect.payload.client_id is None):
            generate_id = True
        else:
            generate_id = False
        
        #send a connack packet to client depending on correct credentials
        connack = None
        if (connect.username == self._username and connect.password == self._password):
            connack = MQTTConnack.ConnackPacket.build(0, MQTTConnack.CONNECTION_ACCEPTED)
            connected = True
        else:
            connack = MQTTConnack.ConnackPacket.build(0, MQTTConnack.BAD_USERNAME_PASSWORD)
            connected = False
        
        writer.write(connack.to_bytes())

        if (not connected):
            return

        new_session = Session()
        if (generate_id):
            new_session.client_id = new_session.generate_client_id()
        else:
            new_session.client_id = connect.client_id
        new_session.remote_address = writer.get_extra_info('peername')
        new_session.remote_port = writer.get_extra_info('socket')
        new_session.username = connect.username
        new_session.password = connect.password
        new_session.keep_alive = connect.keep_alive
        new_session.writer = writer
        new_session.reader = reader
        
        
        self._sessions[new_session.client_id] = new_session
        print("New client; %s connected from %s with keep_alive: %s" % (new_session.client_id, new_session.remote_address, new_session.keep_alive))

        #received packets are checked and the corresponding method is used (publish, sub/unsub, disconnect)
        try:
            while connected:
                mqtt_header = yield from asyncio.wait_for(MQTTFixedHeader.from_stream(reader), new_session.keep_alive+1, loop=self._loop)
                
                cls = packet_class(fixed_header=mqtt_header)
                packet = yield from cls.from_stream(reader, fixed_header=mqtt_header)

                if (packet.fixed_header.packet_type == SUBSCRIBE):
                    for (topic, qos) in packet.payload.topics:
                        if (qos != 0):
                            print("QoS is different than expected")
                            print(qos)
                        self.add_subscription(topic, new_session)
                elif (packet.fixed_header.packet_type == UNSUBSCRIBE):
                    for (topic, qos) in packet.payload.topics:
                        if (qos != 0):
                            print("QoS is different than expected")
                            print(qos)
                        self.del_subscription(topic, new_session)
                elif (packet.fixed_header.packet_type == PUBLISH):
                    topic = packet.variable_header.topic_name
                    message = packet.payload.data
                    print("Broadcast; Topic: %s, Payload: %s" % (topic, message))
                    yield from self.broadcast_message(new_session, topic, message)
                elif (packet.fixed_header.packet_type == DISCONNECT):
                    self.del_all_subscriptions(new_session)
                    self.del_session(new_session.client_id)
                    connected = False
                    print("Client: %s disconnected" % (new_session.client_id))
                elif (packet.fixed_header.packet_type == PINGREQ):
                    pingresp_packet = PingRespPacket.build()
                    writer.write(pingresp_packet.to_bytes())
                else:
                    print('Not yet supported packet received')
        except asyncio.TimeoutError:
            print("Client %s didn't respond; diconnecting" % new_session.client_id)
            self.del_session(new_session.client_id)

    def add_subscription(self, subscription, session):
        if (subscription not in self._subscriptions):
            self._subscriptions[subscription] = []

        match = []
        for s in self._subscriptions[subscription]:
            if (s.client_id == session.client_id):
                match.append(s)
                s.reader = session.reader
                s.writer = session.writer
                

        if (len(match) == 0):
            self._subscriptions[subscription].append(session)
            print("Client %s subscribed to %s" % (session.client_id, subscription))
        else:   
            print("Client %s has already subscribed to %s" % (session.client_id, subscription))
        

    def del_subscription(self, topic, session):
        deleted = False
        subscriptions = self._subscriptions[topic]

        for index, sub_session in enumerate(subscriptions):
            if (sub_session.client_id == session.client_id):
                subscriptions.pop(index)
                deleted = True
                break
        
        return deleted

    def del_all_subscriptions(self, session):
        topic_queue = deque()

        for topic in self._subscriptions:
            if self.del_subscription(topic, session):
                topic_queue.append(topic)
        
        for topic in topic_queue:
            if not self._subscriptions[topic]:
                del self._subscriptions[topic]

    @asyncio.coroutine
    def broadcast_loop(self):
        while True:
            broadcast = yield from self._broadcast_queue.get()

            for sub in self._subscriptions:
                if (broadcast['topic'].startswith('&') and (sub.startswith("+") or sub.startswith("#"))):
                    print("[MQTT-4.7.2-1] - Ignoring broadcasting $ topic to subscriptions starting with + or #")
                elif (self.match(broadcast['topic'], sub)):
                    subscriptions = self._subscriptions[sub]

                    for match_sub in subscriptions:
                        task = asyncio.ensure_future(match_sub.publish(broadcast['topic'], broadcast['data']), loop=self._loop)
    
    def match(self, topic, match_topic):
        if ("#" not in match_topic and "+" not in match_topic):
            #if no wildcards are in matched topic, return if topics are equal (i.e match)
            match = topic == match_topic
            return match
        else:
            #regex to match patterns of strings preceding the topic and including wildcards
            pattern = match_topic.replace('#', '.*').replace('$', '\$').replace('+', '[/\$\s\w\d]')
            match = re.match(pattern, topic)
            return match

    
    @asyncio.coroutine
    def broadcast_message(self, session, topic, payload):
        broadcast = {
            'session': session,
            'topic': topic,
            'data': payload
        }

        yield from self._broadcast_queue.put(broadcast)

    def del_session(self, client_id):
        session = self._sessions[client_id]

        self.del_all_subscriptions(session)

        del self._sessions[client_id]