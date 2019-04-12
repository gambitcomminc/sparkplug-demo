#!/usr/bin/env python

##############################################################################
## Copyright (c) 2019 by Gambit Communications, Inc.
## All Rights Reserved
## This is released under the GNU GPL 3.0
## https://www.gnu.org/licenses/gpl-3.0.en.html
##############################################################################

from __future__ import print_function

import os 
import getopt
import sys
import socket
import time
import logging
import threading
import multiprocessing
import webbrowser
import binascii

# Sparkplug support
sys.path.insert(0, "sparkplug/client_libraries/python/")
import sparkplug_b as sparkplug
from sparkplug_b import *

import paho.mqtt.client as mqtt

import json


###########################################################################


###########################################################################
debug = True
if debug:
  from colorlog import ColoredFormatter
  def setup_logger():
    """Return a logger with a default ColoredFormatter."""
    formatter = ColoredFormatter(
        "(%(threadName)-9s) %(log_color)s%(levelname)-8s%(reset)s %(message_log_color)s%(message)s",
        datefmt=None,
        reset=True,
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red',
        },
        secondary_log_colors={
            'message': {
                'ERROR': 'red',
                'CRITICAL': 'red',
                'DEBUG': 'yellow'
            }
        },
        style='%'
    )

    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

  def config_logger():
    # debug.setLogger(debug.Debug('all'))

    if main.verbose:
	formatting = '%(levelname)s %(asctime)s (%(module)s) - %(message)s'
	logging.basicConfig(level=logging.DEBUG, format=formatting, )

	logger.setLevel(logging.DEBUG)
    else:
	formatting = '%(levelname)s %(asctime)s (%(module)s) - %(message)s'
	logging.basicConfig(level=logging.ERROR, format=formatting, )

	logger.setLevel(logging.ERROR)




  # Create a player
  logger = setup_logger()

  from functools import wraps
  def trace(func):
    """Tracing wrapper to log when function enter/exit happens.
    :param func: Function to wrap
    :type func: callable
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.debug('Start {!r}'. format(func.__name__))
        result = func(*args, **kwargs)
        logger.debug('End {!r}'. format(func.__name__))
        return result
    return wrapper

else:

  from functools import wraps
  def trace(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        return result
    return wrapper


###########################################################################
# Topic statistics
class Topic():
	def __init__(self, number, topic, bytes, last_time, last_payload, rowref):
		self.topic = topic
		self.number = number
		self.count = 1;		# seen at least once
		self.last_count = 0;	# updated every second
		self.bytes = bytes
		self.last_time = last_time
		self.last_payload = last_payload
		self.rowref = rowref

	def dump(self, outfile):
		print ("%6d %20s %6d %6d %s" % (self.number, self.topic, self.count, self.bytes, self.last_payload), file=outfile)

###########################################################################
# EON statistics
class Eon():
	def __init__(self, number, name, last_time):
		self.name = name
		self.number = number
		self.count = 1;		# seen at least once
		self.last_time = last_time

	def dump(self, outfile):
		print ("%6d %20s %6d %6d %s" % (self.number, self.name, self.count, self.last_time), file=outfile)

###########################################################################
# Device statistics
class Device():
	def __init__(self, number, name, last_time):
		self.name = name
		self.number = number
		self.count = 1;		# seen at least once
		self.last_time = last_time

	def dump(self, outfile):
		print ("%6d %20s %6d %6d %s" % (self.number, self.name, self.count, self.last_time), file=outfile)

###########################################################################
# Tag statistics
class Tag():
	def __init__(self, number, name, last_time):
		self.name = name
		self.number = number
		self.count = 1;		# seen at least once
		self.last_time = last_time

	def dump(self, outfile):
		print ("%6d %20s %6d %6d %s" % (self.number, self.name, self.count, self.last_time), file=outfile)

###########################################################################
# MQTT subscriber code
# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    logging.debug ("MQTT client connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(main.topic, main.qos)

# The callback for when a PUBLISH message is received from the server.
# updates metrics for later GUI display
def on_message(client, userdata, msg):
    if main.clear_stats:
	main.messages_received = 0
	main.topics = dict()
	main.num_topics = 0
	main.clear_stats = False
	main.clear_store = True

    bytes = len (msg.payload)
    now = time.time()

#    logging.debug (msg.topic + ' ' + str(bytes) + ' ' + str(msg.payload))

    main.messages_received += 1

    # if not already there, add to set of topics detected
    if msg.topic not in main.topics:
    	main.num_topics += 1
	newtopic = Topic(main.num_topics, msg.topic, bytes, now, msg.payload, None)
	main.topics[msg.topic] = newtopic
    else:
    	existingtopic = main.topics[msg.topic]
	existingtopic.count += 1
	existingtopic.bytes += bytes
	existingtopic.last_time = now
	existingtopic.last_payload = msg.payload

    # decode Sparkplug messages, but only on spBv1.0/# topic
    tokens = msg.topic.split("/")
    if tokens[0] != "spBv1.0":
    	logging.debug ("ignored non-Sparkplug message on topic " + msg.topic)
    	return

    # TODO filter by groupid
    groupid = tokens[1]
    msgtype = tokens[2]
    eon = tokens[3]
    
    if eon not in main.eons:
    	main.num_eons += 1
	neweon = Eon(main.num_eons, eon, now)
	main.eons[eon] = neweon
	if msgtype != 'NBIRTH':
		logging.debug (msgtype + ' --> EON ' + eon + ' needs rebirth')
		main.num_eons_offline += 1

    else:
	if msgtype == 'NDEATH':
		logging.debug (msgtype + ' --> EON ' + eon + ' offline')
		main.num_eons_offline += 1
	if msgtype == 'NBIRTH':
		logging.debug (msgtype + ' --> EON ' + eon + ' online')
		if main.num_eons_offline > 0:
			main.num_eons_offline -= 1

    if msgtype == 'DBIRTH' or msgtype == 'DDATA' or msgtype == 'DDEATH':
	device = eon + '/' + tokens[4]

	if device not in main.devices:
		main.num_devices += 1
		newdevice = Device(main.num_devices, device, now)
		main.devices[device] = newdevice
		if msgtype != 'DBIRTH':
			logging.debug (msgtype + ' --> Device ' + device + ' needs rebirth')
			main.num_devices_offline += 1
		else:
			logging.debug (msgtype + ' --> Device ' + device + ' online')

	else:
		if msgtype == 'DDEATH':
			logging.debug (msgtype + ' --> Device ' + device + ' offline')
			main.num_devices_offline += 1

		if msgtype == 'DBIRTH':
			if main.num_devices_offline > 0:
				main.num_devices_offline -= 1

	# look at the payload only for DDATA
	if msgtype != 'DDATA':
		return

	inboundPayload = sparkplug_b_pb2.Payload()
	inboundPayload.ParseFromString(msg.payload)
	for metric in inboundPayload.metrics:
		logging.debug (msgtype + ' --> Device ' + device + ' metric ' + metric.name + ' = ' + str(metric.int_value))
		tag = device + '/' + metric.name
		if tag not in main.tags:
			main.num_tags += 1
			newtag = Tag(main.num_tags, tag, now)
			main.tags[tag] = newtag
		
		if metric.name != 'XDK/temp':
			logging.debug ('Device ' + device + ' ignored metric ' + metric.name)
			continue

		# logging.debug ('Device ' + device + ' metric ' + metric.name + ' = ' + str(metric.int_value))
		if metric.int_value > main.thresh:
			logging.error ('********* Device ' + device + ' metric '+ metric.name + ' greater ' + str(main.thresh))
			main.total_triggered += 1
			if device not in main.triggered_set:
				main.triggered_set.add(device)

    return


def on_disconnect(client, userdata, rc):

    if rc != 0:
    	logging.error ("unexpected disconnect: " + str(rc))

def subscriber_client(addr):
	client = mqtt.Client()
	client.on_connect = on_connect
	client.on_message = on_message
	client.on_disconnect = on_disconnect

	client.connect(main.host_ip, main.port_num, 60)

	client.loop_forever(False)


###########################################################################
class MyApp:
	def __init__(self):
		self.host_ip = None
		self.port_num = None
		self.verbose = False
		self.topic = 'spBv1.0/#'
		self.qos = 0
		self.thresh = 70000

		self.is_stopped = False
		self.is_paused = False

		self.messages_received = 0
		self.last_received = 0
		self.num_topics = 0
		self.topics = dict()
		self.clear_stats = False
		self.clear_store = False

		self.num_eons = 0
		self.num_eons_offline = 0
		self.eons = dict()

		self.num_devices = 0
		self.num_devices_offline = 0
		self.devices = dict()

		self.num_tags = 0
		self.tags = dict()

		self.total_triggered = 0
		self.triggered_set = set()

	def usage(self):
		print ("Usage: mqtt-stats.py")
		print ("\t[-h|--host host]        broker to connect to; default localhost")
		print ("\t[-p|--port port]        port to connect to; default port 1883")
		print ("\t[-t|--topic topic]      topic; default spBv1.0/#")
		print ("\t[-q|--qos qos]          QoS; default 0")
		print ("\t[-t|--thresh threshold] temperature threshold; default 70000")
		print ("\t[-v|--verbose]    verbose output")
		return

	def start(self):
		self.command_line()

		config_logger ()

		subscriber_client (self.host_ip)
		logger.debug('after subscriber_client')

	###############################
	def command_line(self):
		try:
			opts, args = getopt.getopt(sys.argv[1:], "h:p:t:q:T:v", ["host=", "port=", "topic=", "qos=", "thresh=", "verbose"])
		except getopt.GetoptError as err:
			# print help information and exit:
			logging.error (str(err)) # will print something like "option -a not recognized"
			self.usage()
			sys.exit(1)

		for o, a in opts:
			if o in ("-v", "--verbose"):
			    self.verbose = True
			elif o in ("-h", "--host"):
				self.host_ip = a
			elif o in ("-p", "--port"):
				self.port_num = a
			elif o in ("-t", "--topic"):
				self.topic = a
			elif o in ("-q", "--qos"):
				self.qos = int(a)
			elif o in ("-T", "--thresh"):
				self.thresh = int(a)
			else:
			    assert False, "unhandled option"

		if self.host_ip == None:
			self.host_ip = "127.0.0.1"

		if self.port_num == None:
			self.port_num = 1883

	def quit(self):
		# client.loop_stop()
		self.is_stopped = True
		return


###########################################################################
if __name__ == "__main__":
	main = MyApp()
	main.start()
