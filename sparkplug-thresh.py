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
import ssl

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
      logging.basicConfig(level=logging.INFO, format=formatting, )

      logger.setLevel(logging.INFO)




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
		self.tagaliases = dict()

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

def debug_metric(msgtype, device, metricname, metric):
    if metric.datatype == MetricDataType.Int8 or metric.datatype == MetricDataType.Int16 or metric.datatype == MetricDataType.Int32:
        valuestr = str(metric.int_value)
    elif metric.datatype == MetricDataType.UInt8 or metric.datatype == MetricDataType.UInt16 or metric.datatype == MetricDataType.UInt32:
        valuestr = str(metric.int_value)
    elif metric.datatype == MetricDataType.Int64 or metric.datatype == MetricDataType.UInt64 or metric.datatype == MetricDataType.DateTime:
        valuestr = str(metric.long_value)
    elif metric.datatype == MetricDataType.Float:
        valuestr = str(metric.float_value)
    elif metric.datatype == MetricDataType.Double:
        valuestr = str(metric.double_value)
    elif metric.datatype == MetricDataType.Boolean:
        valuestr = str(metric.boolean_value)
    elif metric.datatype == MetricDataType.String or metric.datatype == MetricDataType.Text or metric.datatype == MetricDataType.UUID:
        valuestr = metric.string_value
    elif metric.datatype == MetricDataType.DataSet:
        valuestr = str(metric.dataset_value)
    else:
        valuestr = "UNKNOWN"
    logging.debug (msgtype + ' --> Device ' + device + ' metric ' + metricname + ' = ' + valuestr)


def _send_node_rebirth(client, groupid, eon):
    try:
        logging.debug ('sending NCMD rebirth...')
        command = sparkplug.getDdataPayload()
        addMetric(command, 'Node Control/Rebirth', 0, MetricDataType.Boolean, True)
        byteArray = bytearray(command.SerializeToString())
        client.publish('spBv1.0/' + groupid + '/NCMD/' + eon, byteArray, 0, False)
    except:
        # print help information and exit:
        logging.error ('some error')
        logging.error (str(err))
    return

def _type2str(typeval):
    if typeval < MetricDataType.Unknown:
        return "ILLEGAL"
    if typeval > MetricDataType.Template:
        return "ILLEGAL"

    typestrings = ['UNKNOWN', 'INT8', 'INT16', 'INT32', 'INT64', 'UINT8', 'UINT16', 'UINT32', 'UINT64', 'FLOAT', 'DOUBLE', 'BOOLEAN', 'STRING', 'DATETIME', 'TEXT', 'UUID', 'BYTES', 'FILE', 'TEMPLATE']
    return typestrings[typeval]


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
        main.messages_ignored += 1
        return

    if len(tokens) < 4:
        logging.debug ("ignored non-Sparkplug topic " + msg.topic)
        main.messages_ignored += 1
        return

    # TODO filter by groupid
    groupid = tokens[1]
    msgtype = tokens[2]
    eon = tokens[3]
    if eon not in main.eons:
        main.num_eons += 1
        neweon = Eon(main.num_eons, eon, now)
        main.eons[eon] = neweon
        if msgtype == 'NCMD':
            # nothing to do
            pass
        elif msgtype != 'NBIRTH':
            logging.debug (msgtype + ' --> Group ' + groupid + ' EON ' + eon + ' needs rebirth')
            main.num_eons_offline += 1
        else:
            logging.debug (msgtype + ' --> Group ' + groupid + ' EON ' + eon + ' online')

    else:
        if msgtype == 'NDEATH':
        	logging.debug (msgtype + ' --> Group ' + groupid + ' EON ' + eon + ' offline')
        	main.num_eons_offline += 1
        if msgtype == 'NBIRTH':
        	logging.debug (msgtype + ' --> Group ' + groupid + ' EON ' + eon + ' online')
        	if main.num_eons_offline > 0:
        		main.num_eons_offline -= 1

    if msgtype == 'DBIRTH' or msgtype == 'DDATA' or msgtype == 'DDEATH':
        device = eon + '/' + tokens[4]

        if device not in main.devices:
        	main.num_devices += 1
        	newdevice = Device(main.num_devices, device, now)
        	main.devices[device] = newdevice
        	if msgtype != 'DBIRTH':
        		logging.debug (msgtype + ' --> Group ' + groupid + ' EON ' + eon + ' Device ' + device + ' needs rebirth')
			# uncomment this line to send Node Control Rebirth
	                #_send_node_rebirth (client, groupid, eon)
        		main.num_devices_offline += 1
        	else:
        		logging.debug (msgtype + ' --> Group ' + groupid + ' EON ' + eon + ' Device ' + device + ' online')

        else:
        	if msgtype == 'DDEATH':
        		logging.debug (msgtype + ' --> Device ' + device + ' offline')
        		main.num_devices_offline += 1

        	if msgtype == 'DBIRTH':
        		if main.num_devices_offline > 0:
        			main.num_devices_offline -= 1

    # look at the payload only for NBIRTH, DBIRTH, DDATA, DDEATH and NDEATH
    # the rest ignored
    if msgtype != 'DDATA' and msgtype != 'DBIRTH' and msgtype != 'NBIRTH' and msgtype != 'DDEATH' and msgtype != 'NDEATH':
        # comment the following out if too many
        logging.debug ('topic ' + msg.topic + ' ignoring msg ' + msgtype)
        inboundPayload = sparkplug_b_pb2.Payload()

        try:
            inboundPayload.ParseFromString(msg.payload)
        except:
            # TODO keep stats on parse failures
            pass

        logging.debug (inboundPayload)
        main.messages_ignored += 1
        return

    # look at the payload only for NBIRTH, DBIRTH, DDATA, DDEATH and NDEATH
    inboundPayload = sparkplug_b_pb2.Payload()
	
    try:
        inboundPayload.ParseFromString(msg.payload)
    except:
        # TODO keep stats on parse failures
        pass

    # periodic statistics
    if main.last_time_received +10 <= now:
        main.last_time_received = now
        logging.info ('received ' + str (main.messages_received) + ' messages')
        logging.info ('ignored ' + str (main.messages_ignored) + ' messages')
        logging.info ('error ' + str (main.messages_error) + ' messages')

    # uncomment below to dump Sparkplug B payload
    #logging.debug (inboundPayload)

    report_sparkplug_metrics = False
    # uncomment below to dump Sparkplug B payload for SPARKPLUG_METRICS
    # if selectively listening on DBIRTH topic, this prints the necessary
    # metrics advertised in the DBIRTH for setting SPARKPLUG_METRICS in MIMIC
    if msgtype == 'DBIRTH':
        report_sparkplug_metrics = True
    if report_sparkplug_metrics:
        SPARKPLUG_METRICS = ''
        for metric in inboundPayload.metrics:
#            print (metric)
            if SPARKPLUG_METRICS != '':
                SPARKPLUG_METRICS = SPARKPLUG_METRICS + ' '
            SPARKPLUG_METRICS = SPARKPLUG_METRICS + '*,' + metric.name
            # have not been able to figure out how to query if alias is really
            # set except via str() method
            if str(metric).find('alias:') != -1:
                SPARKPLUG_METRICS = SPARKPLUG_METRICS + ':' + str(metric.alias)
            SPARKPLUG_METRICS = SPARKPLUG_METRICS + ','
            SPARKPLUG_METRICS = SPARKPLUG_METRICS + _type2str(metric.datatype)

        logging.debug ('SPARKPLUG_METRICS = "' + SPARKPLUG_METRICS + '"')

    eoninfo = main.eons[eon]
    if msgtype == 'DBIRTH':
        # map tag alias to tag name
        for metric in inboundPayload.metrics:
#            print ('checking for tag alias ' + str(metric))
            if str(metric).find('alias:') == -1:
                continue
            if metric.alias in eoninfo.tagaliases:
                if eoninfo.tagaliases[metric.alias] != metric.name:
                    logging.error ('tag alias ' + str(metric.alias) + ' redefined from ' + eoninfo.tagaliases[metric.alias])
            eoninfo.tagaliases[metric.alias] = metric.name
            logging.debug ('tag alias ' + str(metric.alias) + ' defined as ' + metric.name)

    # only consider telemetry in DDATA messages
    if msgtype != 'DDATA':
        main.messages_ignored += 1
        return

    for metric in inboundPayload.metrics:
        logging.debug ('metric' + str(metric))
        # map tag alias to tag name
        metricname = 'unknown tag'
        if metric.name != '':
            metricname = metric.name
        else:
            if metric.alias in eoninfo.tagaliases:
                metricname = eoninfo.tagaliases[metric.alias]
                logging.debug ('tag alias ' + str(metric.alias) + ' found as ' + metricname)

        debug_metric (msgtype, device, metricname, metric)
        tag = device + '/' + metricname
        if tag not in main.tags:
            main.num_tags += 1
            newtag = Tag(main.num_tags, tag, now)
            main.tags[tag] = newtag

        logging.debug ('Device ' + device + ' metric ' + metricname + ' = ' + str(metric.int_value))
        if metricname != main.metric:
            logging.debug ('Device ' + device + ' ignored metric ' + metricname)
            continue

        logging.debug ('Device ' + device + ' metric ' + metricname + ' = ' + str(metric.int_value))
        if metric.int_value > main.thresh:
            logging.error ('********* Device ' + device + ' metric '+ metricname + ' = ' + str(metric.int_value) + ' greater ' + str(main.thresh))
            main.total_triggered += 1
            if device not in main.triggered_set:
                 main.triggered_set.add(device)

    return


def on_disconnect(client, userdata, rc):

    if rc != 0:
    	logging.error ("unexpected disconnect: " + str(rc))

def subscriber_client(addr):
	client = mqtt.Client(main.client_id, protocol=main.protocol)
	client.on_connect = on_connect
	client.on_message = on_message
	client.on_disconnect = on_disconnect

	if (main.user != None):
		logging.debug ("user " + main.user)
		client.username_pw_set(main.user, main.pwd)

	if (main.is_tls):
#		logging.debug ("cafile " + main.cafile)
		client.tls_set(ca_certs=main.cafile, certfile=main.certfile, keyfile=main.keyfile, tls_version=ssl.PROTOCOL_SSLv23, cert_reqs=main.required)
		client.tls_insecure_set(True)

	logging.debug ("connecting to " + main.host_ip + ":" + str(main.port_num))
	client.connect(main.host_ip, main.port_num, 60)

	try:
		client.loop_forever(False)
	except KeyboardInterrupt as err:
		logging.debug ('KeyboardInterrupt')
		logging.error (str(err))


###########################################################################
class MyApp:
	def __init__(self):
		self.protocol = mqtt.MQTTv311
		self.host_ip = None
		self.port_num = None
		self.client_id = None
		self.user = None
		self.pwd = None
		self.is_tls = False
		self.cafile = ""
		self.certfile = None
		self.keyfile = None
		self.required = ssl.CERT_NONE
		self.verbose = False
		self.topic = 'spBv1.0/#'
		self.qos = 0
		self.metric = "XDK/temp"
		self.thresh = 70000

		self.is_stopped = False
		self.is_paused = False

		self.messages_received = 0
		self.messages_ignored = 0
		self.messages_error = 0
		self.last_received = 0
		self.last_time_received = 0
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

		self.num_tagaliases = 0
		self.tagaliases = dict()

		self.total_triggered = 0
		self.triggered_set = set()

	def usage(self):
		print ("Usage: sparkplug-thresh.py")
		print ("\t[-h|--host host]        broker to connect to; default localhost")
		print ("\t[-p|--port port]        port to connect to; default port 1883")
		print ("\t[-i|--id client-id]     client ID; default ")
		print ("\t[-u|--user user]        user name; default blank")
		print ("\t[-P|--pass password]    password; default blank")
		print ("\t[-T|--tls]              TLS; default off")
		print ("\t[-c|--cafile filepath]  CA file; default none")
		print ("\t[-C|--certfile filepath] certificate file; default none")
		print ("\t[-K|--keyfile filepath] key file; default none")
		print ("\t[-t|--topic topic]      topic; default spBv1.0/#")
		print ("\t[-q|--qos qos]          QoS; default 0")
		print ("\t[-T|--metric metric]    metric to monitor; default XDK/temp")
		print ("\t[-T|--thresh threshold] temperature threshold; default 70000")
		print ("\t[-v|--verbose]    verbose output")
		return

	def start(self):
		self.command_line()

		config_logger ()

		subscriber_client (self.host_ip)
		logging.debug('after subscriber_client')

	###############################
	def command_line(self):
		try:
			opts, args = getopt.getopt(sys.argv[1:], "h:p::i:u:P:Tc:C:K:t:q:M:T:v", ["host=", "port=", "id=", "user=", "pass=", "tls", "cafile=", "certfile=", "keyfile=", "topic=", "qos=", "metric=", "thresh=", "verbose"])
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
				self.port_num = int(a)
			elif o in ("-i", "--id"):
				self.client_id = a
			elif o in ("-u", "--user"):
				self.user = a
			elif o in ("-P", "--pass"):
				self.pwd = a
			elif o in ("-T", "--tls"):
				self.is_tls = True
			elif o in ("-c", "--cafile"):
				self.cafile = a
			elif o in ("-C", "--certfile"):
				self.certfile = a
			elif o in ("-K", "--keyfile"):
				self.keyfile = a
			elif o in ("-t", "--topic"):
				self.topic = a
			elif o in ("-q", "--qos"):
				self.qos = int(a)
			elif o in ("-M", "--metric"):
				self.metric = a
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
