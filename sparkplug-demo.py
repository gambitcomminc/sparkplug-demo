#!/usr/bin/env python

##############################################################################
## Copyright (c) 2018 by Gambit Communications, Inc.
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

import gi
gi.require_version('Gtk', '3.0')

try:
	from gi.repository import Gtk
	from gi.repository import GObject
	from gi.repository import Pango as pango
except:
	logging.error ("require Gtk")
	sys.exit(1)

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
class _IdleObject(GObject.GObject):
    """
    Override GObject.GObject to always emit signals in the main thread
    by emmitting on an idle handler
    """

    @trace
    def __init__(self):
        GObject.GObject.__init__(self)

    @trace
    def emit(self, *args):
        GObject.idle_add(GObject.GObject.emit, self, *args)


###########################################################################
class _UpdateThread(threading.Thread, _IdleObject):
	"""
	Cancellable thread which uses gobject signals to return information
	to the GUI.
	"""
	__gsignals__ = {
		"completed": (
		    GObject.SignalFlags.RUN_LAST, None, []),
		"progress": (
		    GObject.SignalFlags.RUN_LAST, None, [
		        GObject.TYPE_FLOAT])  # percent complete
	}

	@trace
	def __init__(self, parent):
		threading.Thread.__init__(self, target=self.update_main, name="Update Thread")
		_IdleObject.__init__(self)
		self.cancelled = False

	# main thread for the update thread
	# this thread periodically checks for any changes
	# and initiates updates
	@trace
	def update_main(self):
		while True:
			# wake up every second for response
			time.sleep (0.999)

			if main.is_stopped:
				break

			if main.is_paused:
				continue

			# but only do the work every N seconds
#			count += 1
#			if count < self.poller.poll_interval:
#				continue
#			count = 0

			logging.debug ("update_cycle start")
			self.update_cycle()
			logging.debug ("update_cycle completed")
			self.emit("completed")

		# logging.debug ("done update_main")

	# run a poll cycle
	@trace
	def update_cycle(self):
		# logging.debug ("done update_cycle " )
		return


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
		logging.debug ('EON ' + eon + ' needs rebirth')
		main.num_eons_offline += 1

    else:
	if msgtype == 'NDEATH':
		logging.debug ('EON ' + eon + ' offline')
		main.num_eons_offline += 1
	if msgtype == 'NBIRTH':
		if main.num_eons_offline > 0:
			main.num_eons_offline -= 1

    if msgtype == 'DBIRTH' or msgtype == 'DDATA' or msgtype == 'DDEATH':
	device = eon + '/' + tokens[4]

	if device not in main.devices:
		main.num_devices += 1
		newdevice = Device(main.num_devices, device, now)
		main.devices[device] = newdevice
		if msgtype != 'DBIRTH':
			logging.debug ('Device ' + device + ' needs rebirth')
			main.num_devices_offline += 1

	else:
		if msgtype == 'DDEATH':
			logging.debug ('Device ' + device + ' offline')
			main.num_devices_offline += 1

		if msgtype == 'DBIRTH':
			if main.num_devices_offline > 0:
				main.num_devices_offline -= 1

	# look at the payload only for DDATA
	if msgtype != 'DDATA':
		return

	inboundPayload = sparkplug_b_pb2.Payload()
	try:
		inboundPayload.ParseFromString(msg.payload)
	except:
		pass
	for metric in inboundPayload.metrics:
		logging.debug ('Device ' + device + ' metric ' + metric.name + ' = ' + str(metric.int_value))
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
			logging.debug ('********* greater ' + str(main.thresh))
			timestamp = time.ctime (time.time())
			rowref = main.trigstore.append(
					[device,
					str(metric.int_value),
					timestamp,
					'red'
					])
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

	client.loop_start()


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
		print ("Usage: sparkplug-demo.py")
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

		self.show_gui()
		# from now on GUI is expected to be up

		subscriber_client (self.host_ip)

		self.update_thread = _UpdateThread(self)
		self.update_thread.connect("completed", self.completed_cb)
		self.update_thread.start()

		Gtk.main()

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

	###############################
	def show_gui(self):
		self.builder = Gtk.Builder()
		dir_path = os.path.dirname(os.path.realpath(__file__))
		glade_path = dir_path+'/sparkplug-demo.glade'
		self.builder.add_from_file(glade_path)
		self.builder.connect_signals(Handler())

		self.window = self.builder.get_object('mainWindow')

		# dialogs
		# Help->About
		self.aboutdialog = self.builder.get_object('aboutdialog1')

		# File->New

		self.errordialog = self.builder.get_object('errordialog')

		# status bar
		self.statusbar = self.builder.get_object('statusmessage')
		self.context_id = self.statusbar.get_context_id('status')
		self.clients = self.builder.get_object('clients')
		self.clients_context = self.statusbar.get_context_id('clients')
		self.freevm = self.builder.get_object('freevm')
		self.freevm_context = self.statusbar.get_context_id('freevm')
		self.activity_meter = self.builder.get_object('activitymeter')

		# the first titlelabel
		self.titlelabel = self.builder.get_object('titlelabel')
		self.titlelabel.set_text('Sparkplug Demo')

		self.infolabel1 = self.builder.get_object('infolabel1')
		self.infolabel1.set_text('MQTT Broker: ' + self.host_ip + '\nStarted: ' + time.ctime (time.time()))

		self.infolabel2 = self.builder.get_object('infolabel2')
		self.infolabel2.set_text('Subscribed Topic: ' + main.topic)

		self.dynlabel3 = self.builder.get_object('dynlabel3')
		self.dynlabel3.set_text('Messages received: ')

		self.dynlabel4 = self.builder.get_object('dynlabel4')
		self.dynlabel4.set_text('Messages / sec: ')

		self.dynlabel5 = self.builder.get_object('dynlabel5')
		self.dynlabel5.set_text('')

		self.dynlabel6 = self.builder.get_object('dynlabel6')
		self.dynlabel6.set_text('Topics: ')

		self.dynlabel7 = self.builder.get_object('dynlabel7')
		self.dynlabel7.set_text('Active topics: ')

		self.dynlabel8 = self.builder.get_object('dynlabel8')
		self.dynlabel8.set_text('')

		self.dynlabel9 = self.builder.get_object('dynlabel9')
		self.dynlabel9.set_text('EON nodes: ')

		self.dynlabel10 = self.builder.get_object('dynlabel10')
		self.dynlabel10.set_text('offline: ')

		self.dynlabel11 = self.builder.get_object('dynlabel11')
		self.dynlabel11.set_text('')

		self.dynlabel12 = self.builder.get_object('dynlabel12')
		self.dynlabel12.set_text('Devices: ')

		self.dynlabel13 = self.builder.get_object('dynlabel13')
		self.dynlabel13.set_text('')

		self.dynlabel14 = self.builder.get_object('dynlabel14')
		self.dynlabel14.set_text('')

		self.dynlabel15 = self.builder.get_object('dynlabel15')
		self.dynlabel15.set_text('')

		self.dynlabel16 = self.builder.get_object('dynlabel16')
		self.dynlabel16.set_text('')

		self.dynlabel17 = self.builder.get_object('dynlabel17')
		self.dynlabel17.set_text('')

		# the liststore containing the agents
		self.trigstore = self.builder.get_object("trigstore")

		treeview = Gtk.TreeView(self.trigstore)
		self.treeview = treeview
		tvcolumn = Gtk.TreeViewColumn('Device')
		treeview.append_column(tvcolumn)
		cell = Gtk.CellRendererText()
		tvcolumn.pack_start(cell, True)
		tvcolumn.add_attribute(cell, 'text', 0)
		tvcolumn.add_attribute(cell, "foreground", 3)
		tvcolumn.set_sort_column_id(0)

		tvcolumn = Gtk.TreeViewColumn('Temperature')
		treeview.append_column(tvcolumn)
		tvcolumn.pack_start(cell, True)
		tvcolumn.add_attribute(cell, 'text', 1)
		tvcolumn.set_sort_column_id(1)

		tvcolumn = Gtk.TreeViewColumn('Time')
		treeview.append_column(tvcolumn)
		tvcolumn.pack_start(cell, True)
		tvcolumn.add_attribute(cell, 'text', 2)
		tvcolumn.set_sort_column_id(1)

		box = Gtk.VBox(False, 6)
		scrolledwindow = self.builder.get_object('scrolledwindow1')
		scrolledwindow.set_policy(
		            Gtk.PolicyType.AUTOMATIC,
			    Gtk.PolicyType.AUTOMATIC)
		scrolledwindow.add(box)
		box.pack_start (treeview, True, True, 0)

		self.window.show_all()

	###############################
	# GUI component, could be separated out into a GUI class
	# the callback on every poller cycle
	@trace
	def completed_cb(self, thread):
		if main.clear_store:
			main.trigstore.clear()
			main.clear_store = False

		msgpersec = self.messages_received - self.last_received
		self.last_received = self.messages_received
		main.dynlabel3.set_markup('<span foreground="green">Messages received: ' + str(main.messages_received) + '</span>')
		main.dynlabel4.set_markup('<span foreground="green">Messages / sec: ' + str(msgpersec) + '</span>')
		num_triggered = len (main.triggered_set)
		if num_triggered > 0:
			main.dynlabel5.set_markup('<span foreground="red">Triggered: ' +
				str(main.total_triggered) +
				'</span>')
			main.dynlabel14.set_markup('<span foreground="red">Triggered: ' +
				str(len(main.triggered_set)) +
				'</span>')
		main.dynlabel6.set_markup('<span foreground="green">Topics: ' + str(len(main.topics)) + '</span>')

		# run through the topics and add to the matrix
		active_topics = 0
		keys = main.topics.keys()
		for key in keys:
		    topic =  main.topics[key]

		    if topic.rowref == None:

			# UTF-8 encodings
			try:
				payloadstr = topic.last_payload.decode('utf-8')
				#    logging.debug ("payload is UTF-8 " + payloadstr)
			except UnicodeError:
				payloadstr = "0x" + binascii.hexlify (topic.last_payload)
				# logging.debug ("UnicodeError: payload is not UTF-8 " + topic.last_payload + " >> " + payloadstr)

			try:
			    topicstr = topic.topic.decode('utf-8')
			#    logging.debug ("topic is UTF-8 " + topicstr)
			except UnicodeError:
			    topicstr = topic.topic
			    # UWE: still get this error
			    # Pango-WARNING **: Invalid UTF-8 string passed to pango_layout_set_text()
			    # but we don't want the hex string for topic as we
			    # do for payload
			    # happens much less frequently for topic

			# TODO calculate msgpersec
			msgpersec = topic.count - topic.last_count

			topic.rowref = "hello"
			active_topics += 1
		    else:
		    	# redisplay only if new messages for topic
#			displayedcount = main.topicstore.get_value (topic.rowref, 2)
			# TODO calculate msgpersec
			msgpersec = topic.count - topic.last_count
			if topic.count != topic.last_count:
				active_topics += 1

			topic.last_count = topic.count;

		main.dynlabel7.set_markup('<span foreground="green">Active topics: ' + str(active_topics) + '</span>')
		main.dynlabel9.set_markup('<span foreground="green">EON nodes: ' + str(main.num_eons) + '</span>')
		main.dynlabel10.set_markup('<span foreground="green">offline: ' + str(main.num_eons_offline) + '</span>')
		main.dynlabel12.set_markup('<span foreground="green">Devices: ' + str(main.num_devices) + '</span>')
		main.dynlabel15.set_markup('<span foreground="green">Tags: ' + str(main.num_tags) + '</span>')

	def dump(self):
		outfile = open ("dump.lst", "w+")
		print ("Number Topic                Messages Bytes Last payload", file=outfile)
		keys = self.topics.keys()
		for key in keys:
		    topic =  self.topics[key]

		    topic.dump(outfile)

		outfile.close()
		print ("dumped to dump.lst")
		return

	def quit(self):
		# client.loop_stop()
		self.is_stopped = True
		return


###########################################################################
class Handler:
	def on_mainWindow_delete_event(self, *args):
		Gtk.main_quit(*args)
		main.quit()
	
	def on_gtk_quit_activate(self, menuitem, data=None):
		Gtk.main_quit()
		main.quit()

	# File->New menu handler
	def on_gtk_filenew_activate(self, menuitem, data=None):
		# clear everything
		main.dynlabel3.set_text('')
		main.dynlabel4.set_text('')
		main.dynlabel5.set_text('')
		main.dynlabel6.set_text('')
		main.dynlabel7.set_text('')
		main.dynlabel8.set_text('')
		main.dynlabel9.set_text('')
		main.dynlabel10.set_text('')
		main.dynlabel11.set_text('')
		main.dynlabel12.set_text('')
		main.dynlabel13.set_text('')
		main.dynlabel14.set_text('')
		main.dynlabel15.set_text('')
		main.dynlabel16.set_text('')
		main.dynlabel17.set_text('')

		main.clear_stats = True

	# File->Save menu handler
	def on_gtk_filesave_activate(self, menuitem, data=None):
		main.dump()

	# Help->About menu handler
	def on_gtk_about_activate(self, menuitem, data=None):
		self.response = main.aboutdialog.run()
		main.aboutdialog.hide()

###########################################################################
if __name__ == "__main__":
	GObject.threads_init()

	main = MyApp()
	main.start()
