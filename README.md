# sparkplug-demo / sparkplug-thresh

## Overview

Demo of MQTT Sparkplug client to monitor sensors exceeding a threshold 

This pair of MQTT subscriber clients is specifically designed to monitor the topic
namespace of the Sparkplug protocol on top of MQTT detailed at

https://www.cirrus-link.com/mqtt-sparkplug-tahu/

The sparkplug-thresh client is stdout based, whereas sparkplug-demo requires
the graphics package GTK.

sparkplug-thresh with -v option (debug mode) displays the received Sparkplug messages.

sparkplug-demo also counts Topics, EON nodes, devices and tags as defined in the Sparkplug
spec.

It also maintains statistics for the last second on Active Topics, messages / sec,
etc.

When the "XDK/temp" metric exported by any device exceeds the threshold (default
of 70000), then it displays triggered devices and tags in red, and lists the value
in the list.

## Installation / Requirements

Requires:

- Python 2.7
- Eclipse Paho Python https://www.eclipse.org/paho/clients/python/docs/
- Sparkplug Python https://github.com/Cirrus-Link/Sparkplug

sparkplug-demo also requires:

- GTK https://python-gtk-3-tutorial.readthedocs.io/en/latest/


## Usage

For sparkplug-thresh:
```
% python sparkplug-thresh.py -h iot.eclipse.org -v
DEBUG 2019-04-12 13:56:38,381 (sparkplug-thresh) - MQTT client connected with result code 0
DEBUG 2019-04-12 13:56:40,884 (sparkplug-thresh) - DDATA --> EON 20:19:AB:F4:0E:10 needs rebirth
DEBUG 2019-04-12 13:56:40,884 (sparkplug-thresh) - DDATA --> Device 20:19:AB:F4:0E:10/sensor11 needs rebirth
DEBUG 2019-04-12 13:56:40,884 (sparkplug-thresh) - DDATA --> Device 20:19:AB:F4:0E:10/sensor11 metric XDK/temp = 50000
```

and on Windows:

![screenshot](https://github.com/gambitcomminc/sparkplug-demo/blob/master/mqtt_sparkplug_thresh.png)

See also this Youtube video https://www.youtube.com/watch?v=8bnMOPzntAM with 50k tags
or this one https://www.youtube.com/watch?v=5299EYSbW8M with 200k.

