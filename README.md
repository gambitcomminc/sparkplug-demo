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

Tested on both Linux and Windows.

## Usage

For sparkplug-thresh:
```
% python3 sparkplug-thresh.py -h broker.gambitcom.com -v
DEBUG 2023-08-31 10:03:56,466 (sparkplug-thresh) - connecting to broker.gambitcom.com:1883
DEBUG 2023-08-31 10:03:56,489 (sparkplug-thresh) - MQTT client connected with result code 0
DEBUG 2023-08-31 10:03:56,930 (sparkplug-thresh) - DDATA --> Group MIMIC EON 20:19:AB:F4:0E:20 needs rebirth
DEBUG 2023-08-31 10:03:56,930 (sparkplug-thresh) - DDATA --> Group MIMIC EON 20:19:AB:F4:0E:20 Device 20:19:AB:F4:0E:20/sensor111 needs rebirth
INFO 2023-08-31 10:03:56,930 (sparkplug-thresh) - received 1 messages
INFO 2023-08-31 10:03:56,930 (sparkplug-thresh) - ignored 0 messages
INFO 2023-08-31 10:03:56,930 (sparkplug-thresh) - error 0 messages
DEBUG 2023-08-31 10:03:56,930 (sparkplug-thresh) - metricname: "XDK/temp"
datatype: 3
int_value: 10008
```

and on Windows:

![screenshot](https://github.com/gambitcomminc/sparkplug-demo/blob/master/mqtt_sparkplug_thresh.png)

See also this Youtube video https://www.youtube.com/watch?v=8bnMOPzntAM with 50k tags
or this one https://www.youtube.com/watch?v=5299EYSbW8M with 200k.

