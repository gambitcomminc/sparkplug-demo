# sparkplug-demo

## Overview

Demo of MQTT Sparkplug client to monitor sensors exceeding a threshold 

This MQTT subscriber client is specifically designed to monitor the topic
namespace of the Sparkplug protocol on top of MQTT detailed at

https://www.cirrus-link.com/mqtt-sparkplug-tahu/

It counts Topics, EON nodes, devices and tags as defined in the Sparkplug
spec.

It also maintains statistics for the last second on Active Topics, messages / sec,
etc.

When the "XDK/temp" metric exported by any device exceeds the threshold (default
of 70000), then it displays triggered devices and tags in red, and lists the value
in the list.

## Installation / Requirements

Requires:

- Eclipse Paho Python https://www.eclipse.org/paho/clients/python/docs/
- GTK https://python-gtk-3-tutorial.readthedocs.io/en/latest/
- Sparkplug Python https://github.com/Cirrus-Link/Sparkplug

## Usage

See this Youtube video https://www.youtube.com/watch?v=8bnMOPzntAM with 50k tags
or this one https://www.youtube.com/watch?v=5299EYSbW8M with 200k.

