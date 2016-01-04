#!/bin/bash
ip="$(ifconfig | grep -A 1 'eth0' | tail -1 | cut -d ':' -f 2 | cut -d ' ' -f 1)"
a="$(ifconfig | grep -A 1 'eth0' | tail -1 | cut -d ':' -f 2 | cut -d ' ' -f 1 | cut -d '.' -f 1)"
b="$(ifconfig | grep -A 1 'eth0' | tail -1 | cut -d ':' -f 2 | cut -d ' ' -f 1 | cut -d '.' -f 2)"
c="$(ifconfig | grep -A 1 'eth0' | tail -1 | cut -d ':' -f 2 | cut -d ' ' -f 1 | cut -d '.' -f 3)"
d="$(ifconfig | grep -A 1 'eth0' | tail -1 | cut -d ':' -f 2 | cut -d ' ' -f 1 | cut -d '.' -f 4)"
echo "$a.$b.$c.$(($d+1)) bigboost-sparkWorker3" >> /etc/hosts
echo "$a.$b.$c.$(($d+2)) bigboost-sparkWorker2" >> /etc/hosts
echo "$a.$b.$c.$(($d+3)) bigboost-sparkWorker" >> /etc/hosts