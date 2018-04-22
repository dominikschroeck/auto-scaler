#!/usr/bin/env python

import sys
import os
import time
import subprocess
from threading import Thread


def proct(pid):
    try:
        with open(os.path.join('/proc/', pid, 'stat'), 'r') as pidfile:
            proctimes = pidfile.readline()
            # get utime from /proc/<pid>/stat, 14 item
            utime = proctimes.split(' ')[13]
            # get stime from proc/<pid>/stat, 15 item
            stime = proctimes.split(' ')[14]
            # count total process used time
            proctotal = int(utime) + int(stime)
            return(float(proctotal))
    except IOError as e:
        print('ERROR: %s' % e)
        sys.exit(2)


def cput():
    try:
        with open('/proc/stat', 'r') as procfile:
            cputimes = procfile.readline()
            cputotal = 0
            # count from /proc/stat: user, nice, system, idle, iowait, irc, softirq, steal, guest
            for i in cputimes.split(' ')[2:]:
                i = int(i)
                cputotal = (cputotal + i)
            return(float(cputotal))
    except IOError as e:
        print('ERROR: %s' % e)
        sys.exit(3)

# assign start values before loop them
def monitor_pid(pid,name):
    proctotal = proct(pid)
    cputotal = cput()
    graphitesend.init(graphite_server="dominikschroeck.de", group="cpu")

    try:
        while True:
            pr_proctotal = proctotal
            pr_cputotal = cputotal

            proctotal = proct(pid)
            cputotal = cput()

            try:
                res = ((proctotal - pr_proctotal) / (cputotal - pr_cputotal) * 100)
                print(str(round(res, 3)) + " %")
                res = res /100
                metrics = [("." + name + "-" + pid,round(res, 3), time.time())]

                graphitesend.send_list(metrics)
            except ZeroDivisionError:
                pass

            time.sleep(1)
    except KeyboardInterrupt:
        sys.exit(0)


def main(pid,name):
    Thread(target=monitor_pid, args=(pid,name,)).start()
    #get_network_bytes('wlp1s0')
    #print('%i bytes received' % rx_bytes)
    #print('%i bytes sent' % tx_bytes)


if len(sys.argv) == 3:
    pid = sys.argv[1]
    name = sys.argv[2]
else:
    print('No PID specified. Usage: %s <PID> <NAME>' % os.path.basename(__file__))
    sys.exit(1)

device = sys.argv[1]
print("Monitoring process: " + pid)

main(pid,name)
