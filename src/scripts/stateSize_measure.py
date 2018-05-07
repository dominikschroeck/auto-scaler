import graphitesend
import os
import time

def monitor():
    graphitesend.init(graphite_server="dominikschroeck.de", group="stateSize")
    while True:
        stateSize = ""
        output = os.popen("/share/hadoop/stable/hadoop-2.7.1/bin/hadoop fs -du -s /user/dschroeck/chk").read()

        for i in range(len(output) - 1):
            if output[i] != "/":
                stateSize += output[i]
            else:
                break

        metrics = [(".stateSize",int(stateSize), time.time())]
        graphitesend.send_list(metrics)

        time.sleep(10)


monitor()
