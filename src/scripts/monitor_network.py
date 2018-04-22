from threading import Thread
import time
import sys
import graphitesend

device = sys.argv[1]
print("Monitoring network device: " + device)

def main():
    Thread(target=sendNetwork, args=(device,)).start()
    #get_network_bytes('wlp1s0')
    #print('%i bytes received' % rx_bytes)
    #print('%i bytes sent' % tx_bytes)

def sendNetwork(device):
    graphitesend.init(graphite_server="dominikschroeck.de",group="network")
    i = 0
    rx_observations = []
    tx_observations = []
    while True:
        rx_rate = 0
        tx_rate = 0


        rx_bytes , tx_bytes = get_network_bytes(device)

        rx_observations.append(rx_bytes)
        i = i + 1
	# Pointers, not values! Hence, this complicated way
        if len(rx_observations)>1:
            rx_rate = rx_observations[len(rx_observations)-1] - rx_observations[len(rx_observations)-2]

        tx_observations.append(tx_bytes)
        i = i + 1
        if len(tx_observations) > 1:
            tx_rate = tx_observations[len(tx_observations) - 1] - tx_observations[len(tx_observations) - 2]

        metrics = [(".tx_rate",tx_rate, time.time()),(".rx_rate",rx_rate, time.time()),(".sum_tx_rx",rx_rate + tx_rate, time.time())]
	#print(metrics)
        graphitesend.send_list(metrics)
        time.sleep(1)


def get_network_bytes(interface):
        for line in open('/proc/net/dev', 'r'):
            if interface in line:
                data = line.split('%s:' % interface)[1].split()
                rx_bytes, tx_bytes = (data[0], data[8])
        return int(rx_bytes), int(tx_bytes)

main()
