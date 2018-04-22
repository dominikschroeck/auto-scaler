import socket
import socks# Import socket module
import os


def send_conf(filename="out.yaml",scaleout=False):
    os.system("cp out.yaml /home/flink/webspace/out.yaml")

# def send_conf(host=socket.gethostname(),port=9999,filename="out.yaml",scaleout=False):
#     SOCKS5_PROXY_HOST = 'localhost'
#     SOCKS5_PROXY_PORT = 8080
#
#     # Create a socks object to tunnel through and be able to access power-1
#     socks.set_default_proxy(socks.SOCKS5, SOCKS5_PROXY_HOST, SOCKS5_PROXY_PORT)
#     socket.socket = socks.socksocket
#     s = socket.socket()
#
#
#
#     s.connect((host, port))
#     if scaleout: s.send(b"out")
#     else: s.send(b"eup")
#
#     f = open(filename, 'rb')
#     l = f.read(1024)
#     while (l):
#         s.send(l)
#         print('Sent ', repr(l))
#         l = f.read(1024)
#     f.close()
#
#     s.close()
#     print('connection closed')

#send_conf(host="dominikschroeck.de")
