#heartbeats
import socket
import sys
from json import loads
from _thread import *
BUF_SIZE = 4096
HOST = '0.0.0.0'
PORT = 7770
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind((HOST, PORT))
print("Socket binding is complete")
server.listen(0)
print("Socket is now listening")

def clientthread(sock1):
    sock1.send(("Server started").encode('utf-8'))
    while True:
        try:
            data = sock1.recv(BUF_SIZE)
            data_loaded = loads(data)
            data_loaded = data_loaded
            print("ip: " + str(data_loaded['ip']) + ", pid: " + str(data_loaded['pid']) + ", status: " + data_loaded['status'])
        except socket.error:
            print("One Client (IP: %s) Connection over!" % data_loaded['ip'])
            break
    sock1.close()

while True:
    sock1, addr = server.accept()
    print("Connected with %s: %s " % (addr[0], str(addr[1])))
    start_new_thread(clientthread, (sock1,))

server.close()
