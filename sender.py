import socket
from threading import *
import time

serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
host = "127.0.0.1"
port = 33001
print (host)
print (port)
serversocket.bind((host, port))

class client(Thread):
    def __init__(self, socket, address):
        Thread.__init__(self)
        self.sock = socket
        self.addr = address
        self.start()

    def run(self):
#        while 1:
         print('Client sent:', self.sock.recv(1024).decode())
         self.sock.send('You sent something to me')

serversocket.listen(5)
print ('server started and listening')
#Lets send it infinetly for now
while 1:
    clientsocket, address = serversocket.accept()
    print ("connection established!")
    # Send our string of data, TODO: move to read input from console.
    message = b"172.32.11.121 736443 /api/2.3\n\
98.134.54.184 182294 /api/2.3\n\
172.32.11.121 736443 /api/2.3/security\n\
203.44.122.18 397832 /api/3.0\n\
172.32.11.121 736443 /api/2.3\n\
98.134.54.184 182294 /api/3.0\n\
172.32.11.121 736443 /api/2.3/logging\n\
172.32.11.121 736443 /api/2.3\n\
71.11.139.205 291187 /api/2.3\n\
172.32.11.121 736443 /api/3.0/security\n\
98.134.54.184 182294 /api/2.3\n\
71.11.139.205 397832 /api/3.0/logging\n"
    clientsocket.send(message)    
    clientsocket.close()
    # Wait 4 seconds before sending again
    time.sleep(4)
    # We dont need client for this task
    # client(clientsocket, address)
