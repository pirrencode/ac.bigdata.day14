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
#while 1:
clientsocket, address = serversocket.accept()
print ("connection established!")
# Send our string of data, TODO: move to read input from console.

message1 = b"6\n\
4\n\
5\n\
2\n\
3\n\
2\n\
1\n\
4\n\
3\n\
1\n"

message2 = "2\n\
5\n\
3\n\
1\n\
3\n\
2\n\
3\n\
4\n\
2\n\
3\n"

message3 = "5\n\
2\n\
3\n\
4\n\
4\n\
2\n\
3\n\
1\n\
2\n\
2\n"

clientsocket.send(message1)
print ("1st sent!") 
time.sleep(10)
clientsocket.send(message2)
print ("2nd sent!") 
time.sleep(10)
clientsocket.send(message3) 
print ("3rd sent!") 

clientsocket.close()
# Wait 4 seconds before sending again
time.sleep(4)
# We dont need client for this task
# client(clientsocket, address)
