import socket
import threading
import os
import signal
import json
import sys
import time

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind(("0.0.0.0", 7777))
sock.listen(10000)
list_publisher = []

# hostname of zookeeper
host = "0.0.0.0"  
port = 7770

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect((host, port))
client.setblocking(0)  # set the socket to not blocking

class Publisher:
    def __init__(self, topic, conn):
        self.Topic = topic
        self.connection = conn
        self.data_list = []

    def publish(self):
        if len(self.data_list) == 0:
            return 'Publisher has not sent any data'
        else:
            data = ''
            i = 0
            for x in self.data_list:
                i = i + 1
                data += '{}\n'.format(x)
            return data


def ascii_decoder(encoded_data):
    return encoded_data.decode('ascii')


def ascii_encoder(decoded_data):
    return decoded_data.encode('ascii')


def client_type(request_string):
    type_client = request_string.split('|')
    if type_client[0] == 'publisher':
        return True
    else:
        return False


def create_publisher(connection, topic):
    return Publisher(topic,connection)


def is_topic_exist(topic, publisher_list):
    exist = False
    for x in publisher_list:
        if x.Topic == topic:
            exist = True
    return exist


def find_topic(topic, publisher_list):
    obj = {}
    for x in publisher_list:
        #print(x.Topic)
        if x.Topic == topic:
            obj = x
    return obj

def create_topic(topic_name):	 #creating a directory for each topic that is already not present

	dirname = topic_name
	os.mkdir('./'+ dirname, mode = 0o777)
	f1 = open('./'+ dirname+'/'+'P1.txt', "w")   #creating 3 partitions in each directory
	f2 = open('./'+ dirname+'/'+'P2.txt', "w")
	f3 = open('./'+ dirname+'/'+'P3.txt', "w")
 
def partition(topic_name, message):
	entries = os.listdir('./'+topic_name+'/')
	for entry in entries:
		f1 = open('./'+topic_name+'/'+entry, "a+")    #writing into each partition till the partition writes 10240 lines
		if len(f1.readlines()) <= 10240:
			f1.write(message)
			break


def handler(connection):
    while True:
        try:
            data = connection.recv(10240)
            decoded_data = ascii_decoder(data)
            if client_type(decoded_data):   #publisher data
                request_data = decoded_data.split('|')
                if request_data[1] == '__init__':  #'publisher|__init__|{}'.format(topic_name) : if request_data[1] == __init__ it creates new topic if it doesn't exist
                    if is_topic_exist(request_data[2], list_publisher):
                        connection.send(ascii_encoder('False'))
                    else:
                        list_publisher.append(create_publisher(connection, request_data[2]))
                        create_topic(request_data[2])
                        connection.send(ascii_encoder('Data for subscriber can be published'))
                else:
                    publisher = find_topic(request_data[1],list_publisher)
                    publisher.data_list.append(request_data[2])
                    connection.send(ascii_encoder('Ack'))

            else:  #subscriber data
                request_data = decoded_data.split('|')
                if is_topic_exist(request_data[1], list_publisher):
                    publisher = find_topic(request_data[1], list_publisher)
                    response = publisher.publish()
                    partition(request_data[1],response)
                    connection.send(ascii_encoder(response))
                    connection.send(ascii_encoder('Recv'))
                else:
                    response = 'Topic not found'
                    connection.send(ascii_encoder(response))

        except socket.error:
            connection.close()
            break


def main():
    while True:
        connection, client_address = sock.accept()
        thread = threading.Thread(target = handler, args = (connection,))
        thread.start()
        host_name = socket.gethostname()
        data_to_server = {'ip': socket.gethostbyname(host_name), 'status': 'alive', 'pid': os.getpid()}
        data_dumped = json.dumps(data_to_server)
        try:
        	client.sendall(data_dumped.encode('utf-8'))
        except socket.error:
        	print("Send failed")
        	sys.exit()
main()
