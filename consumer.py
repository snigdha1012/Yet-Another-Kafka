# import socket
import socket
fp = open('log_file.log', 'a+')
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(("127.0.0.1", 7777))
while True :
    topic_name = input('Enter topic name to subscribe to: ')
    requestdata = 'subscriber|{}'.format(topic_name)
    sock.sendall(requestdata.encode("ascii"))
    response = sock.recv(10240)
    print(response.decode("ascii"))
    fp.write('Subscribed message from topic: ' + topic_name + ':' + str(response) + '\n')
fp.close()
    

