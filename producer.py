import socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(("127.0.0.1", 7777))
topic = ''
fp = open('log_file.log', 'a+')

def init_publisher():
    global topic
    topic_name = input('Input topic name : ')
    requestdata = 'publisher|__init__|{}'.format(topic_name)
    sock.sendall(requestdata.encode("ascii"))
    response = sock.recv(10240)
    topic = topic_name
    return response.decode("ascii")

def send_data():
    data = input('Send data to subscriber: ')
    request = "publisher|{}|{}".format(str(topic), str(data))
    sock.sendall(request.encode("ascii"))
    response = sock.recv(10240)
    print(response.decode("ascii"))
    fp.write('Published message to topic: ' + topic + '\n')
    choice = input("Do you want to add more data? y/n ")
    if choice == 'y' or choice =="Y":
        return True
    elif choice == 'n' or choice == "N":
        print('Publishing done!')
        return False
    else:
        print('Provide only with y or n')
        return False

def main():
    if init_publisher() != 'False':
        while True:
            if not send_data():
                break
    else:
        print('Topic already exist')
        main()
      
main()
fp.close()  
