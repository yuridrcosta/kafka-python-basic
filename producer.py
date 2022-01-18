from time import sleep
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
from getpass import getpass
bootstrap_servers=['localhost:9092'] # default: localhost:9092

# Remember: topics must be created using kafka-topics.sh script
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
# Create 3 topics: 'user-access','system-management' and 'sales'


def login():
    username = input("Please, type your username: ")
    message =  f'{username} logged in'
    print(message)
    return username,message

def restartSystem(username):
    message = f'{username} restarted the system'
    print(message)
    return message

def registerSale(username):
    value = int(input("Type sale value: "))
    message = f'{username} sold {value}'
    print(message)
    return message

def main():
    username, login_msg = login()
    producer.send('user-access',value=login_msg.encode('utf-8'))

    while(True):
        print("Options: [1] register sale [2] restart system")
        option = int(input('>'))
        if option == 1:
            sale_msg = registerSale(username)
            producer.send('sales', value=sale_msg.encode('utf-8'))
        elif option == 2:
            restart_msg = restartSystem(username)
            producer.send('system-management',value=restart_msg.encode('utf-8'))
    

if __name__=='__main__':
    main()

