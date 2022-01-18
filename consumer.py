from kafka import KafkaConsumer
import re
bootstrap_servers=['localhost:9092']

# We have 3 topics: sales, system-management and user-access
sales_consumer = KafkaConsumer('sales',auto_offset_reset='earliest',bootstrap_servers=bootstrap_servers)
sm_consumer = KafkaConsumer('system-management',auto_offset_reset='earliest',bootstrap_servers=bootstrap_servers)
user_consumer = KafkaConsumer('user-access',auto_offset_reset='earliest',bootstrap_servers=bootstrap_servers)


sales = {}
for message in sales_consumer:
    m = message.value.decode('utf-8')  
    print(f'New sale: {m}')
    sale_value = int(m.split(' ')[-1])
    seller = m.split(' ')[0]
    if seller not in sales:
        sales[seller] = sale_value
    else:
        sales[seller] += sale_value
    print(sales)


