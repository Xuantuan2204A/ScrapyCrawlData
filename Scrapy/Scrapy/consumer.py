# -*- coding: utf-8 -*-
import pika
import json
from scrapy.conf import settings
connect_parameters = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(connect_parameters)
chanel = connection.channel()

result = chanel.queue_declare(queue='', exclusive=True)
# queue_name = settings['RABBIT_QUEUE']
queue_name = result.method.queue
print("Queue: ",queue_name)
chanel.queue_bind(exchange='logs', queue=queue_name, routing_key='monitaz_ifollow')

print(' [*] Waiting for logs. To exit press CTRL+C')

def callback(ch, method, properties, body):
    print("=====================")
    print("Method: {}".format(method))
    print("Properties: {}".format(properties))
    print(" [x] %r" % body)
    print("=====================")

chanel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)
print("Start consuming")
chanel.start_consuming()