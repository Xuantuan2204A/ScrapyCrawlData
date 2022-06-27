# -*- coding: utf-8 -*-
import pika
import json

connect_parameters = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(connect_parameters)
chanel = connection.channel()
chanel.queue_declare(queue='rabbit_elasticsearch')
print('[*] Waiting for messages. To exit press CTRL+C')
def on_message_recdived(ch, method, properties, body):
    print("Method: {}".format(method))
    print("Properties: {}".format(properties))
    print(body)
chanel.basic_consume(queue='rabbit_elasticsearch', on_message_callback=on_message_recdived, auto_ack=True)
print('start consuming')
chanel.start_consuming()

