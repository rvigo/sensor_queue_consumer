import pika
import log
import os

def connect_mq():  
    log.info('connecting to rabbitmq instance and queues')
    credentials = pika.PlainCredentials(os.environ['RABBITMQ_USER'], os.environ['RABBITMQ_PWD'])
    parameters = pika.ConnectionParameters(os.environ['RABBITMQ_BROKER'],
                                   5672,
                                   '/',
                                   credentials)

    connection = pika.BlockingConnection(parameters)
    log.info('connected to rabbitmq')
    channel = connection.channel()
    return channel, connection

def create_queue(channel, queue_name):
    channel.queue_declare(queue=queue_name, durable=True, exclusive=False, auto_delete=False)

def publish(channel, queue, payload):
    channel.basic_publish(exchange='',
                      routing_key=queue,
                      body=payload)

# connection.close()
