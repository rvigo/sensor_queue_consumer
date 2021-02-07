import mongo
import rabbit
import ast
import log
import elastic
import os
from datetime import datetime

TEMPERATURE_QUEUE = os.environ['TEMP_QUEUE']
HUMIDITY_QUEUE = os.environ['HUMIDITY_QUEUE']
ERROR_QUEUE = os.environ['ERROR_QUEUE']
ES_TEMPERATURE_INDEX = os.environ['TEMPERATURE_INDEX']
ES_HUMIDITY_INDEX = os.environ['HUMIDITY_INDEX']
ES_ERROR_INDEX = os.environ['ERROR_INDEX']

def connect_to_instances():
    db = mongo.connect_to_mongodb()
    es = elastic.connect_to_es()
    channel, connection = rabbit.connect_mq()
    return db, es, channel, connection


def prepare_message(message: dict):
    prepared_message = ast.literal_eval(message.decode("UTF-8"))
    prepared_message['datetime'] = datetime.strptime(
        prepared_message['datetime'], '%Y-%m-%d %H:%M:%S.%f')
    return prepared_message


def main():
    db = None
    es = None
    channel = None
    connection = None

    try:
        log.info('trying to connect to instances')
        db, es, channel, connection = connect_to_instances()

        def callback_temperature(ch, method, properties, body):
            message = prepare_message(body)
            elastic.insert_at_index(es, message, ES_TEMPERATURE_INDEX)
            mongo.temperature(db).insert_one(message)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        def callback_humidity(ch, method, properties, body):
            message = prepare_message(body)
            elastic.insert_at_index(es, message, ES_HUMIDITY_INDEX)
            mongo.humidity(db).insert_one(message)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        def callback_on_error(ch, method, properties, body):
            message = prepare_message(body)
            elastic.insert_at_index(es, message, ES_ERROR_INDEX)
            mongo.error(db).insert_one(message)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_consume(
            queue=TEMPERATURE_QUEUE, on_message_callback=callback_temperature)
        channel.basic_consume(
            queue=HUMIDITY_QUEUE, on_message_callback=callback_humidity)
        channel.basic_consume(
            queue=ERROR_QUEUE, on_message_callback=callback_on_error)
        
        log.info(
            f'consuming {TEMPERATURE_QUEUE}, {HUMIDITY_QUEUE} and {ERROR_QUEUE} queues')

        log.info('loading data into elasticsearch and mongodb')
        channel.start_consuming()

    except Exception as e:
        log.error(e.args[0], e)


if __name__ == "__main__":
    main()
