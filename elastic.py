from elasticsearch import Elasticsearch
import log
import os


def connect_to_es():
    log.info('connecting to ES')
    client = Elasticsearch([
        {'host': os.environ['ES_INSTANCE'], 'port': 9200, 'useSSL': False}
    ])
    log.info('connected to ES')
    return client

def insert_at_index(client, msg, index):
    client.index(index=index, body=msg)