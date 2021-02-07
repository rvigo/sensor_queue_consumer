from pymongo import MongoClient
import log
import os

def connect_to_mongodb():
    log.info('connecting to mongo db instance')
    client = MongoClient(os.environ['MONGODB_INSTANCE'], 27017)

    log.info('connected to mongodb')
    db = client.get_database('temperature')
    return db

def temperature(db):
    return db.get_collection('temperature')

def humidity(db):
    return db.get_collection('humidity')

def error(db):
    return db.get_collection('error')


