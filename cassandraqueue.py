import pika
import sys
import json
import base64
from cassandra.cluster import Cluster

# myclient = pymongo.MongoClient('mongodb://130.245.170.88:27017/')
# mydb = myclient['finalproject']
# users = mydb['users']
# questions = mydb['questions']
# answers = mydb['answers']

cluster = Cluster(['192.168.122.21'])
session = cluster.connect(keyspace='stackoverflow')

def callback(ch, method, properties, body):
    body = body.decode('utf-8')
    info = body.split(',')
    media_id = info[0]
    b = bytearray(base64.b64decode(info[1]))
    filetype = info[2]
    added = True if info[3] == 'True' else False
    username = info[4]
    cqlinsert = 'insert into media (id, content, type, added, poster) values (%s, %s, %s, %s, %s);'
    session.execute(cqlinsert, (media_id, b, filetype, added, username))
    # session.execute(body)
    # doc = json.loads(body)
    # collection = None
    # if doc['collection'] == 'questions':
    #     collection = questions
    # elif doc['collection'] == 'users':
    #     collection = users
    # elif doc['collection'] == 'answers':
    #     collection = answers
    # collection.insert_one(doc)
    print("got message: " + cqlinsert, sys.stderr)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def dequeue():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    # channel.exchange_declare('mongodb', 'direct')
    exc = channel.queue_declare(queue='cassandra', durable=True)
    # channel.queue_bind(exchange='mongodb', queue='mongo', routing_key='mongo')
    print('listening')
    channel.basic_consume(queue='cassandra', on_message_callback=callback)
    channel.start_consuming()
dequeue()                     
