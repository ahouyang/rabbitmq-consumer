import pika
import sys
import json
import pymongo

myclient = pymongo.MongoClient('mongodb://130.245.170.88:27017/')
mydb = myclient['finalproject']
users = mydb['users']
questions = mydb['questions']
answers = mydb['answers']

def dequeue():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.exchange_declare('mongodb', 'direct')
    exc = channel.queue_declare(queue='mongo', durable=True)
    channel.queue_bind(exchange='mongodb', queue='mongo', routing_key='mongo')
    print('listening')
    while True:
        meth, prop, body = channel.basic_get(queue='mongo')
        if body is not None:
            #channel.close()
            #connection.close()
            body = body.decode('utf-8')
            doc = json.loads(body)
            collection = None
            if doc['collection'] == 'questions':
            	collection = questions
            elif doc['collection'] == 'users':
            	collection = users
            elif doc['collection'] == 'answers':
            	collection = answers
            collection.insert_one(doc)
            print("got message: " + str(doc), sys.stderr)
	    #channel.basic_ack()

dequeue()


