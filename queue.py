import pika
import sys
import json
import pymongo
import threading
import functools

myclient = pymongo.MongoClient('mongodb://130.245.170.88:27017/')
mydb = myclient['finalproject']
users = mydb['users']
questions = mydb['questions']
answers = mydb['answers']

def do_work(connection, channel, delivery_tag, body):
    thread_id = threading.get_ident()
    # fmt1 = 'Thread id: {} Delivery tag: {} Message body: {}'
    # LOGGER.info(fmt1.format(thread_id, delivery_tag, body))
    # Sleeping to simulate 10 seconds of work
    # time.sleep(10)
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
    cb = functools.partial(ack_message, channel, delivery_tag)
    connection.add_callback_threadsafe(cb)

def dequeue():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.exchange_declare('mongodb', 'direct')
    exc = channel.queue_declare(queue='mongo', durable=True)
    channel.queue_bind(exchange='mongodb', queue='mongo', routing_key='mongo')
    threads = []
    on_message_callback = functools.partial(on_message, args=(connection, threads))
    channel.basic_consume(on_message_callback, 'standard')

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()

    # Wait for all to complete
    for thread in threads:
        thread.join()

    connection.close()
	    #channel.basic_ack()

dequeue()


