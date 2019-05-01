import pika
import sys
import json
import pymongo
import smtplib

myclient = pymongo.MongoClient('mongodb://130.245.170.88:27017/')
mydb = myclient['finalproject']
users = mydb['users']
questions = mydb['questions']
answers = mydb['answers']
media = mydb['media']

def callback(ch, method, properties, body):
    body = body.decode('utf-8')
    doc = json.loads(body)
    sender = doc['sender']
    receivers = doc['receivers']
    email = doc['email']
    try:
      smtpObj = smtplib.SMTP('localhost')
      smtpObj.sendmail(sender, receivers, email)         
      print("Successfully sent email", sys.stderr)
    except Exception:
      print("Error: unable to send email", sys.stderr)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def dequeue():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    # channel.exchange_declare('mongodb', 'direct')
    exc = channel.queue_declare(queue='email', durable=True)
    # channel.queue_bind(exchange='mongodb', queue='mongo', routing_key='mongo')
    print('listening')
    channel.basic_consume(queue='email', on_message_callback=callback)
    channel.start_consuming()
dequeue()                     
