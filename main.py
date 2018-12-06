import pika as rmq
import logging
from mq.writer import write_message

QUEUE_ADDR = 'localhost'
NAME = 'workername'
READQUEUE = 'todoq'
WRITE_QUEUE = 'resultq'
WRITE_RETRY = 3


def handler(body):
  #Do some stuff
  # If handler logic is too large, move it to another package
  response = body
  return response


#main block#
logging.basicConfig(format='%(asctime)s,%(msecs)-3d - %(name)-12s - %(levelname)-8s => %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.INFO)
logging.info("Inicializando o worker %s" %NAME )

connection = rmq.BlockingConnection(rmq.ConnectionParameters(host=QUEUE_ADDR))
channel = connection.channel()

channel.queue_declare(queue=READQUEUE, durable=True)
logging.info(' [*] Waiting for messages. To exit press CTRL+C')

def callback(ch, method, properties, body):
  ### TODO Investigar qual a melhor prática neste caso: esperar a writer postar para mandar o ack, ou mandar antes de chamar a writer.
  logging.info(" [x] Received %r" % body)
  write_message(handler(body), QUEUE_ADDR, WRITE_QUEUE, WRITE_RETRY)
  ch.basic_ack(delivery_tag = method.delivery_tag)
  logging.info(" [x] Done")


channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue=READQUEUE)

channel.start_consuming()
