import pika as rmq
import logging

QUEUE_ADDR = 'localhost'
NAME = 'workername'
READQUEUE = 'todoq'
WRITEQUEUE = 'resultq'
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

def writer(message, retry):

  try:
    connection = rmq.BlockingConnection(rmq.ConnectionParameters(host=QUEUE_ADDR))

    channel = connection.channel()

    channel.queue_declare(queue=WRITEQUEUE, durable=True)

    channel.basic_publish(exchange='',
                          routing_key=WRITEQUEUE,
                          body=message,
                          properties=rmq.BasicProperties(
                            delivery_mode = 2, # make message persistent
                          ))
    logging.info(" [x] Sent %r" % message)
    connection.close()

  except Exception as err:
    if retry <= 0:
      logging.error('Failed publishing to Queue after %d attempts.' %WRITE_RETRY)
      logging.critical(err)
      return

    logging.info('Failed publishing to Queue. Remaining attempts : %d' %retry)
    writer(message, retry - 1)
  
  

def callback(ch, method, properties, body):
  ### TODO Investigar qual a melhor prática neste caso: esperar a writer postar para mandar o ack, ou mandar antes de chamar a writer.
  logging.info(" [x] Received %r" % body)
  writer(handler(body),WRITE_RETRY)
  ch.basic_ack(delivery_tag = method.delivery_tag)
  logging.info(" [x] Done")


channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue=READQUEUE)

channel.start_consuming()
