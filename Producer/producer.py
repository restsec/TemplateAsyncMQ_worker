import pika as rmq
import logging

QUEUE_ADDR = 'localhost'
WRITEQUEUE = 'resultq'
WRITE_RETRY = 3

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

