import pika as rmq
import logging

# This function 
def write_message(message, QUEUE_ADDR, WRITE_QUEUE, retry=3):
  """
  write_message function will post a message to a queue using the AMQP 0-9-1 protocol.

  This function helps ensure all messages are posted correclty. They are posted with delivery_mode = 2, this means they are made persistent, and if the queue fails, the messages will be kept in disk storage.

  Parameters
  ----------
  message : str
      Message to be posted as body in the message queue. It should be encoded in the desired scheme (JSON, MessagePack)
  QUEUE_ADDR : str
      Domain name / ip address of the message queue.
  WRITE_QUEUE : str
      Queue name. This should be the same name as the one used in the desired consumer.
  retry : int
      Numer of retry attempts to be performed if the operation fails.
      

  Returns
  -------
  bool
      True : Operation Successfull
      False: Operation Failed. In this case, logging.error and logging.critical messages will be produced, after n logging.info messages, n being the ammount of attempts.

  """
  try:
    connection = rmq.BlockingConnection(rmq.ConnectionParameters(host=QUEUE_ADDR))

    channel = connection.channel()

    channel.queue_declare(queue=WRITE_QUEUE, durable=True)

    channel.basic_publish(exchange='',
                          routing_key=WRITE_QUEUE,
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
      return False

    logging.info('Failed publishing to Queue. Remaining attempts : %d' %retry)
    writer(message, QUEUE_ADDR, WRITE_QUEUE, retry - 1)
    return True

