# MQ Writer Module

Insert this module in any aplication capable of writing to the MessageQueue (aka producers).


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