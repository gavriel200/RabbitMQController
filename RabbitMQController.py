import pika


class Rabbit:
    """
        A class for an easy way to connect publish and consume messages using RabbitMQ.

        Methods
        -------
        def __init__(self, host="localhost", port=5672, username="guest", password="guest"):
            Create a connection to Rabbitmq server.
        def declare_queue(self, queue_name: str, durable=True, auto_delete=False):
            Declare a queue to the server
        def declare_random_queue(self, auto_delete=True):
            Declare a random temporry queue and return its name for later use.
        def declare_exchange(self, exchange_name: str, exchange_type: str, durable=True, auto_delete=False):
            Declare an exchange to the server
        def bind(self, queue: str, exchange: str, *args):
            Bind an already declared queue to an alreay created exchange with none or multiple routing keys..
        def delete_queue(self, queue_name: str):
            Delete and already created queue.
        def delete_exchange(self, exchange_name: str):
            Delete and already created exchange.
        def close_connection(self):
            Close the connection to the RabbitMQ server
        def send_to_queue(self, message: str, queue: str, message_persistent=True):
            Publish a message directly to a queue
        def send_to_exchange(self, message: str, exchange: str, routing_key: str, message_persistent=True):
            Publish a message to an exchange with a routing key.
        def receive(self, queue: str, func: callable, prefetch_count=1, acknowledge=True):
            Consume messages from a queue.
    """

    def __init__(self, host="localhost", port=5672, username="guest", password="guest"):
        """
            Initialize a connection to RabbitMQ server.

        Args:
        -----
            host (str, optional): The ip address of the RabbitMQ server. Defaults to "localhost".
            port (int, optional): the port for connecting to the RabbitMQ server. Defaults to 5672.
            username (str, optional): username for connection. Defaults to "guest".
            password (str, optional): password for connection. Defaults to "guest".
        """
        self.creds = pika.credentials.PlainCredentials(username, password)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, port=port, credentials=self.creds))
        self.channel = self.connection.channel()

    def declare_queue(self, queue_name: str, durable=True, auto_delete=False):
        """
            Declare queue, create if needed. This method creates or checks a
            queue. When creating a new queue the client can specify various
            properties that control the durability of the queue

        Args:
        -----
            queue_name (str): The queue name; if empty string, and error will be raised.
            durable (bool, optional): The queue wont be deleted in case the server will be rebooted. Defaults to True.
            auto_delete (bool, optional): Delete the queue after consumer cancels or disconnects. Defaults to False.

        Raises:
        -------
            SyntaxError: you cant have an empty string for the queue name, if you do the queue will
            be created with a random name.
        """
        if queue_name == "":
            raise SyntaxError(
                "you should not have an empty string as queue if there is no exchange.")

        self.channel.queue_declare(
            queue=queue_name, durable=durable, auto_delete=auto_delete)

    def declare_random_queue(self, auto_delete=True):
        """
            Declare a random queue. This method creates a random named queue.

        Args:
        -----
            auto_delete (bool, optional):  Delete the queue after consumer cancels or disconnects. Defaults to True.

        Returns:
        --------
            Queue name (str): returnes the name of the randrom queue that was created.
        """
        queue_name = self.channel.queue_declare(
            queue="", auto_delete=auto_delete)

        return queue_name.method.queue

    def declare_exchange(self, exchange_name: str, exchange_type: str, durable=True, auto_delete=False):
        """
            This method creates an exchange if it does not already exist, and if
            the exchange exists, verifies that it is of the correct and expected
            class.

        Args:
        -----
            exchange_name (str): The exchange name consists of a non-empty sequence of
                                 these characters: letters, digits, hyphen, underscore,
                                 period, or colon.
            exchange_type (str): The exchange type to use, such as:
                                 direct, topic, headers and fanout.
            durable (bool, optional): The exchange wont be deleted in case the server will be rebooted. Defaults to True.
            auto_delete (bool, optional): Delete the exchange after consumer cancels or disconnects. Defaults to False.
        """
        self.channel.exchange_declare(
            exchange=exchange_name, exchange_type=exchange_type, durable=durable, auto_delete=auto_delete)

    def bind(self, queue: str, exchange: str, *args):
        """
            Bind the queue to the specified exchange

        Args:
        -----
            queue (str): The queue to bind to the exchange.
            exchange (str): The source exchange to bind to.
            *args (str): The routing key/keys for the queue to bind to.
        """
        if len(args) == 0:
            self.channel.queue_bind(exchange=exchange, queue=queue)
        else:
            for routing_keys in args:
                self.channel.queue_bind(
                    exchange=exchange, queue=queue, routing_key=routing_keys)

    def delete_queue(self, queue_name: str):
        """
            Delete a queue from the server.

        Args:
        -----
            queue_name (str): The queue to delete.
        """
        self.channel.queue_delete(queue=queue_name)

    def delete_exchange(self, exchange_name: str):
        """
            Delete the exchange.

        Args:
        -----
            exchange_name (str): The exchange name.
        """
        self.channel.exchange_delete(exchange=exchange_name)

    def close_connection(self):
        """
            Disconnect from RabbitMQ. If there are any open channels, it will
            attempt to close them prior to fully disconnecting. Channels which
            have active consumers will attempt to send a Basic.Cancel to RabbitMQ
            to cleanly stop the delivery of messages prior to closing the channel.
        """
        self.connection.close()

    def send_to_queue(self, message: str, queue: str, message_persistent=True):
        """
            Publish a message directly to a queue. 

        Args:
        -----
            message (str): The message to be send.
            queue (str): The queue name you want to publish to.
            message_persistent (bool, optional): Message durability,
                messages wont be lost even if the server reboots. Defaults to True.
        """
        self.channel.basic_publish(
            exchange='',
            routing_key=queue,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2,) if message_persistent else None)

    def send_to_exchange(self, message: str, exchange: str, routing_key: str, message_persistent=True):
        """
            Publish a message to an exchange with a routing key. 

        Args:
        -----
            message (str): The message to be send.
            exchange (str): The exchange name to which to send the message
            routing_key (str): The routing key for the exchange
            message_persistent (bool, optional):  Message durability,
                messages wont be lost even if the server reboots. Defaults to True.
        """
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2,) if message_persistent else None)

    def receive(self, queue: str, func: callable, prefetch_count=1, acknowledge=True):
        """
            This method is used to start consuming messages from the RabbitMQ server

        Args:
            queue (str): The queue to which you bind and listen.
            func (callable): A callback function that will be called
                with the message as an argument and executed each time
                a new message is consumed
            prefetch_count (int, optional): The number of messages each consumer
                has at a time. Defaults to 1.
            acknowledge (bool, optional): message acknowledgement after each
                consumed message. Defaults to True.
        """
        def callback(ch, method, properties, body):
            func(body.decode("utf-8"))
            if acknowledge:
                ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_qos(prefetch_count=prefetch_count)
        self.channel.basic_consume(
            queue=queue, on_message_callback=callback, auto_ack=False if acknowledge else True)
        self.channel.start_consuming()
