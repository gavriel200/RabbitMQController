import pika
import uuid


class Rabbit:
    """
        A class for an easy way to connect publish and consume messages using RabbitMQ.

        Methods
        -------
        def __init__(self, host="localhost", port=5672, username="guest", password="guest"):
            Create a connection to Rabbitmq server.
        def close_connection(self):
            Close the connection to the RabbitMQ server.
        def declare_queue(self, queue_name: str, durable=True, auto_delete=False):
            Declare a queue to the server.
        def declare_random_queue(self, auto_delete=True):
            Declare a random temporary queue and returns its name for later use.
        def declare_exchange(self, exchange_name: str, exchange_type: str, durable=True, auto_delete=False):
            Declare an exchange and its type.
        def bind(self, queue: str, exchange: str, *args):
            Bind an already declared queue to an alreay created exchange with none or multiple routing keys.
        def delete_queue(self, queue_name: str):
            Delete and already created queue.
        def delete_exchange(self, exchange_name: str):
            Delete and already created exchange.
        def send_to_queue(self, message: str, queue: str, message_persistent=True):
            Publish a message directly to a queue.
        def send_to_exchange(self, message: str, exchange: str, routing_key: str, message_persistent=True):
            Publish a message to an exchange with a routing key.
        def receive(self, queue: str, func: callable, prefetch_count=1, acknowledge=True):
            Consume messages from a queue.
        def rpc_single_client(self, callback_queue: str, server_queue: str, message: str):
            Creates a rpc client that sends a message to an rpc server and then consumes from a callback queue. 
        def rpc_single_server(self, server_queue: str, func: callable):
            Creates a rpc server that consumes from rpc client and returns to the callback queue.
        def rpc_fanout_client(self, random_queue: str, callback_exchange: str, server_queue: str, message: str):
            Creates a rpc client that with a queue bound to a fanout exchange.
        def rpc_fanout_server(self, server_queue: str, func: callable):
            Creates a rpc server that consumes from rpc client and returns to the callback exchange.
        def __enter__(self):
            This method is called when using the 'with' keyword implementing the context manager.
        def __exit__(self, exc_type, exc_value, exc_traceback):
            This method is executed at the end of the context manager.
    """

    def __init__(self, host="localhost", port=5672, username="guest", password="guest"):
        """
            Initialize a connection to RabbitMQ server.

        Args:
        -----
            host (str, optional): The ip address of the RabbitMQ server. Defaults to "localhost".
            port (int, optional): The port for connecting to the RabbitMQ server. Defaults to 5672.
            username (str, optional): Username for connection. Defaults to "guest".
            password (str, optional): Password for connection. Defaults to "guest".
        """

        self.creds = pika.credentials.PlainCredentials(username, password)

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, port=port, credentials=self.creds))

        self.channel = self.connection.channel()

    def close_connection(self):
        """
            Disconnect from RabbitMQ. If there are any open channels, it will
            attempt to close them prior to fully disconnecting. Channels which
            have active consumers will attempt to send a Basic.Cancel to RabbitMQ
            to cleanly stop the delivery of messages prior to closing the channel.
        """

        self.connection.close()

    def declare_queue(self, queue_name: str, durable=True, auto_delete=False):
        """
            Declare queue, create if needed. This method creates or checks a
            queue. When creating a new queue the client can specify various
            properties that control the durability of the queue.

        Args:
        -----
            queue_name (str): The queue name. If an empty string is given and error will be raised.
            durable (bool, optional): The queue wont be deleted in case the server will be rebooted. Defaults to True.
            auto_delete (bool, optional): Delete the queue after consumer cancels or disconnects. Defaults to False.

        Raises:
        -------
            SyntaxError: you cant have an empty string for the queue name, if you do the queue will
            be created with a random name and you wont have its name.
        """

        if queue_name == "":
            raise SyntaxError(
                "You should not have an empty string as queue if there is no exchange.")

        self.channel.queue_declare(
            queue=queue_name, durable=durable, auto_delete=auto_delete)

    def declare_random_queue(self, auto_delete=True):
        """
            Declare a random queue. This method creates a random named queue.

        Args:
        -----
            auto_delete (bool, optional): Delete the queue after consumer cancels or disconnects. Defaults to True.

        Returns:
        --------
            Queue name (str): Returnes the name of the randrom queue that was created.
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
                these characters: letters, digits, hyphen, underscore, period, or colon.
            exchange_type (str): The exchange type to use, such as: direct, topic, headers and fanout.
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
            Delete an exchange from the server.

        Args:
        -----
            exchange_name (str): The exchange name.
        """

        self.channel.exchange_delete(exchange=exchange_name)

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
            exchange (str): The exchange name to which to send the message.
            routing_key (str): The routing key for the exchange.
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
            This method is used to start consuming messages from the RabbitMQ server.

        Args:
        -----
            queue (str): The queue to which you bind and listen.
            func (callable): A callback function that will be called
                with the message as an argument and executed each time
                a new message is consumed.
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

    def rpc_single_client(self, callback_queue: str, server_queue: str, message: str):
        """
            This method creates a rpc client, this client will send a message
            to a rpc server with a callback queue name(the queue the client
            consumes from) and correlation id to check that the client 
            receives only from the rpc server. 

        Args:
        -----
            callback_queue (str): The queue the rpc client will be consuming from.
            server_queue (str): The queue the rpc server is consuming from.
            message (str): The message you want to send to the rpc server.

        Returns:
        --------
            Message str: Returns the response from the rpc server.
        """

        self.response = None
        self.corr_id = str(uuid.uuid4())

        def callback(ch, method, properties, body):
            if self.corr_id == properties.correlation_id:
                self.response = body.decode("utf-8")

        self.channel.basic_consume(
            queue=callback_queue, on_message_callback=callback, auto_ack=True)

        self.channel.basic_publish(exchange='', routing_key=server_queue, properties=pika.BasicProperties(
            reply_to=callback_queue, correlation_id=self.corr_id,), body=message)

        while self.response is None:
            self.connection.process_data_events()

        return self.response

    def rpc_single_server(self, server_queue: str, func: callable):
        """
            This method creates a rpc server that consumes messages
            and sends them back to the callback queue it came from.

        Args:
        -----
            server_queue (str): The queue the server consumes from.
            func (callable): The function that is executed for each message received
                this function takes in the message received.
                The function should return a string that will be sent back to the rpc client.
        """

        def callback(ch, method, properties, body):
            response = func(body.decode("utf-8"))

            ch.basic_publish(exchange='',
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(
                                 correlation_id=properties.correlation_id),
                             body=response)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_qos(prefetch_count=1)

        self.channel.basic_consume(
            queue=server_queue, on_message_callback=callback)

        self.channel.start_consuming()

    def rpc_fanout_client(self, random_queue: str, callback_exchange: str, server_queue: str, message: str):
        """
            This method creates a rpc client made for the use of multiple threads creating
            different requests for the same rpc server.

            When we use this method we should create a different connection and random queue
            for each thread.
            All the random queues will be binded to the same fanout exchange, so all the
            messages will be sent to all the queues but only consumed by the client that created 
            the request(using the correlation id).

            And at the end of each request we should delete the random queue and close the connection.


        Args:
        -----
            random_queue (str): A random queue that was created for the purpose of consuming
                messages from the rpc server. Should be bind to the fanout exchange.
            server_queue (str): The queue the rpc server consumes from.
            callback_exchange (str): The exchange name that the random queue is bind to.
            message (str): The message you want to send to the rpc server.

        Returns:
        --------
            Message str: Returns the response from the rpc server.
        """

        self.response = None
        self.corr_id = str(uuid.uuid4())

        def callback(ch, method, properties, body):
            if self.corr_id == properties.correlation_id:
                self.response = body.decode("utf-8")

        self.channel.basic_consume(
            queue=random_queue, on_message_callback=callback, auto_ack=True)

        self.channel.basic_publish(exchange='', routing_key=server_queue, properties=pika.BasicProperties(
            reply_to=callback_exchange, correlation_id=self.corr_id,), body=message)

        while self.response is None:
            self.connection.process_data_events()

        return self.response

    def rpc_fanout_server(self, server_queue: str, func: callable):
        """
            This method creates a rpc server that consumes messages and sends
            them back to the callback exchange to all the queues bind to itit came from.

        Args:
        -----
            server_queue (str): The queue that the rpc server consumes from
            func (callable): The function that is executed for each message received
                this function takes in the message received.
                The function should return a string that will be sent back to the rpc client.
        """

        def callback(ch, method, properties, body):
            print("a")
            response = func(body.decode("utf-8"))

            ch.basic_publish(exchange=properties.reply_to,
                             routing_key='',
                             properties=pika.BasicProperties(
                                 correlation_id=properties.correlation_id),
                             body=response)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_qos(prefetch_count=1)

        self.channel.basic_consume(
            queue=server_queue, on_message_callback=callback)

        print("start consuming")

        self.channel.start_consuming()

    def __enter__(self):
        """
            This method is implemented when using the Rabbit class as a
            context manager using the 'with' keyword.

        Returns:
        --------
            Rabbit object: Returns a Rabbit object for later use.
        """

        return self
      
    def __exit__(self, exc_type, exc_value, exc_traceback):
        """
            This method is executed at the end of the context manager.
            It closes the connection to the RabbitMQ server.
        """

        self.close_connection()