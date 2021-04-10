# RabbitMQController

The RabbitMQController class is used for and easy way to connect, public and consume messages using RabbitMQ.

To use this class you will need the pika module

```
python -m pip install pika --upgrade
```

to use the class you need to import it and create an object of it:

```python
from RabbitMQController import Rabbit

rabbit = Rabbit()
```

From there we can use some methods to create queues and exhanges, publish and consume messages etc.

```python 
rabbit.declare_exchange("test_exchange", "direct")

rabbit.send_to_exchange("hello world", "test_exchange", "test")

rabbit.close_connection()
```
