from RabbitMQController import Rabbit

rabbit = Rabbit()

rabbit.declare_exchange("test_exchange", "direct")

rabbit.send_to_exchange("hello world", "test_exchange", "test")

rabbit.close_connection()