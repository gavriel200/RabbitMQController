from RabbitMQController import Rabbit


def print_test_func(text):
    print(text)


rabbit = Rabbit()

queue_name = rabbit.declare_random_queue()

rabbit.declare_exchange("test_exchange", "direct")

rabbit.bind(queue_name, "test_exchange", "test")

rabbit.receive(queue_name, print_test_func)
