import pika

try:
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    print("Connection successful!")
except Exception as e:
    print(f"Failed to connect to RabbitMQ: {e}")
