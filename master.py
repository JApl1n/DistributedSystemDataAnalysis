import pika
import json
import time
import subprocess
import docker
import os

def GetWorkerCount():
    workerNamePrefix="worker"
    try:
        client = docker.from_env()
        containers = client.containers.list(filters={"status": "running"})
        workerCount = sum(1 for container in containers if workerNamePrefix in container.name)
        print(f"Detected {workerCount} running workers.")
        return workerCount
    except Exception as e:
        print(f"Error counting workers: {e}")
        return 0

def ConnectToRabbitmq():
    connection = None
    while not connection:
        try:
            print("Connecting to RabbitMQ...")
            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            print("Connected to RabbitMQ.")
        except pika.exceptions.AMQPConnectionError:
            print("RabbitMQ not ready. Retrying in 5 seconds...")
            time.sleep(5)
    return connection

def SendTasks():
    connection = ConnectToRabbitmq()
    channel = connection.channel()
    channel.queue_declare(queue='data_processing', durable=True)

    files = None
    dataDir = "./data/"

    files = [f for f in os.listdir(dataDir)
         if os.path.isfile(os.path.join(dataDir, f)) and not f.startswith('.')]

    for file in files:
        task = {'file': ("./data/"+file), 'fraction': 1}
        channel.basic_publish(exchange='', routing_key='data_processing', body=json.dumps(task))
        print(f"Task sent: {task}")

    # Send one shutdown signal for each worker
    for _ in range(GetWorkerCount()):  # Adjusts to number of workers
        shutdown_signal = {'shutdown': True}
        channel.basic_publish(exchange='', routing_key='data_processing', body=json.dumps(shutdown_signal))
        print("Sent shutdown signal to worker.")

    connection.close()

if __name__ == "__main__":
    SendTasks()

