import pika
import json
import uproot
import awkward as ak
import numpy as np
import vector
import pickle
import time
import os

def CutPhotonReconstruction(photon_isTightID):
    return (photon_isTightID[:,0]==False) | (photon_isTightID[:,1]==False)

def CutPhotonPt(photon_pt):
    return (photon_pt[:,0] < 40) | (photon_pt[:,1] < 30)

def CutIsolationPt(photon_ptcone20):
    return (photon_ptcone20[:,0] > 4) | (photon_ptcone20[:,1] > 4)

def CutPhotonEtaTransition(photon_eta):
    condition_0 = (np.abs(photon_eta[:, 0]) < 1.52) & (np.abs(photon_eta[:, 0]) > 1.37)
    condition_1 = (np.abs(photon_eta[:, 1]) < 1.52) & (np.abs(photon_eta[:, 1]) > 1.37)
    return condition_0 | condition_1

def CalcMass(photon_pt, photon_eta, photon_phi, photon_e):
    p4 = vector.zip({"pt": photon_pt, "eta": photon_eta, "phi": photon_phi, "e": photon_e})
    invariant_mass = (p4[:, 0] + p4[:, 1]).M
    return invariant_mass

def ProcessTask(task):
    print(f"Processing task: {task}")
    try:
        file_path = task['file']
        fraction = task.get('fraction', 1)

        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return {"file": file_path, "error": "File not found"}

        try:
            with uproot.open(file_path + ":analysis") as t:
                tree = t
                numevents = tree.num_entries
                sample_data = []

                for data in tree.iterate(["photon_isTightID", "photon_pt", "photon_ptcone20", "photon_eta", "photon_phi", "photon_e"], library="ak", entry_stop=numevents * fraction):
                    data = data[~CutPhotonReconstruction(data['photon_isTightID'])]
                    data = data[~CutPhotonPt(data['photon_pt'])]
                    data = data[~CutIsolationPt(data['photon_ptcone20'])]
                    data = data[~CutPhotonEtaTransition(data['photon_eta'])]
                    data['mass'] = CalcMass(data['photon_pt'], data['photon_eta'], data['photon_phi'], data['photon_e'])

                    sample_data.append(data)
        except Exception as e:
            print(f"Error opening ROOT file or finding tree: {e}")
            return {"file": file_path, "error": str(e)}

        all_data = ak.concatenate(sample_data)
        serialized_data = pickle.dumps(all_data)
        return serialized_data
                        
    except Exception as e:
        print(f"Error processing file {task['file']}: {e}")
        return {"file": file_path, "error": str(e)}

def OnMessage(channel, method, properties, body):
    try:
        task = json.loads(body) 
        print(f"Received task: {task}")

        # Check for shutdown signal
        if 'shutdown' in task and task['shutdown']:
            print("Received shutdown signal. Exiting worker.")
            channel.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge the shutdown task
            channel.stop_consuming()
            return  # Exit early

        # Process the task
        result = ProcessTask(task)
        if result is not None:
            print(f"Task completed for {task['file']}. Sending results...")
            try:
                channel.basic_publish(
                    exchange='',
                    routing_key='result_queue',
                    body=result,
                    properties=pika.BasicProperties(delivery_mode=2)  # Make message persistent
                )
            except Exception as e:
                print(f"Error publishing result: {e}")
        else:
            print(f"Failed to process task for {task['file']}.")

        # Acknowledge the task
        channel.basic_ack(delivery_tag=method.delivery_tag)
        print(f"Acknowledged task for {task['file']}")

    except Exception as e:
        print(f"Error processing task: {e}")
        # Always acknowledge to prevent blocking, even if an error occurs
        channel.basic_ack(delivery_tag=method.delivery_tag)    

def ConnectToRabbitmq():
    connection = None
    while not connection:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            print("Worker connected")
        except pika.exceptions.AMQPConnectionError:
            print("Waiting for RabbitMQ to be ready...")
            time.sleep(5)
    return connection


connection = ConnectToRabbitmq()
channel = connection.channel()
channel.queue_declare(queue='data_processing', durable=True)

channel.basic_consume(queue='data_processing', on_message_callback=OnMessage)
print("Worker ready, waiting for tasks...")
try:
    channel.start_consuming()
except Exception as e:
    print(f"Error during consuming: {e}")
finally:
    channel.stop_consuming()
    connection.close()

