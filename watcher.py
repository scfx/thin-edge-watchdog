#!/usr/bin/python3
# coding=utf-8
import sys
from paho.mqtt import client as mqtt_client

import time
import logging
import json
import threading
import docker

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.info('Logger for service watchdog mapper was initialised')


broker = 'localhost'
port = 1883
client_id = 'watchdog-service-mapper-client'


client = mqtt_client.Client(client_id)

def on_message(client, userdata, msg):
    try:
        message = json.loads(msg.payload)
        print(message)
        pid = message['pid']
        logger.debug(f'Pid is: {pid}')

        name = msg.topic.split("/")[-1]
        logger.debug(f'Name is: {name}')

        service = "systemd"
        logger.debug(f'Service is: {service}')

        status = message['status']
        logger.debug(f'Status is: {status}')
        #Publishing message
        client.publish("c8y/s/us",f'102,{pid},{service},{name},{status}')
    except Exception as e:
        logger.error(f'The following error occured: {e}, skipping message')

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    pass

def on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.warning("Unexpected MQTT disconnection. Will auto-reconnect")
    pass

def getCpuLoad(stats):
    #https://docs.docker.com/engine/api/v1.40/#tag/Container/operation/ContainerStats
    cpu_delta = stats["cpu_stats"]["cpu_usage"]["total_usage"] - stats["precpu_stats"]["cpu_usage"]["total_usage"]
    system_cpu_delta = stats["cpu_stats"]["system_cpu_usage"] - stats["precpu_stats"]["system_cpu_usage"]
    number_cpus = stats["cpu_stats"]["online_cpus"]
    return (cpu_delta / system_cpu_delta) * number_cpus * 100.0

def getMemoryUsage(stats):
    #https://docs.docker.com/engine/api/v1.40/#tag/Container/operation/ContainerStats
    used_memory = stats["memory_stats"]["usage"] - stats["memory_stats"]["stats"]["cache"]
    available_memory = stats["memory_stats"]["limit"]
    return (used_memory / available_memory) * 100.0

def check_container_status(client):
    # Connect to the Docker daemon
    logger.info("Start docker loop")
    docker_client = docker.from_env()

    while True:
        # Use the Docker daemon to get a list of all containers
        
        # Iterate over the containers, and check their status
        containers = docker_client.containers.list(all=True)
        for container in containers:
            status = 'up' if container.status == 'running' else 'down'
            client.publish("c8y/s/us",f'102,{container.short_id},docker,{container.name},{status}')
            logger.info(f'Send message 102,{container.short_id},docker,{container.name},{status} to topic c8y/s/us')
            if status == 'up':
                stats = container.stats(stream=False)
                topic = f'tedge/measurements/{container.short_id}' #
                message = f'{{"cpu":{{"%":{getCpuLoad(stats)}}},"memory":{{"%":{getMemoryUsage(stats)}}}}}'
                client.publish(topic,message)
                logger.info(f'Send message {message} to {topic}')
            #logger.info(f'Send message 104,{status} to topic c8y/s/us/{container.short_id}')
    # Sleep for 60 seconds before checking the status again
        # for log in container.attach(stdout=False, stderr=True,stream=True):
        #     if "ERROR" in str(log):
        #         print(log)
        #     if "WARNING" in str(log):
        #         print(log)
            
        time.sleep(60)




client.on_connect = on_connect
#client.on_message = on_message
client.on_disconnect = on_disconnect

if __name__== "__main__":
    try:
        logger.info("Connect")
        client.connect(broker, port)
        logger.info("Subscribe")
        #client.subscribe("tedge/health/#")
        logger.info("Loop")
        
        
        # Create a thread that will run the check_container_status function
        thread = threading.Thread(target=check_container_status, args=(client,))
        # Start the thread
        logger.info("Thread")      
        thread.start()
        client.loop_forever()
        
            
    except KeyboardInterrupt:
        sys.exit(1)
    except Exception as e:
        logger.error(f'The following error occured: {e}')
    finally:
        client.disconnect()