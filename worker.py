import redis
from rq import Worker, Queue, Connection
import os

# Define your Redis connection details
redis_host = 'redis-14433.c256.us-east-1-2.ec2.cloud.redislabs.com'
redis_port = 14433
redis_password = 'yanpfAzAcVeMqSYH7DE7Hb2LTfKHLye7'

# Establish a Redis connection
conn = redis.Redis(
    host=redis_host,
    port=redis_port,
    password=redis_password
)


# Define the queues you want to listen on
listen = ['default']

if __name__ == '__main__':
    # Establish a connection with the Redis server
    with Connection(conn):
        # Create a worker for the specified queues
        worker = Worker(map(Queue, listen))
        # Start the worker to process jobs
        worker.work()
    
