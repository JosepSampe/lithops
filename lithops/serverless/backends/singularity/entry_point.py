#
# (C) Copyright Cloudlab URV 2020
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import pika
import os
import sys
import uuid
import json
import logging
import flask
import time
import requests
from functools import partial
from multiprocessing import Value

from lithops.version import __version__
from lithops.utils import setup_lithops_logger, b64str_to_dict
from lithops.worker import function_handler
from lithops.worker.utils import get_runtime_metadata
from lithops.constants import JOBS_PREFIX
from lithops.storage.storage import InternalStorage

from lithops.serverless.backends.k8s import config

logger = logging.getLogger('lithops.worker')

proxy = flask.Flask(__name__)

JOB_INDEXES = {}

def extract_runtime_meta(payload):
    logger.info(f"Lithops v{__version__} - Generating metadata")

    runtime_meta = get_runtime_metadata()

    channel.basic_publish(
        exchange='',
        routing_key='status_queue',
        body=json.dumps(runtime_meta),
        properties=pika.BasicProperties(
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
        ))

def run_job_k8s_rabbitmq(payload, running_jobs):
    logger.info(f"Lithops v{__version__} - Starting kubernetes execution")

    act_id = str(uuid.uuid4()).replace('-', '')[:12]
    os.environ['__LITHOPS_ACTIVATION_ID'] = act_id
    os.environ['__LITHOPS_BACKEND'] = 'k8s_rabbitmq'

    function_handler(payload)
    running_jobs.value += len(payload['call_ids'])

    logger.info("Finishing kubernetes execution")


def callback_work_queue(ch, method, properties, body):
    """Callback to receive the payload and run the jobs"""
    global cpus_pod

    logger.info("Call from lithops received.")

    message = json.loads(body)
    tasks = message['total_calls']

    running_jobs = Value('i', cpus_pod)  # Shared variable to track completed jobs

    # If there are more tasks than cpus in the pod, we need to send a new message
    if tasks <= running_jobs.value:
        processes_to_start = tasks
    else:
        processes_to_start = running_jobs.value

        message_to_send = message.copy()
        message_to_send['total_calls'] = tasks - running_jobs.value
        message_to_send['call_ids'] = message_to_send['call_ids'][running_jobs.value:]
        message_to_send['data_byte_ranges'] = message_to_send['data_byte_ranges'][running_jobs.value:]

        message['total_calls'] = running_jobs.value
        message['call_ids'] = message['call_ids'][:running_jobs.value]
        message['data_byte_ranges'] = message['data_byte_ranges'][:running_jobs.value]

        ch.basic_publish(
            exchange='',
            routing_key='task_queue',
            body=json.dumps(message_to_send),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ))

    logger.info(f"Starting {processes_to_start} processes")

    message['worker_processes'] = running_jobs.value
    running_jobs.value -= processes_to_start
    run_job_k8s_rabbitmq(message, running_jobs)

    logger.info("All processes completed")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def start_rabbitmq_listening(payload):
    global cpus_pod

    # Connect to rabbitmq
    params = pika.URLParameters(payload['amqp_url'])
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='task_queue', durable=True)
    channel.basic_qos(prefetch_count=1)

    # Get the number of cpus of the pod
    cpus_pod = payload['cpus_pod']

    # Start listening to the new job
    channel.basic_consume(queue='task_queue', on_message_callback=callback_work_queue)

    logger.info("Listening to rabbitmq...")
    channel.start_consuming()

def actions_switcher(ch, method, properties, body):
    logger.info("Action received from lithops.")

    message = json.loads(body)
    action = message['action']
    encoded_payload = message['payload']

    payload = b64str_to_dict(encoded_payload)
    setup_lithops_logger(payload.get('log_level', 'INFO'))

    if action == 'get_metadata':
        extract_runtime_meta(payload)

    """elif action == 'start_rabbitmq':
        start_rabbitmq_listening(payload)"""
    

if __name__ == '__main__':
    global cpus_pod

    amqp_url = sys.argv[1]
    cpus_pod = sys.argv[2]

    # print("AMQP_URL: ", amqp_url)
    # print("CPUS_POD: ", cpus_pod)

    # Connect to rabbitmq
    params = pika.URLParameters(amqp_url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='task_queue', durable=True)
    channel.basic_qos(prefetch_count=1)
    
    # Start listening to the new job
    channel.basic_consume(queue='task_queue', on_message_callback=actions_switcher)

    logger.info("Listening to rabbitmq...")
    channel.start_consuming()