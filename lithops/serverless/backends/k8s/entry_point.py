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

import os
import sys
import uuid
import json
import logging
import flask
import time
import requests
import pika
import threading
import queue
import multiprocessing as mp
from functools import partial

from lithops.version import __version__
from lithops.utils import setup_lithops_logger, b64str_to_dict,\
    iterchunks
from lithops.worker import function_handler
from lithops.worker.utils import get_runtime_preinstalls
from lithops.constants import JOBS_PREFIX
from lithops.storage.storage import InternalStorage


logger = logging.getLogger('lithops.worker')

proxy = flask.Flask(__name__)

MASTER_PORT = 8080

JOB_INDEXES = {}


@proxy.route('/getid/<jobkey>/<total_calls>', methods=['GET'])
def get_id(jobkey, total_calls):
    global JOB_INDEXES

    if jobkey not in JOB_INDEXES:
        JOB_INDEXES[jobkey] = mp.Queue()
        for call_id in range(int(total_calls)):
            JOB_INDEXES[jobkey].put(call_id)

    try:
        call_id = str(JOB_INDEXES[jobkey].get(timeout=0.1))
    except queue.Empty:
        call_id = '-1'

    remote_host = flask.request.remote_addr
    proxy.logger.info(f'Job Key: {jobkey} - Sending ID {call_id} to Host {remote_host}')

    return call_id


def master(encoded_payload):
    proxy.logger.setLevel(logging.DEBUG)

    config = b64str_to_dict(encoded_payload)

    rabbit_amqp_url = config['rabbitmq'].get('amqp_url')
    queue = 'CloudButton'
    pikaparams = pika.URLParameters(rabbit_amqp_url)
    connection = pika.BlockingConnection(pikaparams)
    channel = connection.channel()

    def callback(ch, method, properties, body):
        global JOB_INDEXES

        try:
            violation = json.loads(body.decode("utf-8"))
            proxy.logger.info(violation)
            jobkey, call_id = violation['Message'][0]['key'].rsplit('-', 1)
            JOB_INDEXES[jobkey].put(int(call_id))
        except Exception as e:
            proxy.logger(e)
            pass

    proxy.logger.info(f'Starting consuming from queue {queue} at {rabbit_amqp_url}')
    channel.basic_consume(queue, callback, auto_ack=True)
    threading.Thread(target=channel.start_consuming, daemon=True).start()

    proxy.run(debug=True, host='0.0.0.0', port=MASTER_PORT)


def extract_runtime_meta(encoded_payload):
    logger.info("Lithops v{} - Generating metadata".format(__version__))

    payload = b64str_to_dict(encoded_payload)

    setup_lithops_logger(payload['log_level'])

    runtime_meta = get_runtime_preinstalls()

    internal_storage = InternalStorage(payload)
    status_key = '/'.join([JOBS_PREFIX, payload['runtime_name']+'.meta'])
    logger.info("Runtime metadata key {}".format(status_key))
    dmpd_response_status = json.dumps(runtime_meta)
    internal_storage.put_data(status_key, dmpd_response_status)


def run_job(encoded_payload):
    logger.info("Lithops v{} - Starting kubernetes execution".format(__version__))

    payload = b64str_to_dict(encoded_payload)
    setup_lithops_logger(payload['log_level'])

    total_calls = payload['total_calls']
    job_key = payload['job_key']
    master_ip = os.environ['MASTER_POD_IP']

    chunksize = payload['chunksize']
    call_ids_ranges = [call_ids_range for call_ids_range in iterchunks(payload['call_ids'], chunksize)]
    data_byte_ranges = payload['data_byte_ranges']

    job_finished = False
    while not job_finished:
        job_index = None

        while job_index is None:
            try:
                url = f'http://{master_ip}:{MASTER_PORT}/getid/{job_key}/{total_calls}'
                res = requests.get(url)
                job_index = int(res.text)
            except Exception:
                time.sleep(0.1)

        if job_index == -1:
            time.sleep(1)
            continue

        act_id = str(uuid.uuid4()).replace('-', '')[:12]
        os.environ['__LITHOPS_ACTIVATION_ID'] = act_id
        os.environ['__LITHOPS_BACKEND'] = 'k8s'

        logger.info("Activation ID: {} - Job Index: {}".format(act_id, job_index))

        call_ids = call_ids_ranges[job_index]
        dbr = [data_byte_ranges[int(call_id)] for call_id in call_ids]
        payload['call_ids'] = call_ids
        payload['data_byte_ranges'] = dbr

        function_handler(payload)


if __name__ == '__main__':
    action = sys.argv[1]
    encoded_payload = sys.argv[2]

    switcher = {
        'preinstalls': partial(extract_runtime_meta, encoded_payload),
        'run': partial(run_job, encoded_payload),
        'master': partial(master, encoded_payload)

    }

    func = switcher.get(action, lambda: "Invalid command")
    func()
