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
import multiprocessing as mp
from functools import partial
from gevent.pywsgi import WSGIServer

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

mp_manager = mp.Manager()
JOB_INDEXES = mp_manager.dict()
JOBS_DONE = mp_manager.dict()
VIOLATIONS = mp_manager.dict()


@proxy.route('/setdone/<jobkey>/<jobindex>', methods=['POST'])
def set_done(jobkey, jobindex):
    global JOBS_DONE

    try:
        if jobkey in JOBS_DONE and jobindex not in JOBS_DONE[jobkey]:
            JOBS_DONE[jobkey].append(jobindex)
            proxy.logger.info(f'Job Key: {jobkey} - JOB INDEX {jobindex} Set done')
        proxy.logger.info(f'Done indexes in {jobkey}: ' + str(list(JOBS_DONE[jobkey])))
    except Exception as e:
        proxy.logger.error(e)
        pass

    status_code = flask.Response(status=200)
    return status_code


@proxy.route('/getid/<jobkey>/<total_calls>', methods=['GET'])
def get_id(jobkey, total_calls):
    global JOB_INDEXES
    global JOBS_DONE

    if jobkey not in JOB_INDEXES:
        proxy.logger.info(f'Creating {jobkey} job queue')
        JOB_INDEXES[jobkey] = mp_manager.list()
        JOBS_DONE[jobkey] = mp_manager.list()
        for call_id in reversed(range(int(total_calls))):
            JOB_INDEXES[jobkey].append(call_id)

    try:
        call_id = str(JOB_INDEXES[jobkey].pop())
    except Exception:
        if jobkey in JOBS_DONE and len(JOBS_DONE[jobkey]) == int(total_calls):
            call_id = '-2'
        else:
            call_id = '-1'

    remote_host = flask.request.remote_addr
    proxy.logger.info(f'Job Key: {jobkey} - Sending ID {call_id} to Host {remote_host}')

    return call_id


def master(encoded_payload):
    proxy.logger.setLevel(logging.DEBUG)

    config = b64str_to_dict(encoded_payload)

    rabbit_amqp_url = config['rabbitmq'].get('amqp_url')
    queue = config['rabbitmq'].get('queue', 'CloudButton')
    pikaparams = pika.URLParameters(rabbit_amqp_url)
    connection = pika.BlockingConnection(pikaparams)
    channel = connection.channel()

    def callback(ch, method, properties, body):
        global VIOLATIONS
        global JOB_INDEXES

        try:
            violation = json.loads(body.decode("utf-8"))
            proxy.logger.info(violation)
            key = violation['Message'][0]['key']
            violation_time = violation['Fields']['ViolationTime']
            if key not in VIOLATIONS or VIOLATIONS[key] != violation_time:
                VIOLATIONS[key] = violation_time
                jobkey, call_id = key.rsplit('-', 1)
                JOB_INDEXES[jobkey].append(int(call_id))
            else:
                proxy.logger.info(f'Ignoring violation {key}. Already processed')
        except Exception as e:
            proxy.logger.error(dict(JOB_INDEXES))
            proxy.logger.error(e)

    proxy.logger.info(f'Starting consuming from queue {queue} at {rabbit_amqp_url}')
    channel.basic_consume(queue, callback, auto_ack=True)
    threading.Thread(target=channel.start_consuming, daemon=True).start()

    # proxy.run(debug=True, host='0.0.0.0', port=MASTER_PORT, use_reloader=False)

    server = WSGIServer(('0.0.0.0', MASTER_PORT), proxy, log=proxy.logger)
    server.serve_forever()


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
            # No Indexes but job not finished
            time.sleep(5)
            continue

        if job_index == -2:
            # Job finished
            job_finished = True
            break

        act_id = str(uuid.uuid4()).replace('-', '')[:12]
        os.environ['__LITHOPS_ACTIVATION_ID'] = act_id
        os.environ['__LITHOPS_BACKEND'] = 'k8s'

        logger.info("Activation ID: {} - Job Index: {}".format(act_id, job_index))

        call_ids = call_ids_ranges[job_index]
        dbr = [data_byte_ranges[int(call_id)] for call_id in call_ids]
        payload['call_ids'] = call_ids
        payload['data_byte_ranges'] = dbr

        function_handler(payload)

        set_done = False
        while not set_done:
            try:
                url = f'http://{master_ip}:{MASTER_PORT}/setdone/{job_key}/{job_index}'
                res = requests.post(url)
                logger.info(res.status_code)
                set_done = True if res.status_code == 200 else False
            except Exception:
                time.sleep(0.1)

    logger.info(f'Job {job_key} finished, stopping container')


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
