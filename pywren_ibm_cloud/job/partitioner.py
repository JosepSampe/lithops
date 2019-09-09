#
# (C) Copyright IBM Corp. 2019
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

import logging
import requests
from pywren_ibm_cloud import utils
from multiprocessing.pool import ThreadPool
from pywren_ibm_cloud.storage.utils import CloudObject, CloudObjectUrl
from pywren_ibm_cloud.storage.backends.ibm_cos.ibm_cos import StorageBackend as ibm_cos_backend

logger = logging.getLogger(__name__)

CHUNK_THRESHOLD = 128*1024  # 128KB


def create_partitions(config, arg_data, chunk_size, chunk_number):
    """
    Method that returns the function that will create the partitions of the objects in the Cloud
    """
    logger.debug('Starting partitioner')

    parts_per_object = None

    sbs = set()
    buckets = set()
    prefixes = set()
    obj_names = set()
    urls = set()

    logger.debug("Parsing input data")
    for elem in arg_data:
        if 'url' in elem:
            urls.add(elem['url'])
        if 'obj' in elem:
            sb, bucket, prefix, obj_name = utils.split_object_url(elem['obj'])
            if obj_name:
                obj_names.add((bucket, prefix))
            elif prefix:
                prefixes.add((bucket, prefix))
            else:
                buckets.add(bucket)
            sbs.add(sb)

    if len(sbs) > 1:
        raise Exception('Currently we only support to process one storage backend at a time'
                        'Specified storage backends: {}'.format(sb))

    if [prefixes, obj_names, urls, buckets].count(True) > 1:
        raise Exception('You must provide as an input data a list of bucktes, '
                        'a list of buckets with object prefix, a list of keys '
                        'or a list of urls. Intermingled types are not allowed.')

    if not urls:
        # process objects from an object store. No url
        sb = sbs.pop()
        storage = ibm_cos_backend(config[sb])
        objects = {}
        if obj_names:
            for bucket, prefix in obj_names:
                logger.debug("Listing objects in '{}://{}'".format(sb, '/'.join([bucket, prefix])))
                objects[bucket] = storage.list_objects(bucket, prefix)
        elif prefixes:
            for bucket, prefix in prefixes:
                logger.debug("Listing objects in '{}://{}'".format(sb, '/'.join([bucket, prefix])))
                objects[bucket] = storage.list_objects(bucket, prefix)
        elif buckets:
            for bucket in buckets:
                logger.debug("Listing objects in '{}://{}'".format(sb, bucket))
                objects[bucket] = storage.list_objects(bucket)

        keys_dict = {}
        for bucket in objects:
            keys_dict[bucket] = {}
            for obj in objects[bucket]:
                keys_dict[bucket][obj['Key']] = obj['Size']

    if buckets or prefixes:
        partitions, parts_per_object = _split_objects_from_buckets(arg_data, keys_dict, chunk_size, chunk_number)

    elif obj_names:
        partitions, parts_per_object = _split_objects_from_keys(arg_data, keys_dict, chunk_size, chunk_number)

    elif urls:
        partitions, parts_per_object = _split_objects_from_urls(arg_data, chunk_size, chunk_number)

    else:
        raise ValueError('You did not provide any bucket or object key/url')

    return partitions, parts_per_object


def _split_objects_from_buckets(map_func_args_list, keys_dict, chunk_size, chunk_number):
    """
    Create partitions from bucket/s
    """
    logger.info('Creating dataset chunks from bucket/s ...')
    partitions = []
    parts_per_object = []

    for entry in map_func_args_list:
        # Each entry is a bucket
        sb, bucket, prefix, obj_name = utils.split_object_url(entry['obj'])

        if chunk_size:
            logger.info('Creating chunks from objects within: {}'.format(bucket))
        else:
            logger.info('Discovering objects within: {}'.format(bucket))

        for key, obj_size in keys_dict[bucket].items():
            if prefix in key and obj_size > 0:
                logger.debug('Creating partitions from object {} size {}'.format(key, obj_size))
                total_partitions = 0
                size = 0
                if chunk_size is not None and obj_size > chunk_size:
                    while size < obj_size:
                        brange = (size, size+chunk_size+CHUNK_THRESHOLD)
                        size += chunk_size
                        partition = entry.copy()
                        partition['obj'] = CloudObject(sb, bucket, key)
                        partition['obj'].data_byte_range = brange
                        partition['obj'].chunk_size = chunk_size
                        partition['obj'].part = total_partitions
                        partitions.append(partition)
                        total_partitions = total_partitions + 1
                else:
                    partition = entry.copy()
                    partition['obj'] = CloudObject(sb, bucket, key)
                    partition['obj'].data_byte_range = None
                    partition['obj'].chunk_size = chunk_size
                    partition['obj'].part = total_partitions
                    partitions.append(partition)
                    total_partitions = 1

                parts_per_object.append(total_partitions)

    return partitions, parts_per_object


def _split_objects_from_keys(map_func_args_list, keys_dict, chunk_size, chunk_number):
    """
    Create partitions from a list of objects keys
    """
    if chunk_size:
        logger.info('Creating chunks from object keys...')
    partitions = []
    parts_per_object = []

    for entry in map_func_args_list:
        # each entry is a key
        sb, bucket, prefix, obj_name = utils.split_object_url(entry['obj'])
        key = '/'.join([prefix, obj_name])
        try:
            obj_size = keys_dict[bucket][key]
        except Exception:
            raise Exception('Object key "{}" does not exist in "{}" bucket'.format(key, bucket))
        total_partitions = 0

        if chunk_size is not None and obj_size > chunk_size:
            size = 0
            while size < obj_size:
                brange = (size, size+chunk_size+CHUNK_THRESHOLD)
                size += chunk_size
                partition = entry
                partition['obj'] = CloudObject(sb, bucket, key)
                partition['obj'].data_byte_range = brange
                partition['obj'].chunk_size = chunk_size
                partition['obj'].part = total_partitions
                partitions.append(partition)
                total_partitions = total_partitions + 1
        else:
            partition = entry
            partition['obj'] = CloudObject(sb, bucket, key)
            partition['obj'].data_byte_range = None
            partition['obj'].chunk_size = chunk_size
            partition['obj'].part = total_partitions
            partitions.append(partition)
            total_partitions = 1

        parts_per_object.append(total_partitions)

    return partitions, parts_per_object


def _split_objects_from_urls(map_func_args_list, chunk_size, chunk_number):
    """
    Create partitions from a list of objects urls
    """
    if chunk_size:
        logger.info('Creating chunks from urls...')
    partitions = []
    parts_per_object = []

    def _split(entry):
        obj_size = None
        total_partitions = 0
        object_url = entry['url']
        metadata = requests.head(object_url)

        logger.info(object_url)
        #logger.debug(metadata.headers)

        if 'content-length' in metadata.headers:
            obj_size = int(metadata.headers['content-length'])

        if 'accept-ranges' in metadata.headers and chunk_size is not None \
           and obj_size is not None and obj_size > chunk_size:
            size = 0
            while size < obj_size:
                brange = (size, size+chunk_size+CHUNK_THRESHOLD)
                size += chunk_size
                partition = entry
                partition['url'] = CloudObjectUrl(object_url)
                partition['url'].data_byte_range = brange
                partition['url'].chunk_size = chunk_size
                partition['url'].part = total_partitions
                partitions.append(partition)
                total_partitions = total_partitions + 1
        else:
            # Only one partition
            partition = entry
            partition['url'] = CloudObjectUrl(object_url)
            partition['url'].data_byte_range = None
            partition['url'].chunk_size = chunk_size
            partition['url'].part = total_partitions
            partitions.append(partition)
            total_partitions = 1

        parts_per_object.append(total_partitions)

    pool = ThreadPool(128)
    pool.map(_split, map_func_args_list)
    pool.close()
    pool.join()

    return partitions, parts_per_object