#!/usr/local/bin/python -u

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import requests
import requests.exceptions
import json
import json.decoder
import time
import sys
import logging
from kazoo.client import KazooClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout)

logger = logging.getLogger(__name__)

MAX_RETRIES = int(os.environ["MAX_RETRIES"])
SDAP_ZK_SOLR = os.environ["SDAP_ZK_SOLR"]
SDAP_SOLR_URL = os.environ["SDAP_SOLR_URL"]
ZK_LOCK_GUID = os.environ["ZK_LOCK_GUID"]
MINIMUM_NODES = int(os.environ["MINIMUM_NODES"])
CREATE_COLLECTION_PARAMS = os.environ["CREATE_COLLECTION_PARAMS"]


def get_cluster_status():
    try:
        return requests.get("{}admin/collections?action=CLUSTERSTATUS".format(SDAP_SOLR_URL)).json()
    except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError):
        return False


def add_field(schema_url, field_name, field_type, indexed=True, stored=True, **kwargs):
    field_properties = {
        'name': field_name,
        'type': field_type,
        'indexed': indexed,
        'stored': stored
    }

    try:
        field_properties['docValues'] = kwargs['docValues']
    except KeyError:
        field_properties['docValues'] = False

    try:
        field_properties['sortMissingFirst'] = kwargs['sortMissingFirst']
    except KeyError:
        field_properties['sortMissingFirst'] = False

    try:
        field_properties['sortMissingLast'] = kwargs['sortMissingLast']
    except KeyError:
        field_properties['sortMissingLast'] = False

    try:
        field_properties['multiValued'] = kwargs['multiValued']
    except KeyError:
        field_properties['multiValued'] = False

    try:
        field_properties['uninvertible'] = kwargs['uninvertible']
    except KeyError:
        field_properties['uninvertible'] = True

    try:
        field_properties['termVectors'] = kwargs['termVectors']
    except KeyError:
        field_properties['termVectors'] = False

    try:
        field_properties['termPositions'] = kwargs['termPositions']
    except KeyError:
        field_properties['termPositions'] = False

    try:
        field_properties['termOffsets'] = kwargs['termOffsets']
    except KeyError:
        field_properties['termOffsets'] = False

    try:
        field_properties['termPayloads'] = kwargs['termPayloads']
    except KeyError:
        field_properties['termPayloads'] = False

    try:
        field_properties['required'] = kwargs['required']
    except KeyError:
        field_properties['required'] = False

    try:
        field_properties['useDocValuesAsStored'] = kwargs['useDocValuesAsStored']
    except KeyError:
        field_properties['useDocValuesAsStored'] = False

    try:
        field_properties['large'] = kwargs['large']
    except KeyError:
        field_properties['large'] = False

    if 'omitNorms' in kwargs:
        field_properties['omitNorms'] = kwargs['omitNorms']

    if 'omitTermFreqAndPositions' in kwargs:
        field_properties['omitTermFreqAndPositions'] = kwargs['omitTermFreqAndPositions']

    if 'omitPositions' in kwargs:
        field_properties['omitPositions'] = kwargs['omitPositions']

    payload_dict = {
        'add-field': field_properties
    }

    payload_str = json.dumps(payload_dict).encode('utf-8')

    logger.info(f'Adding new field: {field_name} of type {field_type} with properties: \n'
                 "\n".join(["{} = {}".format(key, field_properties[key]) for key in field_properties if key not in ['name', 'type']]))

    response = requests.post(url=schema_url, data=payload_str)

    if response.status_code < 400:
        logger.info("Success.")
    else:
        logger.error(f"Error creating field '{field_name}': {response.text}")


def add_dynamic_field(schema_url, field_name, field_type, indexed=True, stored=True, **kwargs):
    field_properties = {
        'name': field_name,
        'type': field_type,
        'indexed': indexed,
        'stored': stored
    }

    try:
        field_properties['docValues'] = kwargs['docValues']
    except KeyError:
        field_properties['docValues'] = False

    try:
        field_properties['sortMissingFirst'] = kwargs['sortMissingFirst']
    except KeyError:
        field_properties['sortMissingFirst'] = False

    try:
        field_properties['sortMissingLast'] = kwargs['sortMissingLast']
    except KeyError:
        field_properties['sortMissingLast'] = False

    try:
        field_properties['multiValued'] = kwargs['multiValued']
    except KeyError:
        field_properties['multiValued'] = False

    try:
        field_properties['uninvertible'] = kwargs['uninvertible']
    except KeyError:
        field_properties['uninvertible'] = True

    try:
        field_properties['termVectors'] = kwargs['termVectors']
    except KeyError:
        field_properties['termVectors'] = False

    try:
        field_properties['termPositions'] = kwargs['termPositions']
    except KeyError:
        field_properties['termPositions'] = False

    try:
        field_properties['termOffsets'] = kwargs['termOffsets']
    except KeyError:
        field_properties['termOffsets'] = False

    try:
        field_properties['termPayloads'] = kwargs['termPayloads']
    except KeyError:
        field_properties['termPayloads'] = False

    try:
        field_properties['required'] = kwargs['required']
    except KeyError:
        field_properties['required'] = False

    try:
        field_properties['useDocValuesAsStored'] = kwargs['useDocValuesAsStored']
    except KeyError:
        field_properties['useDocValuesAsStored'] = False

    try:
        field_properties['large'] = kwargs['large']
    except KeyError:
        field_properties['large'] = False

    if 'omitNorms' in kwargs:
        field_properties['omitNorms'] = kwargs['omitNorms']

    if 'omitTermFreqAndPositions' in kwargs:
        field_properties['omitTermFreqAndPositions'] = kwargs['omitTermFreqAndPositions']

    if 'omitPositions' in kwargs:
        field_properties['omitPositions'] = kwargs['omitPositions']

    payload_dict = {
        'add-dynamic-field': field_properties
    }

    payload_str = json.dumps(payload_dict).encode('utf-8')

    logger.info(f'Adding new field: {field_name} of type {field_type} with properties: \n'
                 "\n".join(["{} = {}".format(key, field_properties[key]) for key in field_properties if key not in ['name', 'type']]))

    response = requests.post(url=schema_url, data=payload_str)

    if response.status_code < 400:
        logger.info("Success.")
    else:
        logger.error(f"Error creating dynamic field '{field_name}': {response.text}")


logger.info("Attempting to aquire lock from {}".format(SDAP_ZK_SOLR))
zk_host, zk_chroot = SDAP_ZK_SOLR.split('/')
zk = KazooClient(hosts=zk_host)
zk.start()
zk.ensure_path(zk_chroot)
zk.chroot = zk_chroot
lock = zk.Lock("/collection-creator", ZK_LOCK_GUID)
try:
    with lock:  # blocks waiting for lock acquisition
        logger.info("Lock aquired. Checking for SolrCloud at {}".format(SDAP_SOLR_URL))
        # Wait for MAX_RETRIES for the entire Solr cluster to be available.
        attempts = 0
        status = None
        collection_exists = False
        while attempts <= MAX_RETRIES:
            status = get_cluster_status()
            if not status:
                # If we can't get the cluster status, my Solr node is not running
                attempts += 1
                logger.info("Waiting for Solr at {}".format(SDAP_SOLR_URL))
                time.sleep(1)
                continue
            else:
                # If we can get the cluster status, at least my Solr node is running
                # We can check if the collection exists already now
                if 'collections' in status['cluster'] and 'nexustiles' in status['cluster']['collections']:
                    # Collection already exists. Break out of the while loop
                    collection_exists = True
                    logger.info("nexustiles collection already exists.")
                    break
                else:
                    # Collection does not exist, but need to make sure number of expected nodes are running
                    live_nodes = status['cluster']['live_nodes']
                    if len(live_nodes) < MINIMUM_NODES:
                        # Not enough live nodes
                        logger.info("Found {} live node(s). Expected at least {}. Live nodes: {}".format(len(live_nodes), MINIMUM_NODES, live_nodes))
                        attempts += 1
                        time.sleep(1)
                        continue
                    else:
                        # We now have a full cluster, ready to create collection.
                        logger.info("Detected full cluster of at least {} nodes. Checking for nexustiles collection".format(MINIMUM_NODES))
                        break

        # Make sure we didn't exhaust our retries
        if attempts > MAX_RETRIES:
            raise RuntimeError("Exceeded {} retries while waiting for at least {} nodes to become live for {}".format(MAX_RETRIES, MINIMUM_NODES, SDAP_SOLR_URL))

        # Full cluster, did not exceed retries. Check if collection already exists
        if not collection_exists:
            # Collection does not exist, create it.
            create_command = "{}admin/collections?action=CREATE&{}".format(SDAP_SOLR_URL, CREATE_COLLECTION_PARAMS)
            logger.info("Creating collection with command {}".format(create_command))
            create_response = requests.get(create_command).json()
            if 'failure' not in create_response:
                # Collection created, we're done.
                logger.info("Collection created. {}".format(create_response))
                pass
            else:
                # Some error occured while creating the collection
                raise RuntimeError("Could not create collection. Received response: {}".format(create_response))

            schema_api = "{}nexustiles/schema".format(SDAP_SOLR_URL)

            field_type_payload = json.dumps({
                "add-field-type": {
                    "name": "geo",
                    "class": "solr.SpatialRecursivePrefixTreeFieldType",
                    "geo": "true",
                    "precisionModel": "fixed",
                    "maxDistErr": "0.000009",
                    "spatialContextFactory": "com.spatial4j.core.context.jts.JtsSpatialContextFactory",
                    "precisionScale": "1000",
                    "distErrPct": "0.025",
                    "distanceUnits": "degrees"}})

            logger.info("Creating field-type 'geo'...")
            field_type_response = requests.post(url=schema_api, data=field_type_payload)
            if field_type_response.status_code < 400:
                logger.info("Success.")
            else:
                logger.error("Error creating field type 'geo': {}".format(field_type_response.text))

            logger.info(f'Now adding fields and dynamic fields with schema URL: {schema_api}')

            add_field(schema_api, 'table_s',             'string',  False, False)
            add_field(schema_api, 'geo',                 'geo',     True,  False)
            add_field(schema_api, 'solr_id_s',           'string',  False, False)
            add_field(schema_api, 'selectionSpec_s',     'string',  False, True)
            add_field(schema_api, 'dataset_s',           'string',  True,  True)
            add_field(schema_api, 'granule_s',           'string',  True,  True)
            add_field(schema_api, 'tile_var_name_ss',    'strings', False, True)
            add_field(schema_api, 'day_of_year_i',       'pint',    True,  False)
            add_field(schema_api, 'tile_min_lon',        'pdouble', True,  True)
            add_field(schema_api, 'tile_max_lon',        'pdouble', True,  True)
            add_field(schema_api, 'tile_min_lat',        'pdouble', True,  True)
            add_field(schema_api, 'tile_max_lat',        'pdouble', True,  True)
            add_field(schema_api, 'tile_depth',          'pdouble', False, False)
            add_field(schema_api, 'tile_min_time_dt',    'pdate',   True,  True)
            add_field(schema_api, 'tile_max_time_dt',    'pdate',   True,  True)
            add_field(schema_api, 'tile_min_val_d',      'pdouble', False, True)
            add_field(schema_api, 'tile_max_val_d',      'pdouble', False, True)
            add_field(schema_api, 'tile_avg_val_d',      'pdouble', False, True)
            add_field(schema_api, 'tile_count_i',        'pint',    True,  True)

            add_dynamic_field(schema_api, '*.tile_standard_name_s', 'string', False, True)
finally:
    zk.stop()
    zk.close()

# We're done, do nothing forever.
logger.info("Collection init script has finished. Now doing nothing on an endless loop.")
logger.info("If running interactively, you can safely kill this container with Ctrl + C.")
logger.info("Otherwise, you don't need to do anything.")
while True:
    time.sleep(987654321)
