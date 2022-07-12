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
import s3fs
from nexustiles.nexustiles import NexusTileService

import configparser
import io, os

from moto import mock_s3

import pytest

import numpy as np

class DummyTile:
    def __init__(self, tile_id):
        self.tile_id = tile_id

mock_s3 = mock_s3()
bucket_name = 'test-zarr'
root_key = "test_data/"
region = 'us-west-2'

port = 5555
endpoint = f"http://127.0.0.1:{port}"

test_data_path = os.getenv('TEST_DATA', 'zarr_test_data/')

#Got this from s3fs test on github
#https://github.com/fsspec/s3fs/blob/main/s3fs/tests/test_s3fs.py
@pytest.fixture()
def s3():
    import shlex
    import subprocess
    import time
    import requests

    try:
        # should fail since we didn't start server yet
        r = requests.get(endpoint)
    except:
        pass
    else:
        if r.ok:
            raise RuntimeError("moto server already up")
    if "AWS_SECRET_ACCESS_KEY" not in os.environ:
        os.environ["AWS_SECRET_ACCESS_KEY"] = "foo"
    if "AWS_ACCESS_KEY_ID" not in os.environ:
        os.environ["AWS_ACCESS_KEY_ID"] = "foo"
    proc = subprocess.Popen(shlex.split("moto_server s3 -p %s" % port))

    timeout = 5
    while timeout > 0:
        try:
            r = requests.get(endpoint)
            if r.ok:
                break
        except:
            pass
        timeout -= 0.1
        time.sleep(0.1)

    from botocore.session import Session
    session = Session()
    client = session.create_client("s3", endpoint_url=endpoint)

    client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': region})

    fs = s3fs.S3FileSystem(anon=False, client_kwargs={"endpoint_url": endpoint})

    for root, dirs, filenames in os.walk(test_data_path):
        for file in filenames:
            local = os.path.join(root, file)
            rel = os.path.relpath(local, test_data_path)

            key = os.path.join(bucket_name, root_key, rel)

            fs.put(local, key)

    yield fs
    proc.terminate()
    proc.wait()

@pytest.fixture()
def bounds():
    yield {
        'min_lat': 21,
        'max_lat': 29,
        'min_lon': -97,
        'max_lon': -79,
        'start_time': os.getenv('START_TIME', '2017-01-01T09:00:00'),
        'end_time': os.getenv('END_TIME', '2017-02-01T00:00:00')
    }

@pytest.fixture()
def config():
    cfg = f"""
    [s3]
    bucket={bucket_name}
    key={root_key}
    region={region}
    public=false
    """

    buf = io.StringIO(cfg)
    config = configparser.ConfigParser()
    config.read_file(buf)

    yield config

@pytest.fixture()
def tile_service(s3):
    from nexustiles.dao.ZarrProxy import ZarrProxy

    cfg = f"""
    [s3]
    bucket={bucket_name}
    key={root_key}
    region={region}
    public=false
    [datastore]
    store=zarrS3
    """

    buf = io.StringIO(cfg)
    config = configparser.ConfigParser()
    config.read_file(buf)

    svc = NexusTileService(skipMetadatastore=True, config=config, skipDatastore=True)
    svc._datastore = ZarrProxy(config, test_fs=s3)

    yield svc

def test_bounds(bounds, tile_service):
    tile_id = tile_service.bounds_to_direct_tile_id(
        bounds['min_lat'],
        bounds['min_lon'],
        bounds['max_lat'],
        bounds['max_lon'],
        bounds['start_time'],
        bounds['end_time']
    )

    tiles = [DummyTile(tile_id)]

    tile_data = tile_service.fetch_data_for_tiles(*tiles)[0]

    assert bounds['min_lat'] <= np.amin(tile_data.latitudes)
    assert bounds['max_lat'] >= np.amax(tile_data.latitudes)
    assert bounds['min_lon'] <= np.amin(tile_data.longitudes)
    assert bounds['max_lon'] >= np.amax(tile_data.longitudes)

    assert bounds['start_time'] <= str(np.amin(tile_data.times))
    assert bounds['end_time'] >= str(np.amax(tile_data.times))

def test_matchup(bounds, tile_service):
    pass