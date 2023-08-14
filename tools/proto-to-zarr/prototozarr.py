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


import logging
import argparse

import boto3
import xarray as xr
import zarr
import numpy as np
from cassandra.auth import PlainTextAuthProvider
import cassandra.concurrent
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import RoundRobinPolicy, TokenAwarePolicy
from solrcloudpy import SolrConnection, SearchOptions
from solrcloudpy.utils import SolrResponse
from s3fs import S3FileSystem, S3Map
from urllib.parse import urlparse
import json
from os.path import exists
from botocore.config import Config
from botocore.exceptions import ClientError
import nexusproto.DataTile_pb2 as nexusproto
from nexusproto.serialization import from_shaped_array
from uuid import UUID


XARRAY_8016 = tuple([int(n) for n in xr.__version__.split('.')[:3]]) >= (2023, 8, 1)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(threadName)s] [%(name)s::%(lineno)d] %(message)s'
)

logger = logging.getLogger('proto-to-zarr')


def _exists(path: str, store_type: str, aws_params: dict[str, str] = None) -> bool:
    if store_type == 'local':
        return exists(path)
    elif store_type == 's3':
        url = urlparse(path)

        bucket = url.netloc
        key = url.path

        if key[0] == '/':
            key = key[1:]

        if key[-1] == '/':
            key = f'{key}.zgroup'
        else:
            key = f'{key}/.zgroup'

        config = Config(region_name=aws_params.get('region', 'us-west-2'))

        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_params['key'],
            aws_secret_access_key=aws_params['secret'],
            config=config
        )

        try:
            s3.head_object(Bucket=bucket, Key=key)
            return True
        except s3.exceptions.NoSuchKey:
            return False
        except ClientError as e:
            r = e.response
            err_code = r["Error"]["Code"]

            if err_code == '404':
                return False

            logger.error(f'An AWS error occurred: Code={err_code} Message={r["Error"]["Message"]}')
            raise
        except Exception as e:
            logger.error('Something went wrong!')
            logger.exception(e)
            raise


def parse_args():
    parser = argparse.ArgumentParser(
        description='Transfer gridded dataset ingested into SDAP nexusproto format into Zarr',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        '--ds',
        dest='dataset',
        required=True,
        help='Dataset to convert',
    )

    parser.add_argument(
        '--solr',
        dest='solr',
        required=False,
        help='Solr host and port',
        default='localhost:8983',
    )

    parser.add_argument(
        '--cassandra',
        dest='cassandra',
        required=False,
        help='Cassandra hostname(s) / IP(s)',
        nargs='+',
        default=['localhost']
    )

    parser.add_argument(
        '--cassandra-port',
        dest='cassandra_port',
        required=False,
        type=int,
        default=9042,
        help='Cassandra port'
    )

    parser.add_argument(
        '--cassandra-keyspace',
        dest='keyspace',
        required=False,
        default='nexustiles',
        help='Cassandra keyspace'
    )

    parser.add_argument(
        '--cassandra-table',
        dest='table',
        required=False,
        default='sea_surface_temp',
        help='Cassandra table'
    )

    parser.add_argument(
        '-u', '--cassandra-username',
        dest='username',
        required=False,
        default='cassandra',
        help='Cassandra username'
    )

    parser.add_argument(
        '-p', '--cassandra-password',
        dest='password',
        required=False,
        default='cassandra',
        help='Cassandra password'
    )

    parser.add_argument(
        '-d', '--destination',
        dest='dest',
        required=True,
        help='Destination root path. Can be either local or in S3. If in S3, credentials must be provided'
    )

    aws = parser.add_argument_group('AWS', 'AWS info (credentials &c) for storing dataset into S3')

    aws.add_argument(
        '--aws-access-key-id',
        dest='aws_key',
        required=False,
        help='AWS access key ID'
    )

    aws.add_argument(
        '--aws-secret-access-key',
        dest='aws_secret',
        required=False,
        help='AWS secret access key'
    )

    aws.add_argument(
        '--aws-region',
        dest='aws_region',
        required=False,
        default='us-west-2',
        help='AWS region of output bucket'
    )

    aws.add_argument(
        '--aws-file',
        dest='aws_file',
        required=False,
        help='JSON file containing AWS info'
    )

    parser.add_argument(
        '--overwrite',
        dest='overwrite',
        required=False,
        help='Overwrite destination Zarr store if it exists',
        action='store_true'
    )

    parser.add_argument(
        '-k', '--keep',
        dest='keep',
        required=False,
        help='Do not delete source tiles from Solr/Cassandra',
        action='store_true'
    )

    # parser.add_argument(
    #     '-v', '--verbose',
    #     dest='verbose',
    #     required=False,
    #     help='Enable verbose output. Stackable so repeated usage increases verbosity',
    #     action='count',
    #     default=0
    # )

    args = parser.parse_args()

    print(args)

    logger.info('Validating args')

    url = urlparse(args.dest)

    if url.scheme in ['', 'file']:
        store_type = 'local'
    elif url.scheme == 's3':
        store_type = 's3'
    else:
        raise ValueError(f'Unsupported output store type: {url.scheme}')

    if store_type == 's3':
        if args.aws_file is not None:
            with open(args.aws_file) as f:
                aws_params = json.load(f)
        else:
            key = args.aws_key
            secret = args.aws_secret

            assert key is not None, 'AWS key ID and secret are required'
            assert secret is not None, 'AWS key ID and secret are required'

            aws_params = dict(
                key=key,
                secret=secret,
                region=args.aws_region
            )
    else:
        aws_params = {}

    if not args.overwrite and _exists(args.dest, store_type, aws_params):
        raise ValueError(f'Something already exists at {args.dest}')

    return args, store_type


def main(args, store_type):
    logger.info('Connecting to Solr & Cassandra')

    solr_connection = SolrConnection(args.solr)
    solr_collection = solr_connection['nexustiles']

    token_policy = TokenAwarePolicy(RoundRobinPolicy())
    auth_provider = PlainTextAuthProvider(
        username=args.username,
        password=args.password
    )

    cluster = Cluster(
        contact_points=args.cassandra,
        port=args.cassandra_port,
        protocol_version=3,
        execution_profiles={
            EXEC_PROFILE_DEFAULT: ExecutionProfile(load_balancing_policy=token_policy)
        },
        auth_provider=auth_provider
    )

    session = cluster.connect(args.keyspace)

    se = SearchOptions()
    se.commonparams.q(f'dataset_s:{args.dataset}').rows(1)

    logger.info('Sampling dataset...')

    response: SolrResponse = solr_collection.search(se)

    assert response.code == 200, 'Solr query failed'

    if response.result.response.numFound == 0:
        logger.info('No tiles found for the given dataset')
        return
    else:
        logger.info(f'Found {response.result.response.numFound:,} tiles')

    sample_tile_id = response.result.response.docs[0]['id']

    statement = session.prepare(
        "SELECT tile_blob from sea_surface_temp WHERE tile_id = ?"
    )

    sample_tile = session.execute(statement, (UUID(sample_tile_id),)).one()
    sample_tile = nexusproto.TileData.FromString(sample_tile.tile_blob)

    tile_type = sample_tile.WhichOneof('tile_type')

    if tile_type not in ['grid_tile', 'grid_multi_variable_tile']:
        raise ValueError(f'Dataset {args.dataset} is not gridded and therefore will not pe processed')

    logger.info('Verified dataset is gridded; determining days to pull')

    # TODO: Continue from here

if __name__ == '__main__':
    main(*parse_args())



