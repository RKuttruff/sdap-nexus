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
import xarray as xr
import zarr
import numpy as np
from cassandra.auth import PlainTextAuthProvider
import cassandra.concurrent
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import RoundRobinPolicy, TokenAwarePolicy
from solrcloudpy import SolrConnection, SearchOptions
from s3fs import S3FileSystem, S3Map


XARRAY_8016 = tuple([int(n) for n in xr.__version__.split('.')[:3]]) >= (2023, 8, 1)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(threadName)s] [%(name)s::%(lineno)d] %(message)s'
)

logger = logging.getLogger('proto-to-zarr')


def parse_args():
    parser = argparse.ArgumentParser(
        description='Transfer gridded dataset ingested into SDAP nexusproto format into Zarr',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
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

    creds = parser.add_argument_group('Credentials', 'AWS credentials for storing dataset into S3')

    creds.add_argument(
        '--aws-access-key-id',
        dest='aws_key',
        required=False,
        help='AWS access key ID'
    )

    creds.add_argument(
        '--aws-secret-access-key',
        dest='aws_secret',
        required=False,
        help='AWS secret access key'
    )

    creds.add_argument(
        '--aws-file',
        dest='aws_file',
        required=False,
        help='JSON file containing AWS credentials'
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

    parser.add_argument(
        '-v', '--verbose',
        dest='verbose',
        required=False,
        help='Enable verbose output. Stackable so repeated usage increases verbosity',
        action='count',
        default=0
    )

    args = parser.parse_args()

    print(args)

    # TODO: Validate args, set logger(s) verbosity, check if path exists (and is zarr?)
    #  maybe connect to Cassandra & Solr (& check AWS creds if needed?) here
    #  return args namespace & store type

    # Verbosity:
    # 0:  local: INFO,  others: WARNING/CRITICAL
    # 1:  local: DEBUG, others: WARNING/CRITICAL
    # 2:  local: DEBUG, others: INFO
    # 3+: local: DEBUG, others: DEBUG



