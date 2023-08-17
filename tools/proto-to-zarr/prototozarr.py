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


import argparse
import json
import logging
from os.path import exists, join
from urllib.parse import urlparse
from time import sleep
from uuid import UUID

import boto3
import cassandra.concurrent
import nexusproto.DataTile_pb2 as nexusproto
import numpy as np
import xarray as xr
import zarr
from botocore.config import Config
from botocore.exceptions import ClientError
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import RoundRobinPolicy, TokenAwarePolicy
from nexusproto.serialization import from_shaped_array
from s3fs import S3FileSystem, S3Map
from solrcloudpy import SolrConnection, SearchOptions
from solrcloudpy.utils import SolrResponse

XARRAY_8016 = tuple([int(n) for n in xr.__version__.split('.')[:3]]) >= (2023, 8, 0)

WEC_SUPPORT = \
    tuple([int(n) for n in zarr.__version__.split('.')]) >= (2, 11) and \
    tuple([int(n) for n in xr.__version__.split('.')[:3]]) >= (2022, 6, 0)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(threadName)s] [%(name)s::%(lineno)d] %(message)s'
)

logger = logging.getLogger('proto-to-zarr')

SOLR_BATCH_SIZE = 256
CASSANDRA_BATCH_SIZE = 1024


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
        help='Destination root path. Can be either local or in S3. If in S3, credentials must be provided. Output will '
             'be at <destination>/<ds>/'
    )

    parser.add_argument(
        '-c', '--chunking',
        dest='chunking',
        nargs=3,
        type=int,
        required=True,
        help='Chunking shape for zarr array. Must provide 3 integers in the order of time latitude longitude. Ex: '
             '--chunking 7 500 500'
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

    parser.add_argument(
        '-y', '--yes',
        dest='yes',
        action='store_true',
        help='Do not prompt for confirmation on delete'
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

    variable_names = response.result.response.docs[0]['tile_var_name_ss']

    statement = session.prepare(
        "SELECT tile_blob from sea_surface_temp WHERE tile_id = ?"
    )

    sample_tile = session.execute(statement, (UUID(sample_tile_id),)).one()
    sample_tile = nexusproto.TileData.FromString(sample_tile.tile_blob)

    tile_type = sample_tile.WhichOneof('tile_type')

    if tile_type not in ['grid_tile', 'grid_multi_variable_tile']:
        raise ValueError(f'Dataset {args.dataset} is not gridded and therefore will not pe processed')

    is_multi = tile_type == 'grid_multi_variable_tile'

    logger.info('Verified dataset is gridded; determining days to pull')

    se = SearchOptions()

    se.commonparams.\
        q(f'dataset_s:{args.dataset}').\
        rows(0).\
        fq('{!frange l=0 u=0}ms(tile_min_time_dt,tile_max_time_dt)')

    se.facetparams.\
        field('tile_min_time_dt').\
        limit(-1).\
        mincount(1)

    response: SolrResponse = solr_collection.search(se)

    assert response.code == 200, 'Solr query failed'

    days = list(response.result.facet_counts.facet_fields.dict['tile_min_time_dt'].keys())
    days.sort()

    logger.info(f'Found {len(days)} to process')

    slices = []
    processed_ids = []

    for day in days:
        logger.info(f'Gathering tiles for {day}')

        escaped_dt = day.replace(':', '\\:')

        se = SearchOptions()

        se.commonparams. \
            q(f'dataset_s:{args.dataset}'). \
            rows(500). \
            fq(f'tile_min_time_dt:{escaped_dt}').fl('id')

        response: SolrResponse = solr_collection.search(se)

        assert response.code == 200, 'Solr query failed'

        ids = [UUID(doc['id']) for doc in response.result.response.docs]

        logger.info(f'Collected {len(ids)} tiles')
        logger.info('Fetching tile data')

        tiles = []
        retries = 3

        while retries > 0:
            none_failed = True
            retries -= 1
            failed = []

            for tile_id, (success, result) in zip(
                    ids,
                    cassandra.concurrent.execute_concurrent_with_args(
                        session,
                        statement,
                        [(id,) for id in ids],
                        concurrency=50
                    )
            ):
                if not success:
                    none_failed = False
                    failed.append(tile_id)
                else:
                    tile_data = result.one()
                    tile_data = nexusproto.TileData.FromString(tile_data)

                    tile_type = sample_tile.WhichOneof('tile_type')

                    tile_data = getattr(tile_data, tile_type)

                    tile_dict = dict(
                        latitude=from_shaped_array(tile_data.latitude),
                        longitude=from_shaped_array(tile_data.longitude),
                        time=tile_data.time,
                        data=from_shaped_array(tile_data.variable_data)
                    )

                    tiles.append(tile_dict)

            if none_failed:
                break
            else:
                ids = failed
                sleep(2)

        logger.info('Tile data fetched, constructing complete dataset slice')

        lats = np.unique([tile['latitude'] for tile in tiles])
        lons = np.unique([tile['longitude'] for tile in tiles])
        times = np.unique([tile['time'] for tile in tiles])

        vals_3d = np.empty((len(variable_names), len(times), len(lats), len(lons)))

        data_dict = {}

        for tile in tiles:
            time = tile['time']
            data = tile['data']
            if is_multi:
                data = np.moveaxis(data, -1, 0)
            else:
                data = np.expand_dims(data, axis=0)

            for i in range(len(variable_names)):
                variable = variable_names[i]
                for j, lat in enumerate(tile['latitude']):
                    for k, lon in enumerate(tile['longitude']):
                        data_dict[(variable, time, lat, lon)] = data[i, j, k]

        for i, t in enumerate(times):
            for j, lat in enumerate(lats):
                for k, lon in enumerate(lons):
                    for v in range(len(variable_names)):
                        vals_3d[v, i, j, k] = data_dict.get((variable_names[i], t, lat, lon), np.nan)

        ds = xr.Dataset(
            data_vars={
                variable_names[i]: (('time', 'latitude', 'longitude'), vals_3d[i]) for i in range(len(variable_names))
            },
            coords=dict(
                time=('time', times),
                latitude=('latitude', lats),
                longitude=('longitude', lons)
            )
        )

        slices.append(ds)
        processed_ids.extend(ids)

    logger.info('Concatenating slices')

    completed_ds = xr.concat(slices, dim='time').sortby('time')

    chunking = tuple(args.chunking)

    for var in completed_ds.data_vars:
        completed_ds[var] = completed_ds[var].chunk(chunking)

    logger.info('Writing output zarr array')

    if store_type == 'local':
        store = join(args.dest, args.dataset)
    else:
        s3 = S3FileSystem(
            False,
            key=args.aws_key,
            secret=args.aws_secret,
            client_kwargs=dict(region_name=args.aws_region)
        )

        store = S3Map(
            root=f'{args.dest.rstrip("/")}/{args.dataset}',
            s3=s3,
            check=False
        )

    compressor = zarr.Blosc(cname='blosclz', clevel=9)

    encoding = {
        var: {
            'compressor': compressor,
            'chunks': chunking
        } for var in completed_ds.data_vars
    }

    if XARRAY_8016:
        completed_ds.to_zarr(
            store,
            mode='w',
            encoding=encoding,
            write_empty_chunks=False,
            consolidated=True
        )
    else:
        if WEC_SUPPORT:
            for var in completed_ds.data_vars:
                encoding[var]['write_empty_chunks'] = False

        completed_ds.to_zarr(
            store,
            mode='w',
            encoding=encoding,
            consolidated=True
        )

    if args.keep:
        logger.info('Dataset conversion completed')
        return
    else:
        logger.info('Dataset conversion completed. Source tiles will now be deleted from Solr and Cassandra. To keep '
                    'the source tiles, use command line switch \'-k\' / \'--keep\'')

        if not args.yes:
            do_continue = input(f'Are you sure you want to delete {len(processed_ids):,} tiles? y/[n]: ').lower()

            while do_continue not in ['', 'y', 'n']:
                do_continue = input(f'Are you sure you want to delete {len(processed_ids):,} tiles? y/[n]: ').lower()

            if do_continue in ['', 'n']:
                logger.info('Quitting')
                return

            logger.info('Proceeding with delete')

        solr_batches = [processed_ids[i:i+SOLR_BATCH_SIZE] for i in range(0, len(processed_ids), SOLR_BATCH_SIZE)]
        cassandra_batches = [
            processed_ids[i:i+CASSANDRA_BATCH_SIZE] for i in range(0, len(processed_ids), CASSANDRA_BATCH_SIZE)
        ]

        logger.info('Starting Solr delete')

        deleting = 0

        for batch in solr_batches:
            m = json.dumps({'delete': batch})

            deleting += len(batch)

            logger.info(f'Deleting batch of {len(batch)} tiles from Solr | ({deleting:,}/{len(processed_ids):,}) '
                        f'[{deleting/len(processed_ids)*100:7.3f}%]')

            solr_collection._update(m)
            solr_collection.commit()

        logger.info('Starting Cassandra delete')

        statement = session.prepare(
            'DELETE FROM sea_surface_temp WHERE tile_id=?'
        )

        deleting = 0

        for batch in cassandra_batches:
            retries = 3

            while retries > 0:
                none_failed = True
                retries -= 1
                failed = []

                deleting += len(batch)

                logger.info(f'Deleting batch of {len(batch)} tiles from Cassandra | '
                            f'({deleting:,}/{len(processed_ids):,}) [{deleting / len(processed_ids) * 100:7.3f}%]')

                for tile_id, (success, result) in zip(
                    batch,
                    cassandra.concurrent.execute_concurrent_with_args(
                        session,
                        statement,
                        [(UUID(tile_id),) for tile_id in batch],
                        concurrency=100
                    )
                ):
                    if not success:
                        none_failed = False
                        failed.append(tile_id)

                if none_failed:
                    break
                else:
                    if retries > 0:
                        logger.warning(f'Need to retry {len(failed):,} tiles')
                    else:
                        logger.error(f'Some tiles could not be deleted after several retries:\n'
                                     f'{json.dumps(failed, indent=4)}')

                    sleep(2)
                    batch = failed

        logger.info(f'Finished deleting {len(processed_ids):,} tiles')

if __name__ == '__main__':
    main(*parse_args())



