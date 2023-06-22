from webservice.NexusHandler import nexus_handler
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler
from webservice.webmodel import NexusResults, NexusProcessingException

from nexustiles.nexustiles import Tile

import uuid
import logging
import json
from json import JSONEncoder
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)


class NumpyEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, np.ndarray):
            return {
                'type': 'NumPy ndarray',
                'dtype': str(o.dtype),
                'shape': o.shape,
                'data': o.tolist()
            }
        elif isinstance(o, np.ma.MaskedArray):
            return {
                'type': 'NumPy masked ndarray',
                'dtype': str(o.dtype),
                'shape': o.shape,
                'data': np.ma.getdata(o).tolist(),
                'mask': np.ma.getmaskarray(o).tolist()
            }
        elif isinstance(o, uuid.UUID):
            return str(o)
        elif isinstance(o, datetime):
            return str(o.strftime('%Y-%m-%dT%H:%M:%S%z'))
        elif isinstance(o, np.float32):
            return float(o)
        else:
            return JSONEncoder.default(self, o)



@nexus_handler
class TileFetch(NexusCalcHandler):
    name = 'Debug Tile Fetch'
    path = '/tilefetch'
    description = 'Debug handler to fetch tile(s) for a given list of tile ids separated by commas'
    params = {

    }
    singleton = True
    suppress = True

    def __init__(self, tile_service_factory, **kwargs):
        NexusCalcHandler.__init__(self, tile_service_factory, desired_projection='swath')

    def parse_args(self, request):
        provided_tile_ids = request.get_argument('tileIds').split(',')

        def is_uuid(s):
            try:
                uuid.UUID(s)
                return True
            except ValueError:
                return False

        tile_ids = [tid for tid in provided_tile_ids if is_uuid(tid)]

        if len(tile_ids) != len(provided_tile_ids):
            logger.warning('Some tile ids were dropped because they were of invalid form')

        if len(tile_ids) == 0:
            raise NexusProcessingException('No valid tile ids provided', code=400)

        return tile_ids

    def calc(self, request, **kwargs):
        tile_ids = self.parse_args(request)

        tile_service = self._get_tile_service()

        tiles = tile_service.find_tiles_by_id(tile_ids)

        def tile_to_dict(tile: Tile):
            tile_dict = {
                'id': tile.tile_id,
                'dataset_id': tile.dataset_id,
                'section_spec': tile.section_spec,
                'dataset': tile.dataset,
                'granule': tile.granule,
                'bbox': tile.bbox,
                'min_time': tile.min_time,
                'max_time': tile.max_time,
                'tile_stats': tile.tile_stats,
                'is_multi': tile.is_multi,
                'longitudes': tile.longitudes,
                'latitudes': tile.latitudes,
                'times': tile.times,
                'data': tile.data,
                'meta': tile.meta_data
            }

            return tile_dict

        return TileFetchResults(
            [tile_to_dict(t) for t in tiles],
            tile_ids
        )


class TileFetchResults(NexusResults):
    def __init__(self, tiles, ids):
        self.__ids = ids
        self.__tiles = tiles

    def toJson(self):
        tiles_dict = dict([(t['id'], t) for t in self.__tiles])

        return json.dumps(
            {
                'ids': self.__ids,
                'tile_data': tiles_dict
            },
            indent=4,
            cls=NumpyEncoder
        )