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


import io
import gzip
import json
import numpy
import logging

from datetime import datetime
from pytz import timezone

from webservice.NexusHandler import nexus_handler
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler
from webservice.webmodel import NexusResults, NexusProcessingException

EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))
ISO_8601 = '%Y-%m-%dT%H:%M:%S%z'


@nexus_handler
class SamplePluginAlgorithm(NexusCalcHandler):
    name = "Sample Plugin algorithm"
    path = "/plugin/sample"
    description = "Sample Plugin algorithm"
    params = {}
    singleton = True

    def __init__(self, tile_service_factory, **kwargs):
        NexusCalcHandler.__init__(self, tile_service_factory)

    def calc(self, computeOptions, **args):
        logging.getLogger(__name__).info('test ')
        return SimpleResults(results=['SampleResult'])


class SimpleResults(NexusResults):
    def __init__(self, results):
        super().__init__(results=results)

    def toJson(self):
        return json.dumps(self.results())
