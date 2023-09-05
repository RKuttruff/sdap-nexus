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
from nexustiles.backends.nexusproto.dao.SolrProxy import SolrProxy as SolrProxyBase
from datetime import datetime


SOLR_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


class SolrProxy(SolrProxyBase):
    def __init__(self, config):
        super(self, config)
        self.logger = logging.getLogger(__name__)

    def find_tiffs_in_date_range(self, dataset, start, end, **kwargs):
        search = f'dataset_s:{dataset}'

        time_clause = "(" \
                      "min_time_dt:[%s TO %s] " \
                      "OR max_time_dt:[%s TO %s] " \
                      "OR (min_time_dt:[* TO %s] AND max_time_dt:[%s TO *])" \
                      ")" % (
                          start, end,
                          start, end,
                          start, end
                      )

        params = {
            'fq': [time_clause],
            'fl': 'path_s, granule_s'
        }

        self._merge_kwargs(params, **kwargs)

        return self.do_query_all(
            *(search, None, None, False, None),
            **params
        )

    def date_range_for_dataset(self, dataset, **kwargs):
        search = f'dataset_s:{dataset}'

        kwargs['rows'] = 1
        kwargs['sort'] = ['max_time_dt desc']
        kwargs['fl'] = 'min_time_dt, max_time_dt'

        params = {}

        self._merge_kwargs(params, **kwargs)

        results, start, found = self.do_query(*(search, None, None, True, None), **params)

        max_time = self.convert_iso_to_datetime(results[0]['max_time_dt'])

        params['sort'] = ['min_time_dt asc']

        results, start, found = self.do_query(*(search, None, None, True, None), **params)

        min_time = self.convert_iso_to_datetime(results[0]['min_time_dt'])

        return min_time, max_time

    def find_days_in_range_asc(self, min_lat, max_lat, min_lon, max_lon, ds, start_time, end_time, **kwargs):

        search = 'dataset_s:%s' % ds

        search_start_s = datetime.utcfromtimestamp(start_time).strftime(SOLR_FORMAT)
        search_end_s = datetime.utcfromtimestamp(end_time).strftime(SOLR_FORMAT)

        additionalparams = {
            'fq': [
                "geo:[%s,%s TO %s,%s]" % (min_lat, min_lon, max_lat, max_lon),
                "{!frange l=0 u=0}ms(min_time_dt,max_time_dt)",
                "tile_min_time_dt:[%s TO %s] " % (search_start_s, search_end_s)
            ],
            'rows': 0,
            'facet': 'true',
            'facet.field': 'min_time_dt',
            'facet.mincount': '1',
            'facet.limit': '-1'
        }

        self._merge_kwargs(additionalparams, **kwargs)

        response = self.do_query_raw(*(search, None, None, False, None), **additionalparams)

        daysinrangeasc = sorted(
            [(datetime.strptime(a_date, SOLR_FORMAT) - datetime.utcfromtimestamp(0)).total_seconds() for a_date
             in response.facets['facet_fields']['min_time_dt'][::2]])

        return daysinrangeasc

