from webservice.NexusHandler import nexus_handler
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler
from webservice.webmodel import NexusResults


@nexus_handler
class TileFetch(NexusCalcHandler):
    name = 'Debug Tile Fetch'
    path = '/tilefetch'
    description = 'Debug handler to fetch tile(s) by id or by query'
    params = {}
    singleton = True
    suppress = True

    def calc(self, request, **kwargs):
        pass


class TileFetchResults(NexusResults):
    pass