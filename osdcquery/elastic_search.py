#!/usr/bin/python

''' Query object for Elastic Search queries '''

import urllib2
import json


class EsQuery(object):
    ''' Queries an elastic search engine instance for file metadata'''

    def __init__(self, url, fields):
        ''' Takes the url of the elasticquery service e.g:
        localhost:9200/cghub/analysis/_search'''

        self.url = url

        # ["files","analysis_id","disease_abbr"]
        self.fields = fields

    def run_query(self, query_string):
        ''' There is no clear set of parameters or options for the
        query as it is updated when the metadata is consumed and sent
        to a database of values.  We will asssume all errors are handled
        by the keyservice.

        Returns list of metdata for files
        '''

        json_data = json.dumps(
        {
            "fields": self.fields,
            "query": {
                "query_string": {
                     # "disease_abbr:OV AND center_name:BCM"
                     "query": query_string
                }
            }
        })

        #curl localhost:9200/cghub/analysis/_search -d \
        # @es_queries/query_string.json
        req = urllib2.Request(self.url, json_data)
        response = urllib2.urlopen(req)
        result = response.read()

        hits = json.loads(result)

        if "hits" in hits and "hits" in hits["hits"]:
            return [{field: file_info["fields"][field]
                for field in self.fields}
                for file_info in hits["hits"]["hits"]]

        else:
            return []
