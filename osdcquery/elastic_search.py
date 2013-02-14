#!/usr/bin/python

''' Query object for Elastic Search queries.  Implements interface run_query'''

import json
import logging
import re
import urllib2


class EsQuery(object):
    ''' Queries an elastic search engine instance for file metadata'''

    def __init__(self, url, fields, non_disease_dir):
        ''' Takes the url of the elasticquery service e.g:
        localhost:9200/cghub/analysis/_search'''

        self.logger = logging.getLogger('osdcquery')
        self.logger.debug("Logger in elastic_search")

        self.url = "".join([url.rstrip('/'), '/'])

        #going to assume everything up to the first slash in the url is the
        #host (no non-root urls, not sure if this is a problem?)
        #might want to separate out the host and index into two arguments?
        url_re = '(?P<host>(http.*://)?[^:/ ]+(:[0-9]+)?).*'
        m = re.search(url_re, url)
        self.host = ''.join([m.group('host'), '/'])
        self.logger.debug(self.host)

        # ["files","analysis_id","disease_abbr"]
        self.fields = fields
        self.non_disease_dir = non_disease_dir

    def get_json_query(self, query_string):
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
        return json_data

    def run_query(self, query_string, size=10, timeout='1m'):
        ''' public facing function...polymorphism '''
        return self._run_scroll_query(query_string, size, timeout)

    def _run_scroll_query(self, query_string, size=10, timeout='1m'):
        ''' The suggested way by elasticsearch to get large results
        is to use the scroll functionality, this will scroll until the end.
        Note that with scan the actual number of results returned is the
        size times the number of shards.
        '''
        req_url = ''.join([self.url, '_search?search_type=scan&scroll=', timeout,
            '&size=%d' % size])

        self.logger.debug("request url %s", req_url)

        post_data = self.get_json_query(query_string)

        self.logger.debug("data %s", post_data)

        req = urllib2.Request(req_url, post_data)

        response = urllib2.urlopen(req)
        result = response.read()

        self.logger.debug("Initial response %s", result)

        scroll_response = json.loads(result)
        scroll_id = scroll_response['_scroll_id']
        total_hits = scroll_response['hits']['total']

        full_results = []

        if total_hits == 0:
            return full_results

        #Because it makes me feel better, going to limit this loop to the
        #number of total hits, but I think the theoretical max is
        #total_hits / size
        iter_max = total_hits
        iter_curr = 0
        while True:
            req = urllib2.Request(''.join([self.host, '_search/scroll?scroll=',
                timeout]), scroll_id)
            response = urllib2.urlopen(req)
            result = response.read()
            result_json = json.loads(result)

            self.logger.debug(result_json)

            num_hits = len(result_json['hits']['hits'])
            if num_hits == 0:
                break

            scroll_id = result_json['_scroll_id']

            if 'hits' in result_json and 'hits' in result_json['hits']:
                full_results.extend([{field: file_info['fields'][field]
                    if field in file_info['fields'] else self.non_disease_dir
                         for field in self.fields}
                        for file_info in result_json['hits']['hits']])

            iter_curr = iter_curr + 1
            if iter_curr > iter_max:
                break

        #may want to check the num of results returning versus the reported
        #total
        return full_results

    def _run_flat_query(self, query_string, size=10):
        ''' There is no clear set of parameters or options for the
        query as it is updated when the metadata is consumed and sent
        to a database of values.  We will asssume all errors are handled
        by the keyservice.

        Returns list of metadata for files '''

        #curl localhost:9200/cghub/analysis/_search -d \
        # @es_queries/query_string.json
        req = urllib2.Request(''.join([self.url, '?size=%d' % size]),
            self.get_json_query(query_string))

        response = urllib2.urlopen(req)
        result = response.read()

        hits = json.loads(result)

        if "hits" in hits and "hits" in hits["hits"]:
            return [{field: file_info["fields"][field]
                for field in self.fields}
                for file_info in hits["hits"]["hits"]]

        else:
            return []


class FieldList(object):
    '''Shows what fields can be searched over '''

    def __init__(self, url):
        self.url = url

    def attributes(self):
        ''' look at base url .../_mapping '''

        req = urllib2.Request('/'.join([self.url, '_mapping']))
        response = urllib2.urlopen(req)
        result = response.read()
        properties = json.loads(result)
        name = self.url.rstrip('/').split('/')[-1]
        properties = properties[name]
        return properties["properties"].keys()
