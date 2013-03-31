#  Copyright 2013 Open Cloud Consortium
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

from pyelasticsearch import ElasticSearch
import os
import logging
import requests
import json
import datetime

from util import get_simple_logger

class ESQuery(object):
    '''
    An object which contains a elasticsearch query and its results.
    '''
    def __init__(self, query_url, query_string, query_name, query_index, 
        query_doc_type):
        self.query_url = query_url
        self.query_string = query_string
        self.query_name = query_name
        self.query_index = query_index
        self.query_doc_type = query_doc_type
        self.es_conn = ElasticSearch(self.query_url)
        #self.logger = logging.getLogger('osdcquery')
        self.logger = get_simple_logger("DEBUG", True)
        self.status_results = None
        self.query_results = None
        self.query_datetime = None

    #because of the inconsistency between _count and _search
    def get_count_query(self):
        query = {
                    "query_string": {
                        "query": self.query_string
                    }
                }
        return query

    def get_search_query(self, fields=None, size=10):
        query = {}
        query["query"] = self.get_count_query()
        query["size"] = size

        #the docs say you can use * to get all fields, doesn't seem to work
        if fields:
            query["fields"] = fields
        return query

    def get_query_summary(self):
        return {
            "Query Name": self.query_name,
            "Search URL": '/'.join([self.query_url, self.query_index, 
                self.query_doc_type]),
            "Query": self.query_string,
            "Query Date/Time": self.query_datetime.isoformat()
            }

    def perform_query(self):
        self._search()
        return self.query_results

    def _search(self):
        count_query = self.get_count_query()
        count_result = self.es_conn.count(count_query, index=self.query_index, doc_type=self.query_doc_type)

        num_results = int(count_result["count"])
        self.logger.debug(num_results)

        search_query = self.get_search_query(size=num_results)

        self.logger.debug(search_query)
        self.query_results = self.es_conn.search(search_query, index=self.query_index, doc_type=self.query_doc_type)

        self.query_datetime = datetime.datetime.now()

        num_hits = len(self.query_results["hits"]["hits"])

        if num_results != num_hits:
            self.logger.warning("Count returned %d results, "
                "while search returned %d results" % (num_results, num_hits))

class ESQueryMetadata:
    '''
    An object that manages metadata around a query
    '''

    def __init__(self, dir_builder, fs_handler, top_dir, target_dir=None, es_query=None, cdb_query=None, manifest=None):
        self.es_query = es_query
        self.cdb_query = cdb_query
        self.manifest = manifest
        self.manifest_filename = None
        #self.dirbuild_class = dirbuild_class
        self.fs_handler = fs_handler
        self.target_dir = target_dir
        self.top_dir = top_dir
        self.data_dir = os.path.join(top_dir, "data")
        self.metadata_dir = os.path.join(top_dir, "metadata")
        #self.builder = dirbuild_class(target_dir, top_dir)
        self.builder = dir_builder
        self.logger = logging.getLogger("osdcquery")
        if es_query is not None and cdb_query is not None:
            self._build_manifest()

    def _build_manifest(self):
        if self.manifest is not None and "_id" in self.manifest:
            self.manifest = { "_id" : self.manifest["_id"]}
        else:
            self.manifest = {}

        #going to include some redundant data between summary and manifest
        #idea is that query can be performed from the manifest and results
        #compared

        self.manifest["es_query_name"] = self.es_query.query_name
        self.manifest["es_query_string"] = self.es_query.query_string
        self.manifest["es_query_url"] = self.es_query.query_url
        self.manifest["es_query_index"] = self.es_query.query_index
        self.manifest["es_query_doc_type"] = self.es_query.query_doc_type
        self.manifest["cdb_query_url"] = self.cdb_query.url
        self.manifest["cdb_status_db"] = self.cdb_query.status_db
        self.manifest["target_dir"] = self.target_dir
        self.manifest["top_dir"] = self.top_dir
        #self.manifest["dirbuild_class"] = self.builder.__name__
        #self.manifest["dirbuild_module"] = self.builder.__module__
        self.manifest["es_query_datetime"] = self.es_query.query_datetime.isoformat()

        self.manifest["results"] = {}

        if self.es_query.query_results["hits"]["total"] < 1:
            logger.info("Query returned 0 results")
        else:
            self.manifest["results"] = self.builder.associate(self.es_query, self.cdb_query)

    def _write_manifest(self):
        self.fs_handler.write_file(os.path.join(self.metadata_dir, 
            self.manifest_filename), json.dumps(self.manifest, sort_keys=True, 
            indent=4))

    def _write_summary(self):
        summary = dict(self.get_manifest_summary().items()) 
        #+ self.cdb_query.get_status_summary().items()
        #+ self.es_query.get_query_summary().items() 

        self.fs_handler.write_file(os.path.join(self.metadata_dir, "SUMMARY.json"), 
            json.dumps(summary, sort_keys=True, indent=4))

    def write_metadata(self):
        if "_id" not in self.manifest:
            self.logger.warning("Manifest not registered, using query timestamp for filename")
            epoch = datetime.datetime.utcfromtimestamp(0)
            delta = self.es_query.query_datetime - epoch
            self.manifest_filename = "".join(["MANIFEST-", str(int(delta.total_seconds())), ".json"])
        else:
            self.manifest_filename = "".join(["MANIFEST-", self.manifest["_id"], ".json"])

        self._write_manifest()
        self._write_summary()

    def read_metadata(self):
        metadata_dir = os.path.join(self.top_dir, "metadata")
        summary_filename = os.path.join(metadata_dir, "SUMMARY.json")
        summary = json.loads(self.fs_handler.read_file(summary_filename))
        manifest_filename = os.path.join(metadata_dir, summary["Manifest"])
        self.manifest = json.loads(self.fs_handler.read_file(manifest_filename))

    def _get_es_query(self):
        if self.es_query is None:
            if self.manifest is None:
                return None
            self.es_query = ESQuery(self.manifest["es_query_url"], 
                self.manifest["es_query_string"], self.manifest["es_query_name"], 
                self.manifest["es_query_index"], self.manifest["es_query_doc_type"])

    def _get_cdb_query(self, username, password):
        if self.cdb_query is None:
            if self.manifest is None:
                return None
            self.cdb_query = CDBQuery(self.manifest["cdb_query_url"], self.manifest["cdb_status_db"], 
                username, password)

    def update_query_status_symlinks(self, dangle=False, username=None, password=None):
        self._get_es_query()
        self.es_query.perform_query()
        self._get_cdb_query(username, password)
        self.cdb_query.get_status(self.es_query.query_results)
        self._build_manifest()
        self.update_symlinks(dangle)

    def get_manifest_summary(self):
        summary = {}
        errors = {}
        num_total = 0
        num_linked = 0

        for key, value in self.manifest["results"].items():
            if "reason" in value:
                error_reason = value["reason"]
                if error_reason not in errors:
                    errors[error_reason] = 0
                errors[error_reason] += 1

            if value["linked"]:
                num_linked += 1

            num_total += 1
        summary["Not Linked Reasons"] = errors
        summary["Analyses Found"] = num_total
        summary["Analyses Linked"] = num_linked
        summary["Analyses Origin Dir"] = self.target_dir
        summary["Analyses Result Dir"] = self.top_dir
        summary["Manifest"] = self.manifest_filename
        return summary

    def make_dir_structure(self):
        self.logger.info("Making directory %s" % self.top_dir)
        self.fs_handler.mkdir(self.top_dir)
        self.fs_handler.mkdir(self.data_dir)
        self.fs_handler.mkdir(self.metadata_dir)

    def update_symlinks(self, dangle=False):
        for filename in os.listdir(self.data_dir):
            full_filename = os.path.join(self.data_dir, filename)
            os.unlink(full_filename)

        self.create_symlinks(dangle, True)

    def create_symlinks(self, dangle=False, update=False):
        if len(self.manifest) < 1:
            self.logger.info("No links to be created")
        else:
            if not update:
                self.make_dir_structure()
            else:
                self.logger.info("Updating directory %s" % self.top_dir)

        num_links = 0

        results = self.manifest["results"]

        for key, value in results.items():
            if "reason" in value:
                results[key]["linked"] = False
                continue

            if "md5_ok" not in value:
                results[key]["linked"] = False
                results[key]["reason"] = "md5_not_found"
                continue

            if value["md5_ok"] == False:
                results[key]["linked"] = False
                results[key]["reason"] = "md5_not_ok"
                continue

            target = value["target"]
            link_name = value["link_name"]
            exists = self.fs_handler.exists(target)

            if not exists:
                results[key]["reason"] = "not_found_linking"
                if not dangle:
                    results[key]["linked"] = False
                    continue

            try:
                self.fs_handler.symlink(target, link_name)
                results[key]["linked"] = True
                num_links += 1
            except Exception as e:
                results[key]["reason"] = str(e)
                results[key]["linked"] = False

        return results, num_links


class CDBQuery:
    '''
    Manages direct couchdb queries, including status and registering the queries
    ''' 
    def __init__(self, url, status_db, query_db, username=None, password=None):
        self.url = url
        self.status_db = status_db
        self.query_db = query_db
        self.username = username
        self.password = password
        self.verify_ssl = False
        self.status_results = None

    def get_status(self, query_results):
        keys = {}
        keys["keys"] = []
        for entry in query_results["hits"]["hits"]:
            keys["keys"].append(entry["_id"])

        bulk_url = "".join([self.url, "/", self.status_db, 
            "/_all_docs?include_docs=true"])
        headers = {"content-type" : "application/json"}
        r = requests.post(bulk_url, data=json.dumps(keys), headers=headers, verify=self.verify_ssl)
        rjson = r.json()

        self.status_results = {}

        for row in rjson["rows"]:
            if "error" in row:
                self.status_results[row["key"]] = {}
                self.status_results[row["key"]]["reason"] = row["error"]
            else:
                self.status_results[row["key"]] = {}
                self.status_results[row["key"]]["md5_ok"] = row["doc"]["md5_ok"]

        return self.status_results

    def get_manifest(self, manifest_id):
        get_url = "/".join([self.url, self.query_db, manifest_id])
        r = requests.get(get_url, auth=(self.username, self.password), verify=self.verify_ssl)
        return r.json()

    def update_manifest(self, manifest_id, manifest):
        update_url = "/".join([self.url, self.query_db, manifest_id])
        headers = {"content-type" : "application/json"}
        r = requests.put(update_url, data=json.dumps(manifest), headers=headers, auth=(self.username, self.password), 
            verify=self.verify_ssl)
        return r.json()

    def get_status_summary(self):
        return { "Status URL": "/".join([self.url, self.status_db]) }

    def register_manifest(self, manifest):
        write_url = "/".join([self.url, self.query_db])
        headers = {"content-type" : "application/json"}
        r = requests.post(write_url, data=json.dumps(manifest), headers=headers, auth=(self.username, self.password),
            verify=self.verify_ssl)
        manifest["_id"] = r.json()["id"]
        return manifest["_id"]
