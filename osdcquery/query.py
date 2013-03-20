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

class ESQuery(object):
    '''
    An object which contains a elasticsearch query and its results.
    '''
    def __init__(self, query_url, query_string, query_name, query_index, 
        query_doc_type, status_url, status_db):
        self.query_url = query_url
        self.query_string = query_string
        self.query_name = query_name
        self.query_index = query_index
        self.query_doc_type = query_doc_type
        self.status_url = status_url
        self.status_db = status_db
        self.es_conn = ElasticSearch(self.query_url)
        self.logger = logging.getLogger('osdcquery')
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
            "Status URL": '/'.join([self.status_url, self.status_db]),
            "Query": self.query_string,
            "Query Date/Time": self.query_datetime.isoformat()
            }

    def perform_query_with_status(self):
        self._search()
        self._status()
        return self.query_results, self.status_results

    def _status(self):
        keys = {}
        keys["keys"] = []
        for entry in self.query_results["hits"]["hits"]:
            keys["keys"].append(entry["_id"])

        bulk_url = "".join([self.status_url, "/", self.status_db, 
            "/_all_docs?include_docs=true"])
        headers = {"content-type" : "application/json"}
        r = requests.post(bulk_url, data=json.dumps(keys), headers=headers)
        rjson = r.json()

        self.status_results = {}

        for row in rjson["rows"]:
            if "error" in row:
                self.status_results[row["key"]] = {}
                self.status_results[row["key"]]["error"] = row["error"]
            else:
                self.status_results[row["key"]] = {}
                self.status_results[row["key"]]["md5_ok"] = row["doc"]["md5_ok"]

    def _search(self):
        count_query = self.get_count_query()
        self.logger.debug(count_query)

        count_result = self.es_conn.count(count_query, index=self.query_index, 
            doc_type=self.query_doc_type)

        num_results = int(count_result["count"])
        self.logger.debug(num_results)

        search_query = self.get_search_query(size=num_results)

        self.logger.debug(search_query)
        self.query_results = self.es_conn.search(search_query, 
            index=self.query_index, doc_type=self.query_doc_type)

        self.query_datetime = datetime.datetime.now()

        num_hits = len(self.query_results["hits"]["hits"])

        if num_results != num_hits:
            self.logger.warning("Count returned %d results, "
                "while search returned %d results" % (num_results, num_hits))

 
class ESQueryMetadata:
    '''
    An object that manages metadata around a query
    '''

    def __init__(self, dirbuild_class, fs_handler, top_dir, target_dir=None, query=None):
        self.query = query
        self.manifest = None
        self.manifest_filename = None
        self.dirbuild_class = dirbuild_class
        self.fs_handler = fs_handler
        self.target_dir = target_dir
        self.top_dir = top_dir
        self.data_dir = os.path.join(top_dir, "data")
        self.metadata_dir = os.path.join(top_dir, "metadata")
        self.builder = dirbuild_class(target_dir, top_dir)
        self.logger = logging.getLogger("osdcquery")
        if query is not None:
            self._build_manifest()

    def _build_manifest(self):
        self.manifest = {}

        #going to include some redundant data between summary and manifest
        #idea is that query can be performed from the manifest and results
        #compared

        self.manifest["query_name"] = self.query.query_name
        self.manifest["query_string"] = self.query.query_string
        self.manifest["query_url"] = self.query.query_url
        self.manifest["query_index"] = self.query.query_index
        self.manifest["query_doc_type"] = self.query.query_doc_type
        self.manifest["status_url"] = self.query.status_url
        self.manifest["status_db"] = self.query.status_db
        self.manifest["target_dir"] = self.target_dir
        self.manifest["top_dir"] = self.top_dir
        self.manifest["dirbuild_class"] = self.dirbuild_class.__name__
        self.manifest["dirbuild_module"] = self.dirbuild_class.__module__
        self.manifest["query_datetime"] = self.query.query_datetime.isoformat()

        self.manifest["results"] = {}

        if self.query.query_results["hits"]["total"] < 1:
            logger.info("Query returned 0 results")
        else:
            self.manifest["results"] = self.builder.associate(self.query)

        epoch = datetime.datetime.utcfromtimestamp(0)
        delta = self.query.query_datetime - epoch
        self.manifest_filename = "".join(['MANIFEST-', 
            str(int(delta.total_seconds())), '.json'])

    def _write_manifest(self):
        self.fs_handler.write_file(os.path.join(self.metadata_dir, 
            self.manifest_filename), json.dumps(self.manifest, sort_keys=True, 
            indent=4))

    def _write_summary(self):
        summary = dict(self.get_manifest_summary().items() + 
            self.query.get_query_summary().items())

        self.fs_handler.write_file(os.path.join(self.metadata_dir, "SUMMARY.json"), 
            json.dumps(summary, sort_keys=True, indent=4))

    def write_metadata(self):
        self._write_manifest()
        self._write_summary()

    def read_metadata(self):
        metadata_dir = os.path.join(self.top_dir, "metadata")
        summary_filename = os.path.join(metadata_dir, "SUMMARY.json")
        summary = json.loads(self.fs_handler.read_file(summary_filename))
        manifest_filename = os.path.join(metadata_dir, summary["Manifest"])
        self.manifest = json.loads(self.fs_handler.read_file(manifest_filename))

    def _get_query(self):
        if self.query is None:
            if self.manifest is None:
                return None
            self.query = ESQuery(self.manifest["query_url"], 
                self.manifest["query_string"], self.manifest["query_name"], 
                self.manifest["query_index"], self.manifest["query_doc_type"], 
                self.manifest["status_url"], self.manifest["status_db"])


    def update_query_and_symlinks(self, dangle=False):
        self._get_query()
        self.query.perform_query_with_status()
        self._build_manifest()
        self.update_symlinks(dangle)

    def get_manifest_summary(self):
        summary = {}
        errors = {}
        num_total = 0
        num_linked = 0

        for key, value in self.manifest["results"].items():
            if "error" in value:
                error_reason = value["error"]
                if error_reason not in errors:
                    errors[error_reason] = 0
                errors[error_reason] += 1

            if value["linked"]:
                num_linked += 1

            num_total += 1
        summary["Errors"] = errors
        summary["Analyses Found"] = num_total
        summary["Analyses Linked"] = num_linked
        summary["Analyses Origin Dir"] = self.target_dir
        summary["Analyses Result Dir"] = self.top_dir
        summary["Manifest"] = self.manifest_filename
        return summary


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
                self.logger.info("Making directory %s" % self.top_dir)
                self.fs_handler.mkdir(self.top_dir)
                self.fs_handler.mkdir(self.data_dir)
                self.fs_handler.mkdir(self.metadata_dir)
            else:
                self.logger.info("Updating directory %s" % self.top_dir)

        num_links = 0

        results = self.manifest["results"]

        for key, value in results.items():
            if "error" in value:
                results[key]["linked"] = False
                continue

            if "md5_ok" not in value:
                results[key]["linked"] = False
                results[key]["error"] = "md5_not_found"
                continue

            if value["md5_ok"] == False:
                results[key]["linked"] = False
                results[key]["error"] = "md5_not_ok"
                continue

            target = value["target"]
            link_name = value["link_name"]
            exists = self.fs_handler.exists(target)

            if not exists:
                results[key]["error"] = "not_found_linking"
                if not dangle:
                    results[key]["linked"] = False
                    continue

            try:
                self.fs_handler.symlink(target, link_name)
                results[key]["linked"] = True
                num_links += 1
            except Exception as e:
                results[key]["error"] = str(e)
                results[key]["linked"] = False

        return results, num_links

