# A simple client to sync data from solr to elastic.
# The parameters are configurable via a config file.
# An example config file format:
"""
[solr]
    url=http://user:pass@host/solr/core
    query=*:*
    fl=*
    start=0
    rows=100
    # rows is page size, the client internally paginates over all docs matched to query
[elastic]
    cluster=http://localhost:9400/
    index=weapons
    type=testdoc
[import]
    log_delay=2000
"""
# Author : Thamme Gowda
# Date   : March 04, 2016

from argparse import ArgumentParser
from configparser import ConfigParser
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import requests
import time
import re
from datetime import datetime


current_milli_time = lambda: int(round(time.time() * 1000))  # replacement for System.currentMillis() ;)

class Solr(object):
    '''
    Solr client for querying docs
    '''
    def __init__(self, solr_url):
        self.query_url = solr_url + ('' if solr_url.endswith('/') else '/' ) + 'select'

    def query_iterator(self, query='*:*', start=0, rows=20, **kwargs):
        '''
        Queries solr server and returns Solr response  as dictionary
        returns None on failure, iterator of results on success
        '''
        payload = {
                   'q': query,
                    'wt': 'python',
                    'rows': rows
                   }

        if kwargs:
            for key in kwargs:
                payload[key] = kwargs.get(key)

        total = start + 1
        while start < total:
            payload['start'] = start
            print('start = %s, total= %s' % (start, total))
            resp = requests.get(self.query_url, params=payload)
            if not resp:
                print('no response from solr server!')
                break

            if resp.status_code == 200:
                resp = eval(resp.text)
                total = resp['response']['numFound']
                for doc in resp['response']['docs']:
                    start += 1
                    yield doc
            else:
                print(resp)
                print('Oops! Some thing went wrong while querying solr')
                print('Solr query params = %s', payload)
                break

class Solr2Elastic(object):
    """
    A client for Importing data from solr to elastic search
    """

    def __init__(self, config):
        self.config = config
        self.solr = Solr(config.get('solr', 'url'))
        hosts = [config.get('elastic','cluster')]
        self.elastic = Elasticsearch(hosts)

    def sync(self, transform_func, batch=200):
        """
        Syncs data from solr to elastic
        :param transform_func: function to transform solr document to elastic document
        :param batch: bulk request size
        :return: num docs processed
        """
        qry = self.config.get('solr','query')
        start = int(self.config.get('solr', 'start'))
        rows = int(self.config.get('solr', 'rows'))
        fl = self.config.get('solr', 'fl')
        docs = self.solr.query_iterator(query=qry, start=start, rows=rows, fl=fl)
        index = self.config.get('elastic', 'index')
        type = self.config.get('elastic','type')

        buffer = []

        st = current_milli_time()
        progress_delay = int(self.config.get('import', 'log_delay'))
        count = 0
        num_batches = 0
        for solr_doc in docs:
            count += 1
            (id, es_doc) = transform_func(solr_doc)
            es_doc['imported_at'] = datetime.now()
            buffer.append({
                "_index": index,
                "_type": type,
                "_id": id,
                "_source":es_doc
            })
            self.elastic.index(index, type, es_doc, id=id)
            if len(buffer) > batch:
                helpers.bulk(self.elastic, buffer)
                del buffer[:]
                num_batches += 1

            if current_milli_time() - st > progress_delay:
                print("%d ,Batch:%d LastDoc:%s" %(count, num_batches, id))
                st = current_milli_time()

        if len(buffer) > 0:
            helpers.bulk(self.elastic, buffer)
        print("Done: %d" % count)
        return count

def transform_edr2cdr(doc):
    """
    This function transforms EDR(aka Solr) doc to CDR (aka Elastic)
    :param doc: Solr document
    :return: (id, elastic_doc)
    """
    id = doc['id']
    res = {}
    metadata = {} # since ES can take nested json, we club all metadata keys from solr
    for key, val in doc.items():
        if key in Config.mapping:
            res[Config.mapping[key]] = val
            continue
        match = Config.md_pattern.match(key)
        if match:
            metadata[match.group(1)] = val
        elif key not in Config.removals:
            res[key] = val
    res['extracted_metadata'] = metadata
    res['obj_stored_url'] = id.replace(Config.dump_path, Config.mount_point)
    res.update(Config.additions)
    # res['crawl_data'] = None FIXME: these are not found in solr
    # res['timestamp'] = None
    # res['obj_parent] = None
    return (id, res)


class Config(object):
    """
    transformation config
    """

    additions = {
        'crawler':'Nutch-1.12-SNAPSHOT',
        'team': 'NASA_JPL',
        'version':2.0
    }
    mapping = {
        'id': 'obj_id',
        'outlinks': 'obj_outlinks',
        'outpaths': 'obj_outurls',
        'contentType': 'content_type',
        'content': 'extracted_text',
        'url': 'obj_original_url'
    }
    removals = {}
    md_pattern = re.compile(r"(.*)_(ts?|ss?|ds?|bs?|fs?|is?|l?)_md")
    dump_path = "file:/data2/USCWeaponsStatsGathering/nutch/full_dump/"
    mount_point = "http://imagecat.dyndns.org/weapons/alldata/"

if __name__ == '__main__':
    parser = ArgumentParser(prog="Import EDR docs to CDR",
                            description="This program copies data from EDR(Solr) to CDR(Elastic) and was developed at NASA JPL to copy index from solr to Elastic Search")
    parser.add_argument('-cfg','--config', help='Configuration File', required=True)
    args = vars(parser.parse_args())
    config = ConfigParser()
    config.read(args['config'])
    s2e = Solr2Elastic(config)
    count = s2e.sync(transform_edr2cdr)
    print("Total docs imported=%d"%count)
