#!/usr/bin/python
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, parallel_bulk, streaming_bulk
from itertools import islice
import re
from random import randint
import datetime

def get_params():
    # type: () -> object
    """
    Define command line options and parses them.
    """
    parser = ArgumentParser()

    parser.add_argument("--source-host", dest="source_host", default='localhost',
                        help="Host for Source ElasticSearch bulk commands")
    parser.add_argument("--source-port", dest="source_port", type=int, default=9200,
                        help="Port for Source ElasticSearch bulk commands")
    parser.add_argument("--target-host", dest="target_host", default='localhost',
                        help="Host for Target ElasticSearch bulk commands")
    parser.add_argument("--target-port", dest="target_port", type=int, default=9200,
                        help="Port for Target ElasticSearch bulk commands")
    parser.add_argument("-si", "--source-index", dest="source_index", required=True,
                        help="Source Index where data will be extracted")
    parser.add_argument("-ti", "--target-index", dest="target_index", required=True,
                        help="Target Index where data will be stored")

    parser.add_argument("-l", "--limit", dest="limit", type=int, required=False,
                        help="Maximum number of documents to store")

    parser.add_argument("--target-routing", dest="target_routing", required=False,
                        help="Target routing, expected is a single value or a comma separated "\
                             "list of values, in the later case, a random will be pick per document")

    parser.add_argument("-u", "--username", dest="username", required=False, default=None,
                        help="Username for basic authentication for ElasticSearch bulk commands")

    parser.add_argument("-pw", "--password", dest="password", required=False, default=None,
                        help="Password for basic authentication for ElasticSearch bulk commands")

    parser.add_argument("-tu", "--target-username", dest="target_username", required=False, default=None,
                        help="Username for basic authentication for ElasticSearch bulk commands")

    parser.add_argument("-tpw", "--target-password", dest="target_password", required=False, default=None,
                        help="Password for basic authentication for ElasticSearch bulk commands")

    parser.add_argument("-q", "--query", dest="query",
                        required=False, default=None,
                        help="Query to narrow documents in source index")

    parser.add_argument("--num-threads", dest="num_threads",
                        required=False, default=1, type=int,
                        help="Number of threads used for indexing")

    parser.add_argument("--only-metadata", dest="only_metadata",
                        action="store_true", required=False, default=False,
                        help="Only create target index if it doesn't exists")

    return parser.parse_args()



def get_es_client(host="127.0.0.1", port=9200, user='', password=''):

    http_auth = None
    if user is not None:
        http_auth = (user, password)

    return Elasticsearch([{"host": host, "port": port}],
                         http_auth=http_auth
    )



def assign_routing(hit, routing):
    
    if "_routing" in hit:
        del hit["_routing"]
    
    if routing is None:
        return

    regex = '(([^,]+),?)+'

    if re.search(regex, routing) is None:
        return


    routing_values = routing.split(',')

    rand_pos = randint(0, len(routing_values) - 1)

    hit["_routing"] = routing_values[rand_pos]


def process_hit(hit):
    #hit["_source"]["publication_id"] = long(hit["_source"]['id']['frontiers'])
    #hit["_id"] = randint(0, 100000000)
    pass


def process_scan_response(scan_result_iter, new_index, routing=None):
    for hit in scan_result_iter:
        hit["_index"] = new_index
        assign_routing(hit, routing)
        process_hit(hit)
        yield hit


def realod_es(source_client, source_index, source_query, target_client, target_index, limit=None, routing=None,
              num_threads=1):
    scan_result_iter = scan(source_client, index=source_index, query=source_query)

    if limit is not None:
        scan_result_iter = islice(scan_result_iter, 0, limit)

    gen = None
    if num_threads <= 1:
        gen = streaming_bulk(target_client, process_scan_response(scan_result_iter, target_index, routing),
                             raise_on_error=False)
    else:
        gen = parallel_bulk(target_client, process_scan_response(scan_result_iter, target_index, routing),
                            raise_on_error=False, thread_count=num_threads)

    #TODO: process failed items
    success = 0
    failed = 0
    failed_ids = []
    for ok, item in gen:
        # go through request-reponse pairs and detect failures
        if not ok:
            failed += 1
            failed_ids.append(item['_id'])
        else:
            success += 1

    return success, failed, failed_ids


def get_query_or_load_from_file(query):
    if query is not None and query.startswith('@'):
        with open(query[1:]) as f: query = f.read()

    return query

def create_index_if_not_exists(target_client, target_index_name, source_client, source_index_name):
    if not target_client.indices.exists(index = target_index_name):
        source_index = source_client.indices.get(index=source_index_name).values()[0]
        #clear aliases cause otherwise alias would point to both source and target
        del source_index['aliases']
        target_client.indices.create(index=target_index_name, body=source_index)


def main():
    args = get_params()

    source_es_client = get_es_client(args.source_host, args.source_port, args.username, args.password)
    target_es_client = get_es_client(args.target_host, args.target_port, args.target_username, args.target_password)
    
    start_time = datetime.datetime.now()
    try:
   
        create_index_if_not_exists(target_client=target_es_client, target_index_name=args.target_index,
                                   source_client=source_es_client, source_index_name=args.source_index)

        if args.only_metadata:
            print("--only-metadata option used, no data will be migrated")
            return

        source_query = args.query  # "@/tmp/query"#'{"query" : {"match" : {"is_internal" : true}}}'

        source_query = get_query_or_load_from_file(source_query)

        results = realod_es(source_es_client, args.source_index, source_query, target_es_client, args.target_index,
                            args.limit, args.target_routing, args.num_threads)
        print(results)
    finally:
        print('Execution times was: {0}'.format(datetime.datetime.now() - start_time))


if __name__ == '__main__':
    main()

