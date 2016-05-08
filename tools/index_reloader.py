#!/usr/bin/python
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from lib.reloader import Reloader


def get_params():
    # type: () -> object
    """
    Define command line options and parses them.
    """
    parser = ArgumentParser()
    parser.add_argument("-zr", "--zero-replicas", dest="zero_replicas",
                        action="store_true", default=True,
                        help="Set to 0 the number of replicas in the destination index " \
                             "during the reload process to gain speed")
    parser.add_argument("-dr", "--disable-refresh-interval", dest="disable_refresh",
                        action="store_true", default=True,
                        help="Deactivate automatic refresh of the target index to gain speed")
    parser.add_argument("-pl", "--parallel", dest="enable_parallel",
                        action="store_true", default=True,
                        help="Enable parallel execution")

    parser.add_argument("-ht", "--host", dest="host",
                        action="store", default='localhost',
                        help="Host for ElasticSearch bulk commands")
    parser.add_argument("-p", "--port", dest="port", type=int,
                        action="store", default=9201,
                        help="Port for ElasticSearch bulk commands")
    parser.add_argument("-si", "--source-index", dest="source_index",
                        action="store", required="True",
                        help="Source Index where data will be extracted")
    parser.add_argument("-di", "--target-index", dest="target_index",
                        action="store", required="True",
                        help="Target index where data will be loaded")
    parser.add_argument("-cs", "--chunk-size", dest="size",
                        action="store", type=int, default=500,
                        help="No. of records per bulk operation or per output file.")
    parser.add_argument("-u", "--username", dest="username",
                        action="store", required=False, default=None,
                        help="Username for basic authentication for ElasticSearch bulk commands")
    parser.add_argument("-pw", "--password", dest="password",
                        action="store", required=False, default=None,
                        help="Password for basic authentication for ElasticSearch bulk commands")

    return parser.parse_args()


def main():
    """
    Entry point
    :return:
    """
    args = get_params()

    reloader = Reloader(args.host, args.port)
    reloader.reload(args.source_index, args.target_index);

    pass


if __name__ == '__main__':
    main()
