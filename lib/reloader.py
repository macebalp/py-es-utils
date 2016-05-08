# -*- coding: utf-8 -*-

from multiprocessing.dummy import Pool as ThreadPool
from random import randint
from time import sleep
from elasticsearch import Elasticsearch
from elasticsearch.helpers import reindex

class Reloader:

    def __init__(self, host, port):
        self.host = host
        self.port = port;
        self.es_client = Elasticsearch([{ "host" : host, "port" : port }])


    def reload(self, source_index, target_index):
        """

        :return:
        """
        num_threads=5

        urls = ["thread: {0}".format(i) for i in range(num_threads) ]

        pool = ThreadPool(num_threads)

        # Open the urls in their own threads
        # and return the results
        results = pool.map(self.__reload_query, urls)

        # close the pool and wait for the work to finish
        pool.close()
        pool.join()


    def __reload_query(self, query):
        """

        :return:
        """
        print "Entering thread: %s" % query
        sleep(randint(5, 10))
        pass