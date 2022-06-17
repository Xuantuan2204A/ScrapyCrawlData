# -*- coding: utf-8 -*-
__author__ = 'XUANTUAN'

from elasticsearch import Elasticsearch


class GetSpider():
    def __init__(self, *args, **kwargs):
        self.es_doc_type = "fb"
        self.es_index = "database_scrapy"
        self.es = Elasticsearch("http://127.0.0.1:9200")

    def startGet(self):
        page = 1
        limit = 10
        while True:
            print("\n------------------------")
            print("#Page: " + str(page) + "\n")

            begin = ((page-1)*limit)
            # self.es.indices.refresh(index=self.es_index)
            docs = {
                "_source": ["category_url","createdate","category_name"],
                "query": {
                    "bool" : {
                        "filter" : {
                            "bool" : {
                                "must" : [
                                    {"term" : { "category_url" : "b1d49da1b7fc9eecce0bce89efe5dda7" }},
                                ]
                            }
                        }
                    }
                },
                # "query": {
                #    "match_all":{}
                # },
                "size": limit,
                "from": begin
            }
            res = self.es.search(index=self.es_index, body=docs)
            if(len(res['hits']['hits']) == 0):
                break
        
            for hit in res['hits']['hits']:
                id = hit["_id"]
                print(id) 
                self.es.update(index=self.es_index , id=id, body=dict(
                    {'doc': {'category_name': "Agriculture"}}))
            page = page + 1

getdata = GetSpider()
getdata.startGet()
