#-*- coding: utf-8 -*-
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
search = Search(using=es, index="database_index")

for hit in search.scan():
    name = hit['category_name'];
    print(name.encode('utf-8'))
q = {
  "script": {
    "source": "ctx._source.category_name='life style'",
    "lang": "painless"
  },
  "query": {
    "match": {
        "category_url": "https://thegioihoinhap.vn/category/loi-song/"
    }
  }
}

es.update_by_query(body=q, doc_type='doc_name', index='database_index')
   
