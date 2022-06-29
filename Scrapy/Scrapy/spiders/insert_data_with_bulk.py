# # -*- coding: utf-8 -*-
# import json
# from elasticsearch import Elasticsearch
# from elasticsearch import helpers
# from const import (
#     SAMPLE_DATA_DIR,
#     ES_HOST,
#     ES_PORT
# )

# es = Elasticsearch(host= ES_HOST, port= ES_PORT)
# es = Elasticsearch()

# def load_jsondata():
#     with open(SAMPLE_DATA_DIR) as f:
#         return json.load(f)

# def insert_data_by_bulk(data):
#     res = helpers.bulk(es, data)
#     print(res)


# if __name__ == "__main__":
#     demo_data_2 = load_jsondata()
#     insert_data_by_bulk(demo_data_2)