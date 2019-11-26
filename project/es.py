from elasticsearch import Elasticsearch


def elastic_search():
    return Elasticsearch([{'host': 'localhost', 'port': 9200}])


def send_loc_to_es(data):
    es = elastic_search()
    if not es.indices.exists(index="location"):
        datatype = {
            "mappings": {
                "request-info": {
                    "properties": {
                        "text": {
                          "type": "text"
                        },
                        "location": {
                            "type": "geo_point"
                        },
                        "analysis": {
                            "type": "keyword"
                        }
                    }
                }
            }
        }
        es.indices.create(index="location", body=datatype)
    es.index(index="location", doc_type="request-info", body=data)
