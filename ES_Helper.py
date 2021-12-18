

from elasticsearch import Elasticsearch



class ESHelper:
    def __init__(self, serverConfig="data"):
        self.servers = ELASTICSEARCH[serverConfig]
        self.client = None
        if not self.client:
            self.client = Elasticsearch(hosts=self.servers)
    
    def create_index(self, index):
        try:
            res = self.client.indices.create(index=index)
            return res["acknowledged"]
        except Exception as e:
            if e.error == "resource_already_exists_exception":
                return (False, "Index Exists")
            else:
                return (False, f"Some issue has been encountered - {str(e)}")

    def index_exists(self, index):
        return self.client.indices.exists(index=index)

    def delete_exists(self, index):
        try:
            res = self.client.indices.delete(index=index)
            return res["acknowledged"]
        except Exception as e:
            if e.error == "index_not_found_exception":
                return (False, "Index Does Not Exists")
            else:
                return (False, f"Some issue has been encountered - {str(e)}")
        return res["acknowledged"]

    def upsert_document(self, index, doc_type, credential, doc):
        res = self.client.index(index=index, doc_type=doc_type, id=credential, body=doc)
        
        if res:
            if res['result'] == 'created':
                return True
            elif res['result'] == 'updated':
                return True
        return False

    def partial_update_document(self, index, doc_type, credential, body):
        doc = {"doc": body}
        res = self.client.update(index=index, doc_type=doc_type, id=credential, body=doc)
        if res:
            if res['result'] == 'updated':
                return True
        return False

    def get_document(self, index, credential):
        try:
            res = self.client.get(index=index, id=credential)
            return res['_source']
        except Exception as e:
            return None

    def refresh_index(self, index):
        self.client.indices.refresh(index=index)

    def search_document(self, index, query):
        try:
            res = self.client.search(index=index, body=query)
            if res:
                count = res['hits']['total']['value']
                results = []
                for item in res['hits']['hits']:
                    results.append(item['_source'])
                return (count, results)
            else:
                return (0, None)
            return res
        except Exception as e:
            return (0, None)
