import requests
import elasticsearch

class Plugin(object):
    def __init__(self, configuration):

        self.client = elasticsearch.Elasticsearch([
            {
                'host': configuration["host"],
                'port': int(configuration["port"])
            }
        ])

        response = requests.get('http://{}:{}'.format(configuration["host"], configuration["port"]))
        #print(response.content)

        self.index = configuration["index"]
        if "mappings" in configuration:
            self.mappings = configuration["mappings"]
            self.client.indices.create(index=self.index, ignore=400, body= { "mappings" : self.mappings })
        else:
            self.client.indices.create(index=self.index, ignore=400)            

    def process(self, profile, state, vertex):
        self.client.index(index=self.index, doc_type="vertex", body=vertex)
        return { "properties": None, "neighbors" : [] }
