import json
import sys
from collections import OrderedDict
class MapReduce:
    def __init__(self):
        self.intermediate = OrderedDict()
        self.result = []

    def emitIntermediate(self, key, value):
        self.intermediate.setdefault(key, [])
        self.intermediate[key].append(value)

    def emit(self, value):
        self.result.append(value) 

    def execute(self, data, mapper, reducer):
        for line in data:
            record = json.loads(line)
            mapper(record)

        for key in self.intermediate:
            reducer(key, self.intermediate[key])

        jenc = json.JSONEncoder()
        for item in self.result:
            print item 

mapReducer = MapReduce()

def mapper(record):
    key = record["key"]
    value = record["value"]
    
    mapReducer.emitIntermediate(value, key)

def reducer(key, list_of_values):
    
    if(key > 10 and key%2 == 1):
        mapReducer.emit(key)


if __name__ == '__main__':
    inputData = []
    counter = 0
    for line in sys.stdin:
        counter += 1
        if counter == 1:
            pass
        else:
            inputData.append(json.dumps({"key":counter,"value":int(line)}))
    mapReducer.execute(inputData, mapper, reducer)