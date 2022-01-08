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
            print item[0]

mapReducer = MapReduce()

def mapper(record):
    key = record["key"]
    value = record["value"]
    
    mapReducer.emitIntermediate(value, key)
        

def reducer(key, list_of_values):
    #if((len(list_of_values) == 1 and list_of_values[0] == "nr")):
    if("ns" not in list_of_values):
        mapReducer.emit((key, ""))


    
if __name__ == '__main__':
    inputData = []
    counter = 0
    Nr,Ns = map(int,raw_input().strip().split())
    for r in xrange(Nr):
        i = int(raw_input().strip())
        inputData.append(json.dumps({"key":"nr","value": i}))
    for r in xrange(Ns):
        i = int(raw_input().strip())
        inputData.append(json.dumps({"key":"ns","value": i}))
    mapReducer.execute(inputData, mapper, reducer)