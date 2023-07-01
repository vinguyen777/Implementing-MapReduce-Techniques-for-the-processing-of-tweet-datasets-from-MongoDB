import json

class MapReduce:
    def __init__(self):
        # Create 10000 empty buckets
        self.buckets = [[] for i in range(10000)]
        # Create an empty list to store the final result
        self.result = []

    def emit_intermediate(self, bucket, ob_id):
        # key: a bucket
        # value: an object id
        if bucket != len(self.buckets):
            self.buckets[bucket].append(ob_id)
        else:
            self.buckets[len(self.buckets) - 1].append(ob_id)
            
    def emit1(self, bucket, list_of_ob_ids):
        # key: a bucket
        # values: a list of sorted object ids
        self.buckets[bucket] = list_of_ob_ids

    def emit2(self, _, list_of_ob_ids):
        # discard the key; it is just None
        # each item of lists_of_ob_ids is a list of sorted object ids
        self.result.extend(list_of_ob_ids) 

    def execute(self, data, mapper, reducer1, reducer2):
        all_ob_ids = [json.loads(line)[0] for line in data]
        max_ob_id = max(all_ob_ids)
        
        # Assign each object id to its relevant bucket
        mapper(all_ob_ids, max_ob_id)
            
        # Sort the object ids within every bucket ascendingly
        for bucket, list_of_ob_ids in enumerate(self.buckets):
            reducer1(bucket, list_of_ob_ids)
            
        # Obtain the total ordering as the buckets are ordered in their own rights
        reducer2(None, self.buckets)

        jenc = json.JSONEncoder()
        for item in self.result:
            print(jenc.encode(item))