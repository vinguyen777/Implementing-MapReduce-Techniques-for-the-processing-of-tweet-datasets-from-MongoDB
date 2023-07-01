import json

class MapReduce:
    def __init__(self):
        # Create empty dictionaries to store results of the mapper and reducers after every iteration
        self.intermediate = {}
        self.result1 = {}
        self.result2 = {}
        
    def emit_intermediate(self, partition, ob_id):
        # key: a partition
        # value: an object id
        self.intermediate.setdefault(partition, [])
        self.intermediate[partition].append(ob_id)
        
    def emit1(self, partition, list_of_ob_ids):
        # input key: a partition
        # input values: a list of sorted object ids
        # output key: a merged partition
        # output values: a pair of lists of sorted object ids
        self.result1.setdefault(partition, [])
        self.result1[partition].append(list_of_ob_ids)

    def emit2(self, partition, sorted_ob_ids):
        # key: a partition
        # values: a list of sorted object ids
        self.result2[partition] = sorted_ob_ids

    def execute(self, data, mapper, reducer1, reducer2): 

        for partition, line in enumerate(data):
            ob_id = json.loads(line)[0]
            # Map each object id to a partition for downstream comparision
            mapper(partition, ob_id)
        
        # Recursively execute two reducers until no pairs remain in the final result of the iteration
        while True:
            self.result1.clear()
            self.result2.clear()
            
            # Merge two successive partitions into one for comparison
            for partition, list_of_ob_ids in self.intermediate.items():
                reducer1(partition, list_of_ob_ids)
            
            # Sort object ids among two sorted sub-partitions of a partition ascendingly after merging
            for partition, sorted_ob_ids in self.result1.items():
                reducer2(partition, sorted_ob_ids)
            
            # Overwrite the intermediate result with the final sorting of every iteration
            self.intermediate = self.result2.copy()
            
            # If all pairs have been merged into one which suggests the correct ascending order of object ids, exit the loop
            if len(list(self.result2.values())) == 1:
                break  
        
        #jenc = json.JSONEncoder(encoding='latin-1')
        jenc = json.JSONEncoder()
        for partition, sorted_ob_ids in self.result2.items():
            for ob_id in sorted_ob_ids:
                print(jenc.encode(ob_id))