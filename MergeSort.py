import MergeSort_MapReduce
import sys

mr = MergeSort_MapReduce.MapReduce()

def mapper(partition, ob_id):
    """Assigns each object id to a partition for downstream comparison"""
    # input key: a partition
    # input value: an object id
    # output key: a partition
    # output value: an object id
    mr.emit_intermediate(partition, ob_id)

def reducer1(partition, list_of_ob_ids):
    """Merge two successive partitions for comparison by dividing their index by 2 to obtain the same index"""
    # input key: a partition
    # input values: a list of sorted object ids
    # output key: a merged partition
    # output values: a pair of lists of sorted object ids
    partition = partition // 2
    mr.emit1(partition, list_of_ob_ids)
    
def reducer2(partition, pair_of_lists):
    """Merges two sorted sub-partitions of the merged partition ascendingly"""
    # input key: the merged partition
    # input values: a pair of lists of sorted object ids
    # output key: the merged partition
    # output values: a list of sorted object ids
    sorted_ob_ids = []
    if len(pair_of_lists) == 2:
        left = pair_of_lists[0] #sub-partition on the left of the partition
        right = pair_of_lists[1] #sub-partition on the right of the partition
        
        # Use x and y to keep track of the indexing of two lists
        x = 0 
        y = 0
        
        # Sort the object ids within two sub-partitions ascendingly
        while(x < len(left) and y < len(right)):
            if(left[x] < right[y]):
                sorted_ob_ids.append(left[x])
                x += 1
            else:
                sorted_ob_ids.append(right[y])
                y += 1
        while(x < len(left)):
            sorted_ob_ids.append(left[x])
            x += 1
        while(y < len(right)):
            sorted_ob_ids.append(right[y])
            y += 1
    else:
        sorted_ob_ids.extend(pair_of_lists[0])
    mr.emit2(partition, sorted_ob_ids)

if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer1, reducer2)