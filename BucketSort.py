import BucketSort_MapReduce
import sys

mr = BucketSort_MapReduce.MapReduce()

def mapper(all_ob_ids, max_ob_id):
    """Assigns each object id to its relevant bucket"""
    # output key: len(all_ob_ids) * (ob_id // max_ob_id) (indicating the index of the bucket in which object id belongs within 10000 buckets
    # output value: an object id
    for ob_id in all_ob_ids:
        mr.emit_intermediate(len(all_ob_ids) * (ob_id // max_ob_id), ob_id)

def reducer1(bucket, list_of_ob_ids):
    """Sorts the object ids within a bucket ascendingly"""
    # input key: a bucket
    # input values: a list of (unsorted) object ids
    # output key: a bucket
    # output values: a list of sorted object ids
    for i in range(1, len(list_of_ob_ids)):
        ahead = list_of_ob_ids[i]
        j = i - 1
        while j >= 0 and list_of_ob_ids[j] > ahead:
            list_of_ob_ids[j + 1] = list_of_ob_ids[j]
            j -= 1
        list_of_ob_ids[j + 1] = ahead
    mr.emit1(bucket, list_of_ob_ids)

def reducer2(_, lists_of_ob_ids):
    """Obtains the total ordering as the buckets are ordered in their own rights"""
    # discard the key; it is just None
    # each item of lists_of_ob_ids is a list of sorted object ids
    for list_of_ob_ids in lists_of_ob_ids:
        mr.emit2(None, list_of_ob_ids)
        
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer1, reducer2)