import json
from collections import OrderedDict

class MapReduce:
    def __init__(self):
        self.intermediate1 = OrderedDict()
        self.result1 = OrderedDict()
        self.intermediate2 = OrderedDict()
        self.result2 = OrderedDict()
        self.intermediate3 = OrderedDict()
        self.result3 = OrderedDict()

    def emit_intermediate1(self, word, document_name, count):
        # output key: <word, document name>
        # output value: count (= 1 for each occurrence of the word)
        key = (word, document_name)
        self.intermediate1.setdefault(key, [])
        self.intermediate1[key].append(count)
            
    def emit1(self, word, document_name, word_frequency):
        # output key: <word, document name>
        # output value: frequency of the word in the same document
        key = (word, document_name)
        self.result1[key] = word_frequency
        
    def emit_intermediate2(self, document_name, word, word_frequency):
        # output key: document name
        # output value: <word, frequency of the word in the same document>
        values = (word, word_frequency)
        self.intermediate2.setdefault(document_name, [])
        self.intermediate2[document_name].append(values)

    def emit2(self, word, document_name, word_frequency, total_count_within_document):
        # output key: <word, document name>
        # output value: <frequency of the word in the same document, total number of words in this document>
        key = (word, document_name)
        values = (word_frequency, total_count_within_document)
        self.result2[key] = values
        
    def emit_intermediate3(self, word, document_name, word_frequency, total_count_within_document, count):
        # output key: word
        # output value: <document name, frequency of the word in the same document, total number of words in this document, count (= 1)>
        values = (document_name, word_frequency, total_count_within_document, 1)
        self.intermediate3.setdefault(word, [])
        self.intermediate3[word].append(values)

    def emit3(self, word, document_name, tf_idf):
        # output key: <word, document name>
        # output value: TF-IDF
        key = (word, document_name)
        self.result3[key] = tf_idf

    def execute(self, data, mapper1, reducer1, mapper2, reducer2, mapper3, reducer3):
        
        mapper1(data)
        
        # Derive TF Numerator
        for (word, document_name), counts in self.intermediate1.items():
            reducer1(word, document_name, counts)
            
        for (word, document_name), word_frequency in self.result1.items():
            mapper2(word, document_name, word_frequency)
        
        # Derive TF Denominator
        for document_name, word_and_frequency in self.intermediate2.items():
            frequency_counts = [tup[1] for tup in word_and_frequency]
            for tup in word_and_frequency:
                reducer2(tup[0], document_name, tup[1], frequency_counts)
                
        for (word, document_name), (word_frequency, total_count_within_document) in self.result2.items():
            mapper3(word, document_name, word_frequency, total_count_within_document)
        
        # Derive TF-IDF
        document_names = [f"Tweet {i + 1}" for i in range(10000)]
        for word, statistics in self.intermediate3.items():
            counts = [statistic[3] for statistic in statistics]
            for statistic in statistics:
                reducer3(word, statistic[0], statistic[1], statistic[2], counts)

        jenc = json.JSONEncoder()
        for (word, document_name), tf_idf in self.result3.items():
            if word == "health":
                print(jenc.encode(word + " - " + document_name + " - " + str(tf_idf)))