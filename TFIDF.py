import TFIDF_MapReduce
import sys
import math
import json

mr = TFIDF_MapReduce.MapReduce()
total_number_of_tweets = 0 # for TF-IDF computation 

def mapper1(data):
    # output key: <word, document name>
    # output value: 1 (for each occurrence of the word)
    
    global total_number_of_tweets
    
    for index, line in enumerate(data):
        text = json.loads(line)[0]
        if len(text) != 0:
            # if the tweet is not blank after being curated
            text = text.lower()
            words = text.split()
            for word in words:
                mr.emit_intermediate1(word, f"Tweet {index + 1}", 1)
        else:
            # if the tweet is blank after being curated
            mr.emit_intermediate1("N/A", f"Tweet {index + 1}", 0)
        total_number_of_tweets += 1

def reducer1(word, document_name, counts):
    """Derives TF Numerator"""
    # output key: <word, document name>
    # output value: frequency of the word in the same document
    word_frequency = sum(counts)
    mr.emit1(word, document_name, word_frequency)
    
def mapper2(word, document_name, word_frequency):
    # output key: document name
    # output value: <word, frequency of the word in the same document>
    mr.emit_intermediate2(document_name, word, word_frequency)
    
def reducer2(word, document_name, word_frequency, frequency_counts):
    """Derives TF Denominator"""
    # output key: <word, document name>
    # output value: <frequency of the word in the same document, total number of words in this document>
    total_count_within_document = sum(frequency_counts)
    if total_count_within_document == 0:
        # if the tweet is blank, its total number of words is adjusted to 0 + 1 to avoid "division by zero" error
        mr.emit2(word, document_name, word_frequency, total_count_within_document + 1)
    else:
        mr.emit2(word, document_name, word_frequency, total_count_within_document)
    
def mapper3(word, document_name, word_frequency, total_count_within_document):
    # output key: word
    # output value: <document name, frequency of the word in the same document, total number of words in this document, 1>
    mr.emit_intermediate3(word, document_name, word_frequency, total_count_within_document, 1)

def reducer3(word, document_name, word_frequency, total_count_within_document, counts):
    """Derives TF-IDF"""
    # output key: <word, document name>
    # output value: TF-IDF
    documents_containing_word = sum(counts)
    tf = word_frequency / total_count_within_document
    idf = math.log10(total_number_of_tweets / documents_containing_word)
    tf_idf = tf * idf
    mr.emit3(word, document_name, tf_idf)
        
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper1, reducer1, mapper2, reducer2, mapper3, reducer3)