import argparse
import string
import ssl
import re
from collections import defaultdict
from math import log
from urllib2 import HTTPError
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, sort_array, collect_list, struct
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from bs4 import BeautifulSoup
from bs4.element import Comment
from nltk.corpus import stopwords, wordnet
import urllib2
import nltk

# NOTE: cache stop words to avoid having to do this in a loop
cachedStopWords = stopwords.words("english")
# NOTE: define helper functions to extract text from document 
def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True

def remove_punctuation(word):
    remove = string.punctuation
    remove = remove.replace("-", "") # don't remove hyphens
    pattern = r"[{}]".format(remove) # create the pattern
    return re.sub(pattern, "", word) 

def remove_numbers(word):
    return ''.join([i for i in word if not i.isdigit()])

lemma = nltk.stem.WordNetLemmatizer()
def normalize_text(text): 
    stripped = text.strip()
    original_len = len(stripped)
    stripped = remove_numbers(stripped)
    stripped = remove_punctuation(stripped)
    stripped = lemma.lemmatize(stripped)
    # if we ended up turning a string like, 1112e into just e, 
    # we don't want to index it.
    if (len(stripped) < 2 and original_len > 2):
        return ""
    return stripped

def clean_html_text(body):
    soup = BeautifulSoup(body, 'html.parser')    
    texts = soup.findAll(text=True)
    visible_texts = filter(tag_visible, texts)  
    return u" ".join(normalize_text(t) for t in visible_texts if t not in cachedStopWords)

def text_from_html(url):
    header= {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) ' 
      'AppleWebKit/537.11 (KHTML, like Gecko) '
      'Chrome/23.0.1271.64 Safari/537.11',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
      'Accept-Encoding': 'none',
      'Accept-Language': 'en-US,en;q=0.8',
      'Connection': 'keep-alive'}
    req = urllib2.Request(url=url, headers=header) 
    try:
        ssl._create_default_https_context = ssl._create_unverified_context
        body = urllib2.urlopen(req).read()
        return clean_html_text(body)
    except HTTPError as err:
        return ""

# creates sdf w/ schema: (docId, url, contents)
def create_raw_sdf(sdf):
    '''
    Args:
        sdf -> raw dataframe from csv
    '''
    return sdf.rdd\
        .map(lambda x: (x["id"], clean_html_text(x["contents"])))\
        .toDF(["id", "contents"])

def _count_num_occ(l):
    c = {}
    for w in l:
        if w not in c:
            c[w] = 1
        else:
            c[w] += 1
        
    return c

# creates sdf w/ schema: (word, occ (map), corpus_freq)
def create_sdf_with_corpus_term_freq_map(full_sdf):
    '''
    Args:
        full_sdf -> sdf from create_raw_sdf
    '''    
    # flatten full_sdf 
    output = full_sdf.rdd.flatMap(lambda x: [(x['id'], word) for word in x['contents'].lower().split()])\
      .map(lambda y: (y[1],[y[0]]))\
      .reduceByKey(lambda a,b: a+b)

    return output.map(lambda x: (x[0], _count_num_occ(x[1]), len(x[1]))).toDF(["word", "occ", "corpus_freq"])

def _flatten_dict(word, d, corpus_freq):
    output_pairs = []
    for k in d:
        output_pairs.append((word, k, d[k], corpus_freq))
    
    return output_pairs

# creates sdf w/ schema: (word, docid, tf, corpus_freq)
def create_sdf_with_flattened_term_freq(sdf):
    '''
    Args:
        sdf -> sdf from create_sdf_with_corpus_term_freq_map
    '''    
    return sdf.rdd.flatMap(lambda x: _flatten_dict(x[0], x[1], x[2])).toDF(["word", "docid", "tf", "corpus_freq"])

def _log_udf(numDocuments, s):
    return log(numDocuments, 2) - log(s, 2)

def split_doc_list(row):
    n = 2500
    doc_list = row["doc_list"]
    res = []
    rank = 1
    for i in range(0, len(doc_list), n):
        res.append((row["word"], row["idf"], rank, doc_list[i:i+n]))
        rank += 1
        
    return res    

def calculate_inverted_index(output_path_raw, output_path_list, raw_sdf, spark):
    """
    Args:
        raw_sdf -> sdf read from input csv
        spark -> initialized session 
    """
    full_sdf = create_raw_sdf(raw_sdf)
    sdf_with_corpus_term_freq_map = create_sdf_with_corpus_term_freq_map(full_sdf)

    flattened_df = create_sdf_with_flattened_term_freq(sdf_with_corpus_term_freq_map)
    flattened_df.createOrReplaceTempView("rawOutput")

    # compute maximum word # of occurrences per document 
    max_tf_occ_per_document_query = '''SELECT docid, MAX(tf) as max_tf FROM rawOutput GROUP BY docid'''
    res = spark.sql(max_tf_occ_per_document_query)
    res.createOrReplaceTempView("maxTfOccPerDoc")

    # NOTE: compute normalized tf factor given by 
    # f(i, j) = a + (1 - a) * freq(i, j) / max(freq(l, j))
    combined_max_tf_with_output_df = spark.sql('''SELECT word, r.docid as docid, tf, corpus_freq, max_tf FROM rawOutput r JOIN maxTfOccPerDoc m ON m.docid = r.docid''')
    tf_normalized_df = combined_max_tf_with_output_df.withColumn("normalized_tf", 0.5 + (0.5 * (combined_max_tf_with_output_df['tf'] / combined_max_tf_with_output_df['max_tf'])))
    tf_normalized_df.createOrReplaceTempView("tfNormalizedTable")

    # NOTE: compute normalized idf factor given by 
    # idf(i) = log(N / (n_j + 1))
    num_documents = spark.sql('''SELECT DISTINCT docid FROM rawOutput''').count()

    word_idf_count = spark.sql('''SELECT word, COUNT(word) as idfCount FROM rawOutput GROUP BY word''')
    word_idf_count.createOrReplaceTempView("idfCount")
    combined_tf_with_idf_count_df = spark.sql('''SELECT r.word, r.docid as docid, normalized_tf, tf, corpus_freq, idfCount FROM tfNormalizedTable r JOIN idfCount i ON r.word = i.word''')    
    log_udf = udf(_log_udf, DoubleType())
    final = combined_tf_with_idf_count_df.withColumn("idf", log_udf(lit(num_documents), col("idfCount"))).withColumn("tfIdf", col("idf") * col("normalized_tf"))
    final.write.parquet(output_path_raw)

    final_with_lists = final.groupBy("word").agg(
        F.max("idf").alias("idf"),
        sort_array(collect_list(struct("tfIdf", "docid")), False).alias("doc_list")
    )
    final_with_split_lists = final_with_lists.rdd.flatMap(split_doc_list).toDF(["word", "idf", "rank", "docList"])
    final_with_split_lists.write.parquet(output_path_list)
    return final_with_split_lists


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output_path_raw', help="The path where the final raw inverted index will be written!")
    parser.add_argument(
        '--output_path_list', help="The path where the final inverted index will be written!")        
    args = parser.parse_args()
    with SparkSession.builder.appName("Inverted index").getOrCreate() as spark:
        s3_sample_input_path = 's3://555finalproject/invertedIndexUtilities/crawlInputFinal.csv'
        sdf = spark.read.option("header", "true").option("quote", "\"").option("escape", "\"").csv(s3_sample_input_path)
        calculate_inverted_index(args.output_path_raw, args.output_path_list, sdf, spark)

