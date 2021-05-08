from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
spark = SparkSession.builder.master("local").appName("Basic Inverted Index").getOrCreate()
sc.install_pypi_package("boto3")

import boto3

# NOTE: create spark data frame from CSV that represents crawl 
s3_sample_input_path = 's3://555-project/sample_crawl.csv'
sdf = spark.read.option("header", "true").csv(s3_sample_input_path)

--
import nltk
import string
from bs4 import BeautifulSoup
from bs4.element import Comment
import urllib.request
from nltk.corpus import stopwords, wordnet
# NOTE: cache stop words to avoid having to do this in a loop
cachedStopWords = stopwords.words("english")

# NOTE: define helper functions to extract text from document 
def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True

#lemma = wordnet.WordNetLemmatizer()
def normalize_text(text): 
    stripped = text.strip()
    #lemmatized = lemma.lemmatize(stripped)
    stripped = stripped.translate(str.maketrans('', '', string.punctuation))
    return stripped

def text_from_html(url):
    header= {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) ' 
      'AppleWebKit/537.11 (KHTML, like Gecko) '
      'Chrome/23.0.1271.64 Safari/537.11',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
      'Accept-Encoding': 'none',
      'Accept-Language': 'en-US,en;q=0.8',
      'Connection': 'keep-alive'}
    req = urllib.request.Request(url=url, headers=header) 
    body = urllib.request.urlopen(req).read()
    soup = BeautifulSoup(body, 'html.parser')    
    texts = soup.findAll(text=True)
    visible_texts = filter(tag_visible, texts)  
    return u" ".join(normalize_text(t) for t in visible_texts if t not in cachedStopWords)

--
# create df w/ docId, url, contents schema
full_sdf = sdf.rdd\
    .map(lambda x: (x["docid"], x[" url"], text_from_html(x[" url"])))\
    .toDF(["docid", "url", "contents"])

--
from collections import defaultdict
local_map = defaultdict(list)
def parseDoc(row):
    [local_map[word].append(row['docid']) for word in row.contents] 
    print(local_map)
 
# compute corpus term frequency 
output = full_sdf.rdd.flatMap(lambda x: [(x['docid'], word) for word in x['contents'].lower().split()])\
      .map(lambda y: (y[1],[y[0]]))\
      .reduceByKey(lambda a,b: a+b)

--
# compute per-document frequency (i.e. TF)
from collections import defaultdict
def count_num_occ(l):
    c = {}
    for w in l:
        if w not in c:
            c[w] = 1
        else:
            c[w] += 1
        
    return c
    
df_output = output.map(lambda x: (x[0], count_num_occ(x[1]), len(x[1]))).toDF(["word", "occ", "corpus_freq"])

--
# NOTE: flatten dict to individual rows
def flatten_dict(word, d, corpus_freq):
    output_pairs = []
    for k in d:
        output_pairs.append((word, k, d[k], corpus_freq))
    
    return output_pairs
        
flattened_df = df_output.rdd.flatMap(lambda x: flatten_dict(x[0], x[1], x[2])).toDF(["word", "docid", "tf", "corpus_freq"])
--

from math import log
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import DoubleType

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
@udf("float")
def log_udf(numDocuments, s):
    return log(numDocuments, 2) - log(s, 2)

word_idf_count = spark.sql('''SELECT word, COUNT(word) as idfCount FROM rawOutput GROUP BY word''')
word_idf_count.createOrReplaceTempView("idfCount")
combined_tf_with_idf_count_df = spark.sql('''SELECT r.word, r.docid as docid, normalized_tf, tf, corpus_freq, idfCount FROM tfNormalizedTable r JOIN idfCount i ON r.word = i.word''')
final_tf_idf_df = combined_tf_with_idf_count_df.withColumn("idf", log_udf(lit(num_documents), col("idfCount"))).withColumn("tfIdf", col("idf") * col("normalized_tf"))

final_tf_idf_df.createOrReplaceTempView("final")
spark.sql('''SELECT * FROM final ORDER BY tfIdf DESC LIMIT 30''').show(30, False)
#final_tf_idf_df.show(20, False)