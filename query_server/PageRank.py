from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from math import log2, ceil
import logging
import argparse
from operator import add


def preprocess_df(spark_df, spark):
    # remove all sinks, and also just remove that url from anywhere in the graph
    
    spark_df = spark_df.distinct()
    
    spark_df = spark_df.na.drop('any')
    
    spark_df.createOrReplaceTempView('graph')
    
    query = "SELECT dst as src,src as dst FROM graph WHERE dst NOT IN (SELECT DISTINCT src FROM graph)"
    
    reverse_sinks = spark.sql(query)

    df_concat = spark_df.union(reverse_sinks)
    
    return df_concat

def ceil5(x):
    return ceil(x/5)*5


def computeContribs(urls, rank):
    """Used in reduce, avoid dividing by zero"""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls + .000000001)


        
def no_collect_pr(input_uri, output_uri, pr_iterations, process_iterations, alpha=0.85):
    
    logger = logging.getLogger(__name__)

    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    ALPHA = alpha
    BETA = 1 - ALPHA
    
    with SparkSession.builder.appName("Calculate PageRank").getOrCreate() as spark:
        sdf = spark.read.option("header", "true").csv(input_uri)
        logger.debug('read csv')
        sdf.createOrReplaceTempView("graph")

        sdf = preprocess_df(sdf, spark)

        # REMOVES SINKS: by adding back referall links
        sdf = preprocess_df(sdf, spark)

        # for i in range (process_iterations):
        #     sdf = preprocess_df(sdf, spark)

        logger.debug('preprocessed df')

        sdf_rdd = sdf.rdd.map(tuple)

        # removes all self loops DEBUGGING
        sdf_rdd = sdf_rdd.filter(lambda x: x[0] != x[1])

        sdf_grouped = sdf_rdd.groupByKey()

        logger.debug('did group by key')

        # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
        ranks = sdf_grouped.map(lambda url_neighbors: (url_neighbors[0], 1.0))
        
        # Calculates and updates URL ranks continuously using PageRank algorithm.
        for iteration in range(pr_iterations):
            # Calculates URL contributions to the rank of other URLs.
            contribs = sdf_grouped.join(ranks).flatMap(
                # flat mapping will unravel so have url -> url,rank
                # but we only care abotu the values, hence the url_urls_rank[1]
                # url_urls_rank[1][0] is urls, url_urls_rank[1][1] is rank, computes the contributions asfter 
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            # Re-calculates URL ranks based on neighbor contributions.
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * ALPHA + BETA)
        
        # Collects all URL ranks and dump them to console.

        ranks = ranks.map(lambda x : f'{x[0]},{x[1]}')
        
        ranks.saveAsTextFile(output_uri)

        # for (link, rank) in ranks.take(20):
        #     logger.info("%s has rank: %s." % (link, rank))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    parser.add_argument(
        '--pr_iterations', type=int, help="The number of iterations to run PageRank.")
    parser.add_argument(
        '--process_iterations', type=int, help="The number of iterations to process dangling links.")
    
    
    parser.add_argument(
        '--input_uri', help="The URI where the CSV restaurant data is saved, typically an S3 bucket.")
    
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, typically an S3 bucket.")
    
    parser.add_argument(
        '--alpha', type=float, help="The alpha number for damping.")
    
    args = parser.parse_args()
    
    pr_iterations = args.pr_iterations
    
    process_iterations = args.process_iterations
    
    input_uri = args.input_uri
    
    output_uri = args.output_uri
    
    alpha = args.alpha
    
#     calc_pageRank(input_uri, output_uri, pr_iterations, process_iterations, alpha)
    
    no_collect_pr(input_uri, output_uri, pr_iterations, process_iterations, alpha)

