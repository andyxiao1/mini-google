// https://aws.amazon.com/blogs/big-data/using-spark-sql-for-etl/



// RUN THIS FIRST AFTER SSH-ing IN
// spark-shell --jars /usr/share/aws/emr/ddb/lib/emr-ddb-hadoop.jar

import org.apache.hadoop.io.Text;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat
import org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io.LongWritable
import java.util.HashMap

val input_dir = "s3://555finalproject/daniel_test/documents_final_crawl"

val rdd2 = spark.read.parquet(input_dir)

var ddbConf = new JobConf(sc.hadoopConfiguration)
ddbConf.set("dynamodb.output.tableName", "inverted_index_final")
ddbConf.set("dynamodb.throughput.write.percent", "1.5")
ddbConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")
ddbConf.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")

var ddbInsertFormattedRDD = rdd2.rdd.map(a => {
    var ddbMap = new HashMap[String, AttributeValue]()

    var word = new AttributeValue()
    word.setS(a.getString(0))
    ddbMap.put("term", word)

    var docid = new AttributeValue()
    docid.setS(a.getString(1))
    ddbMap.put("doc_id", docid)

    var idf = new AttributeValue()
    idf.setN(a.getDouble(6).toString())
    ddbMap.put("idf", idf)

    var tfidf = new AttributeValue()
    tfidf.setN(a.getDouble(7).toString())
    ddbMap.put("tfidf", tfidf)

    var item = new DynamoDBItemWritable()
    item.setItem(ddbMap)

    (new Text(""), item)
    }
)

ddbInsertFormattedRDD.saveAsHadoopDataset(ddbConf)

output_words.write.format("text").option("header", "false").mode("append").save("s3://555finalproject/daniel_test/distinct_words.txt")





////// EVERYHTING BELOW IS DEBUGGING STUFF

val input_dir_small = "s3://555finalproject/PageRankFolder/output/run5/part-*"

/////////

val input_url_map = "s3://555finalproject/urlmap/*.csv"
val sdf = spark.read.option("header", "true").csv(input_url_map)

sdf.createOrReplaceTempView("graph")

val query = "SELECT graph.dst as src, graph.src as dst FROM graph WHERE graph.dst NOT IN (SELECT DISTINCT src FROM graph)"

val updated_sdf = spark.sql(query)

val concat_sdf = sdf.union(updated_sdf)

concat_sdf.count()

val lines = concat_sdf.rdd

val links = lines.map{ s =>
      (s(0), s(1))
    };

var grouped_links = links.distinct().groupByKey().cache()
var ranks = grouped_links.mapValues(v => 1.0)

for (i <- 1 to 2) {
    val contribs = grouped_links.join(ranks).values.flatMap{ case (urls, rank) =>
    val size = urls.size
    urls.map(url => (url, rank / size))
    }
    ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
}

ranks.count()
