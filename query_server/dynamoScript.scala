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




val input_dir = "s3://555finalproject/PageRankFolder/output/run7/part-*"

val rdd2 = spark.sparkContext.textFile(input_dir)
rdd2.count()


val newrdd2 = rdd2.map{a => 
    val parts = a.split(",")
    (parts(0), parts(1))
}

for ( a <- newrdd2.take(1)) {
    print(a._1)
    print("\n" + a._2)
}

var ddbConf = new JobConf(sc.hadoopConfiguration)
ddbConf.set("dynamodb.output.tableName", "PageRank")
ddbConf.set("dynamodb.throughput.write.percent", "1")
ddbConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")
ddbConf.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")


var ddbInsertFormattedRDD = newrdd2.map(a => {

    var ddbMap = new HashMap[String, AttributeValue]()

    var doc_id = new AttributeValue()
    doc_id.setS(a._1)
    ddbMap.put("doc_id", doc_id)

    var rank = new AttributeValue()
    rank.setN(a._2)
    ddbMap.put("rank", rank)

    var item = new DynamoDBItemWritable()
    item.setItem(ddbMap)

    (new Text(""), item)
    }
)

ddbInsertFormattedRDD.saveAsHadoopDataset(ddbConf)
