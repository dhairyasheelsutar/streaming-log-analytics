import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import scala.util.Try

object Main extends SparkSessionBuilder {
  def main(args: Array[String]): Unit = {

    val schema = List(
      StructField("IP", StringType),
      StructField("Status", StringType),
      StructField("Time", StringType),
      StructField("URL", StringType)
    )

    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = buildSparkSession

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "logs")
      .option("statingOffsets", "earliest")
      .load()
      .select(col("timestamp"), col("value"))

    val stringifyDf = df.selectExpr("CAST(value as STRING)", "timestamp")
    val spark2 = spark
    import spark2.implicits._
    var dataSet = stringifyDf.select(from_json($"value", StructType(schema)).as("logs"), col("timestamp"))
    val get_last = udf((xs: Seq[String]) => Try(xs.last).toOption)
    var dataFrame = dataSet
      .selectExpr("logs.IP", "logs.Status", "logs.Time", "logs.URL", "timestamp")
      .withColumn("timeNew", unix_timestamp($"Time", "dd/MMM/yyyy':'HH:mm:ss").cast(TimestampType))
      .withColumn("Request", split(col("URL"), " ").getItem(0))
      .withColumn("PageUrl", split(col("URL"), " ").getItem(1))
      .withColumn("Page", get_last(split(col("PageUrl"), "/")))
      .drop("URL", "PageUrl", "Time")

    //Create new features
    val query2 = dataFrame
      .withWatermark("timestamp", "0 seconds")
      .groupBy(col("IP"), window(col("timestamp"), "5 seconds")).count()
      .writeStream.trigger(Trigger.ProcessingTime("5 seconds")).outputMode("update").format("console").option("truncate", false).start()
    //.writeStream.trigger(Trigger.ProcessingTime("5 seconds")).outputMode("append").format("cassandra-streaming").option("keyspace", "log_analytics").option("table", "ip_agg").start()

    val query3 = dataFrame
      .withWatermark("timestamp", "0 seconds")
      .groupBy(col("Request"), window(col("timestamp"), "5 seconds")).count()
      //.writeStream.trigger(Trigger.ProcessingTime("5 seconds")).outputMode("update").format("console").option("truncate", false).start()
      .writeStream.trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode("update")
      .foreach(new CassandraSinkForeach("log_analytics", "request_agg", Array("request", "window", "count")))
      .start()

    val query4 = dataFrame
      .withWatermark("timestamp", "0 seconds")
      .groupBy(col("Page"), window(col("timestamp"), "5 seconds")).count()
      .writeStream.trigger(Trigger.ProcessingTime("5 seconds")).outputMode("update").format("console").option("truncate", false).start()
    //.writeStream.trigger(Trigger.ProcessingTime("5 seconds")).outputMode("append").format("cassandra-streaming").option("keyspace", "log_analytics").option("table", "page_agg").start()

    val query5 = dataFrame
      .withWatermark("timestamp", "0 seconds")
      .groupBy(col("Status"), window(col("timestamp"), "5 seconds")).count()
      .writeStream.trigger(Trigger.ProcessingTime("5 seconds")).outputMode("update").format("console").option("truncate", false).start()
    //.writeStream.trigger(Trigger.ProcessingTime("5 seconds")).outputMode("append").format("cassandra-streaming").option("keyspace", "log_analytics").option("table", "status_agg").start()

    query2.awaitTermination()
    query3.awaitTermination()
    query4.awaitTermination()
    query5.awaitTermination()
  }
}
