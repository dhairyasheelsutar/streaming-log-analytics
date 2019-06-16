import com.datastax.spark.connector.cql.CassandraConnector

class CassandraDriver(keyspace: String, table: String, columns: Array[String]) extends SparkSessionBuilder {
  val spark = buildSparkSession
  import spark.implicits._
  val connector = CassandraConnector(spark.sparkContext.getConf)
  val namespace = keyspace
  val foreachTableSink = table
  val attr: Array[String] = columns
}