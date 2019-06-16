import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class SparkSessionBuilder extends Serializable {
  def buildSparkSession: SparkSession = {
    @transient lazy val conf: SparkConf = new SparkConf()
      .setAppName("Log analysis")
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.sql.streaming.checkpointLocation", "checkpoint")
    @transient lazy val spark = SparkSession
      .builder()
      .config(conf)
      .config("spark.master", "local[4]")
      .getOrCreate()
      spark
  }
}