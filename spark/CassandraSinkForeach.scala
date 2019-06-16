import org.apache.spark.sql.ForeachWriter

class CassandraSinkForeach(keyspace: String, table: String, columns: Array[String]) extends ForeachWriter[org.apache.spark.sql.Row] {

  val cassandraDriver = new CassandraDriver(keyspace, table, columns)
  def open(partitionId: Long, version: Long): Boolean = {
    // open connection
    println(s"Open connection")
    true
  }

  def process(record: org.apache.spark.sql.Row) = {
    println(s"Process new $record")
    cassandraDriver.connector.withSessionDo(session =>
      session.execute(s"""
       insert into ${cassandraDriver.namespace}.${cassandraDriver.foreachTableSink} (${cassandraDriver.attr.mkString(", ")})
       values('${record(0)}', '${record(1)}', ${record(3)}""")
    )
  }

  def close(errorOrNull: Throwable): Unit = {
    // close the connection
    println(s"Close connection")
  }
}