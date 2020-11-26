package com.dtstack.flink.sql.launcher

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

class mysqlSource extends RichSourceFunction[SensorReading] {
  var conn: Connection = null
  var ps: PreparedStatement = null

  override def open(parameters: Configuration): Unit = {
    // 加载驱动
    Class.forName("com.mysql.jdbc.Driver")
    // 数据库连接
    conn = DriverManager.getConnection("jdbc:mysql://master:3306/flink-test?useSSL=false", "root", "123456")
    ps = conn.prepareStatement("select * from fee")

  }

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    try {
      var resultSet: ResultSet = ps.executeQuery()
      while (resultSet.next()) {
        var id: String = resultSet.getString("id")
        var acount: Double = resultSet.getDouble("account")
        sourceContext.collect(SensorReading(id, acount))
      }
    } catch {
      case _: Exception => 0
    } finally {
      conn.close()
    }
  }

  override def cancel(): Unit = {
    try {
      if (conn != null) {
        conn.close()
      }
      if (ps != null) {
        ps.close()
      }
    } catch {
      case _: Exception => print("error")
    }
  }
}

case class SensorReading(id: String, account: Double) extends Serializable