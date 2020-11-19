import joo.flink.sql.oracle.OracleCatalog
import joo.flink.sql.oracle.config.OracleProperties
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.types.Row


object test {
  def main(args: Array[String]): Unit = {

    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val blinkStreamSettings: EnvironmentSettings = EnvironmentSettings
      .newInstance
      .useBlinkPlanner
      .inStreamingMode.build

    val blinkStreamTableEnv = StreamTableEnvironment.create(senv, blinkStreamSettings)


    val oracleProperties = new OracleProperties
    oracleProperties.setUrl("jdbc:oracle:thin:@10.158.5.84:1521:dbm")
    oracleProperties.setDriverClassName("oracle.jdbc.OracleDriver")
    oracleProperties.setUserName("ogg")
    oracleProperties.setPassword("ogg")
    oracleProperties.setTableName("HRA00_DEPARTMENT")
    val oracleCatalog = new OracleCatalog("ogg", oracleProperties)
    blinkStreamTableEnv.registerCatalog("ogg", oracleCatalog)
    //        blinkStreamTableEnv.useCatalog("ogg")


    val sql =
      """
        |CREATE TABLE HRA00_DEPARTMENT
        |(
        |    DEPARTMENT_CHINESE_NAME STRING
        |)
        |  WITH (
        |    'connector.type' = 'oracle',
        |    'oracle.url' = 'jdbc:oracle:thin:@10.158.5.84:1521:dbm',--JDBC连接串（后台配置后可忽略）
        |    'oracle.username' = 'ogg',--用户名（后台配置后可忽略）
        |    'oracle.password' = 'ogg',--密码（后台配置后可忽略）
        |    'oracle.primary-key-columns'='ID',
        |    'oracle.schema' = 'ogg',--属主名
        |    'oracle.table' = 'HRA00_DEPARTMENT'--表名
        |)
      """.stripMargin


        blinkStreamTableEnv.executeSql(sql)
    val table1: Table = blinkStreamTableEnv.sqlQuery("select * from HRA00_DEPARTMENT")

    import org.apache.flink.api.scala._
    //    blinkStreamTableEnv.createTemporaryView("t",table1);

    val d: DataStream[Row] = blinkStreamTableEnv.toAppendStream[Row](table1)

    d.map(x => {
      println(x.toString)
    })


  }

}
