package com.dtstack.flink.sql.launcher

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment


object kafka_upsert_sink {

  def main(args: Array[String]): Unit = {


    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val blinkStreamSettings: EnvironmentSettings = EnvironmentSettings
      .newInstance
      .useBlinkPlanner
      .inStreamingMode.build
    //
    val blinkStreamTableEnv = StreamTableEnvironment.create(senv, blinkStreamSettings)


    val source =
      """
        |CREATE TABLE mrm_first_page_operation (
        |    ID STRING,
        |    OPERATION_DATE TIMESTAMP ,
        |    OPERATION_NAME STRING
        |) WITH (
        |'connector' = 'kafka',
        |'properties.bootstrap.servers' = 'master:9092,slave2:9092,slave3:9092',
        |'scan.startup.mode' = 'earliest-offset',
        |'topic' = 'mrm_first_page_operation'
        |)
      """.stripMargin


    val sink =
      """
        |CREATE TABLE user_behavior_sink (
        |    ID STRING
        |) WITH (
        |'connector' = 'kafka09',
        |'properties.bootstrap.servers' = 'master:9092,slave2:9092,slave3:9092',
        |'scan.startup.mode' = 'earliest-offset',
        |'topic' = 'user_behavior_sink',
        |'updateMode'='append',
        |'format' = 'json'
        |)
      """.stripMargin


    var dim =
      """
        |CREATE TABLE fee (
        |  ID STRING,
        |  ACCOUNT DOUBLE,
        |  proc_time AS PROCTIME() --使用维表时需要指定该字段
        |) WITH (
        |'connector' = 'jdbc',
        |'driver' = 'com.mysql.jdbc.Driver',
        |'username' = 'root',
        |'password' = '123456',
        |'table-name' = 'fee',
        |'url' = 'jdbc:mysql://10.158.5.83:3306/joo'
        |)
      """.stripMargin
     dim =
      """
        |CREATE TABLE FE (
        |  ID STRING,
        |  ACCOUNT DOUBLE,
        |  proc_time AS PROCTIME()
        |) WITH (
        |'connector' = 'jdbc',
        |'driver' = 'com.mysql.jdbc.Driver',
        |'username' = 'root',
        |'password' = '123456',
        |'table-name' = 'fee',
        |'url' = 'jdbc:mysql://10.158.5.83:3306/joo'
        |)
      """.stripMargin





    val sql =
      """
        |insert into user_behavior_sink
        |select f.ACCOUNT from mrm_first_page_operation op
        |left join fee f on op.ID = f.ID
      """.stripMargin


    blinkStreamTableEnv.sqlUpdate(dim)
//    blinkStreamTableEnv.sqlUpdate(source)
//    blinkStreamTableEnv.sqlUpdate(sink)
//    blinkStreamTableEnv.sqlUpdate(sql)


    val table: Table = blinkStreamTableEnv.sqlQuery("select * from FE")

    println(table)


    blinkStreamTableEnv.execute("kafka sink upsert")

    //    senv.execute("")

  }
}
