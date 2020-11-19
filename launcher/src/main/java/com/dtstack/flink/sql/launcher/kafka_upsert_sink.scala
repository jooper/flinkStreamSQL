package com.dtstack.flink.sql.launcher

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}


object kafka_upsert_sink {

  def main(args: Array[String]): Unit = {


//    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
//    val bTb: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv)
//
//    val tEnv = TableEnvironment.create(EnvironmentSettings
//      .newInstance()
//      .useBlinkPlanner()
//      .inBatchMode()
//      .build())


    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val blinkStreamSettings: EnvironmentSettings = EnvironmentSettings
      .newInstance
      .useBlinkPlanner
      .inStreamingMode.build

    val blinkStreamTableEnv = StreamTableEnvironment.create(senv, blinkStreamSettings)


    val source =
      """
        |CREATE TABLE opc_registration (
        |  DEPARTMENT_ID VARCHAR
        |) WITH (
        |    'connector.type' = 'kafka',
        |    'connector.version' = 'universal',
        |    'connector.topic' = 'opc_registration',
        |    'connector.startup-mode' = 'latest-offset',
        |    'connector.properties.0.key' = 'zookeeper.connect',
        |    'connector.properties.0.value' = 'slave1:2181',
        |    'connector.properties.1.key' = 'bootstrap.servers',
        |    'connector.properties.1.value' = 'slave2:9092',
        |    'format.type' = 'json',
        |    'format.derive-schema' = 'true'
        |)
      """.stripMargin


    //    'connector.properties.0.key' = 'zookeeper.connect',
    //    'connector.properties.0.value' = 'slave1:2181',
    //    'connector.properties.1.key' = 'bootstrap.servers',
    //    'connector.properties.1.value' = 'slave2:9092',

    //    'connector.properties.group.id'='test',
    //    'connector.properties.bootstrap.servers' = 'slave2:9092',
    //    'connector.properties.zookeeper.connect' ='slave1:2181',

    val sink =
      """
        |create table user_behavior_sink
        |(
        |department_id string
        |) WITH(
        |'connector.type' = 'upsertKafka',
        |'connector.version' = 'universal',
        |'connector.topic' = 'user_behavior_sink',
        |'connector.properties.0.key' = 'zookeeper.connect',
        |'connector.properties.0.value' = 'slave1:2181',
        |'connector.properties.1.key' = 'bootstrap.servers',
        |'connector.properties.1.value' = 'slave2:9092',
        |'connector.startup-mode' = 'latest-offset',
        |'format.type' = 'json',
        |'format.derive-schema' = 'true'
        |)
      """.stripMargin


    //    ,'connector.url' = 'jdbc:oracle:thin:@10.158.5.84:1521:dbm'
    val dim =
      """
        |CREATE TABLE hra00_department(
        |    ID VARCHAR,
        |    DEPARTMENT_CHINESE_NAME VARCHAR
        | )WITH(
        |    'connector.type' = 'jdbc'
        |    ,'connector.url' = 'jdbc:oracle:thin:@10.158.5.84:1521:dbm'
        |    ,'connector.table' = 'HRA00_DEPARTMENT'
        |    ,'connector.username' = 'ogg'
        |    ,'connector.password' = 'ogg'
        |    ,'connector.write.flush.max-rows' = '1'
        | )
      """.stripMargin


    val dim_mysql =
      """
        |CREATE TABLE customers (
        |    customerid VARCHAR,  -- 省份id
        |    city  VARCHAR, -- 省份名称
        |	region_name VARCHAR -- 区域名称
        |) WITH (
        |    'connector.type' = 'jdbc',
        |    'connector.url' = 'jdbc:mysql://master/joo',
        |    'connector.table' = 'customers',
        |    'connector.driver' = 'com.mysql.jdbc.Driver',
        |    'connector.username' = 'root',
        |    'connector.password' = '123456',
        |    'connector.lookup.cache.max-rows' = '5000',
        |    'connector.lookup.cache.ttl' = '10min'
        |)
      """.stripMargin

    var sql = "insert into user_behavior_sink select DEPARTMENT_ID from opc_registration"


    sql =
      """
        |insert into user_behavior_sink
        |select distinct dep.DEPARTMENT_CHINESE_NAME from opc_registration opc
        |left join hra00_department dep on opc.DEPARTMENT_ID=dep.ID
        |where dep.DEPARTMENT_CHINESE_NAME is not null
      """.stripMargin


//    blinkStreamTableEnv.executeSql(source)
//    blinkStreamTableEnv.executeSql(sink)
//    blinkStreamTableEnv.executeSql(dim)
//    blinkStreamTableEnv.executeSql(sql)
//    blinkStreamTableEnv.execute("kafka sink upsert")

  }
}
