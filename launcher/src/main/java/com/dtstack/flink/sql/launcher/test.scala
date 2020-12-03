package com.dtstack.flink.sql.launcher

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}

object test {

  def main(args: Array[String]): Unit = {


    val smEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val setting: EnvironmentSettings = EnvironmentSettings
      .newInstance
      .useBlinkPlanner
      .inStreamingMode.build
    val smTbEnv: StreamTableEnvironment = StreamTableEnvironment.create(smEnv, setting)


    import org.apache.flink.api.scala._

    val tb: DataStream[SensorReading] = smEnv.addSource(new mysqlSource)

    val table: Table = smTbEnv.fromDataStream(tb)


    tb.addSink(x => {
      println("==============================================")
      println(x.account + ":" + x.id)
      println("==============================================")
    })


    val sink =
      """
        |CREATE TABLE user_behavior_sink (
        |    ID STRING
        |) WITH (
        |'connector.type' = 'kafka',
        |'connector.version'='universal',
        |'connector.properties.group.id' = 'testGroup',
        |'bootstrap.servers' = 'master:9092,slave2:9092,slave3:9092',
        |'scan.startup.mode' = 'earliest-offset',
        |'format.type' = 'json',
        |'update-mode' = 'append',
        |'topic' = 'user_behavior_sink'
        |)
      """.stripMargin


    //    'connector.type' = 'kafka',
    //    'connector.version' = 'universal',
    //    'connector.startup-mode' = 'earliest-offset',
    //    'connector.topic' = 'browTopic',
    //    'connector.properties.group.id' = 'testGroup',
    //    'connector.properties.zookeeper.connect' = 'localhost:2181',
    //    'connector.properties.bootstrap.servers' = 'localhost:9092',
    //    'update-mode' = 'append',
    //    'format.type' = 'json',
    //    'format.derive-schema' = 'true'

    smTbEnv.sqlUpdate(sink)

    //    smTbEnv.createTemporaryView("t", smTbEnv.fromDataStream(stream))
    //    smTbEnv.createTemporaryView("fe", table)
    //    val tablet: Table = smTbEnv.sqlQuery("select * from t union all select * from fee")


    //    val properties = new Properties()
    //    properties.setProperty("bootstrap.servers", "master:9092")
    //    // only required for Kafka 0.8
    //    properties.setProperty("zookeeper.connect", "slave2:2181")
    //    properties.setProperty("group.id", "test")
    //
    //    val myProducer = new FlinkKafkaProducer[String]("master:9092", "user_behavior_sink",
    //      new SimpleStringSchema)
    //    myProducer.setWriteTimestampToKafka(true)
    //
    //    tb.map(x => {
    //
    //      (x.id, x.account).toString()
    //    })
    //      .addSink(myProducer)
    //
    //
    //    val stream = smEnv
    //      .addSource(new FlinkKafkaConsumer[String]("user_behavior_sink", new SimpleStringSchema(), properties))
    //    //      .print()
    //
    //    println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
    //    val wind: AllWindowedStream[String, TimeWindow] = tb.map(x => {
    //      (new Gson).toJson(x.account)
    //    })
    //      .timeWindowAll(Time.seconds(10))
    //    val value: DataStream[String] = wind.max(0)
    //    println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
    //
    //    stream.map(x => {
    //      (x, value)
    //    }).print()
    //
    //
    //    smEnv.execute("")
    //    smTbEnv.execute("xxx")
  }

}
