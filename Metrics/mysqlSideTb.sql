CREATE TABLE mrm_first_page_operation (
    ID VARCHAR,
    OPERATION_DATE TIMESTAMP,
    OPERATION_NAME VARCHAR
) WITH (
    type ='kafka10',
    bootstrapServers ='master:9092,slave2:9092,slave3:9092',
    zookeeperQuorum ='master:2181,slave2:2181,slave3:2181/kafka',
    offsetReset ='earliest',
    topic ='mrm_first_page_operation',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
);


CREATE TABLE user_behavior_sink (
    id VARCHAR
) WITH (
    type='kafka10',
    bootstrapServers='master:9092,slave2:9092,slave3:9092',
    zookeeperQuorum ='master:2181,slave2:2181,slave3:2181/kafka',
    topic='user_behavior_sink',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    updateMode='upsert',
    parallelism ='1'
);


CREATE TABLE fee (
    id string,
    account  double
) WITH (
     type='mysql',
     url='jdbc:mysql://master:3306/flink-test?useSSL=false',
     userName='hive',
     password='123456',
     tableName='fee',
     cache ='LRU',
     cacheSize ='10000',
     cacheTTLMs ='60000',
     parallelism ='1',
     partitionedJoin='false'
);


insert into user_behavior_sink
select fee.account from mrm_first_page_operation op
left join fee f on op.id=fee.id;