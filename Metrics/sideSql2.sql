CREATE TABLE opc_registration(
DEPARTMENT_ID VARCHAR(20),
LINKMAN_ADDRESS VARCHAR(100)
 )WITH(
    type ='kafka10',
    bootstrapServers ='slave2:9092',
    zookeeperQuorum ='master:2181,slave2:2181,slave3:2181/kafka',
    offsetReset ='latest',
    topic ='opc_registration',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
 );

 CREATE TABLE resultss(
    LINKMAN_ADDRESS VARCHAR,
    DEPARTMENT_CHINESE_NAME VARCHAR
 )WITH(
    type='kafka10',
    bootstrapServers='slave2:9092',
    topic='oracle_side_kafka',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    updateMode='upsert',
    parallelism ='1'
 );

insert into resultss
SELECT LINKMAN_ADDRESS,''as DEPARTMENT_CHINESE_NAME from opc_registration;