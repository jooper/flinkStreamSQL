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

CREATE TABLE hra00_department(
    ID VARCHAR(20),
    DEPARTMENT_CHINESE_NAME VARCHAR(100),
    PRIMARY KEY (ID),
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type='oracle',
    url ='jdbc:oracle:thin:@10.158.5.84:1521:dbm',
    userName = 'ogg',
    password = 'ogg',
    tableName = 'HRA00_DEPARTMENT',
    --schema = 'dtstack',
    cache = 'LRU',
    asyncPoolSize ='3'
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

-- insert into resultss
-- SELECT opc.LINKMAN_ADDRESS,dep.DEPARTMENT_CHINESE_NAME from opc_registration opc
-- left join hra00_department dep  on opc.DEPARTMENT_ID=dep.ID;





insert into resultss
SELECT opc.LINKMAN_ADDRESS,dep.DEPARTMENT_CHINESE_NAME from opc_registration opc
left join hra00_department dep  on opc.DEPARTMENT_ID=dep.ID
