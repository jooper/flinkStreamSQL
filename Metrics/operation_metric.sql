 CREATE TABLE metric (
    pk VARCHAR,
    metric VARCHAR,
    dt VARCHAR,
    k VARCHAR,
    v DECIMAL
) WITH (
    type='kafka10',
    bootstrapServers='master:9092,slave2:9092,slave3:9092',
    topic='metric',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    updateMode='upsert',
    parallelism ='1'
);



 CREATE TABLE mrm_first_page_operation (
    ID VARCHAR,
    OPERATION_DATE TIMESTAMP,
    OPERATION_NAME VARCHAR
) WITH (
    type ='kafka10',
    bootstrapServers ='master:9092,slave2:9092,slave3:9092',
    zookeeperQuorum ='master:2181,slave2:2181,slave3:2181/kafka',
    offsetReset ='latest',
    topic ='mrm_first_page_operation',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
);


--手术人数
insert into metric
select
'1' as pk,
'operation_count'as metric,
''as dt,
'手术例数'as k,
count(DISTINCT ID) as v
FROM mrm_first_page_operation;