 CREATE TABLE metric (
    PRO_NAME VARCHAR,
    CODE VARCHAR,
    LABEL VARCHAR,
    DATA DECIMAL,
    DT VARCHAR,
    REMARK VARCHAR
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
'1' as PRO_NAME,
'operation_count'as CODE,
'手术例数'as LABEL,
count(DISTINCT ID) as DATA,
DATE_FORMAT(PROCTIME,'yyyy-mm-dd') as DT,
'' REMARK
FROM mrm_first_page_operation
group by DATE_FORMAT(PROCTIME,'yyyy-mm-dd');