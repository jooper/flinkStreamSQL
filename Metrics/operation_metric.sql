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



 CREATE TABLE metric_sink_kfk (
    PRO_NAME VARCHAR,
    CODE VARCHAR,
    LABEL VARCHAR,
    DATA VARCHAR,
    DT VARCHAR ,
    REMARK VARCHAR
) WITH (
    type='kafka10',
    bootstrapServers='master:9092,slave2:9092,slave3:9092',
    topic='hospital_metric',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    updateMode='upsert',
    parallelism ='1'
);



--手术人数
insert into metric_sink_kfk
select
'1' as PRO_NAME,
'operation_count'as CODE,
'手术例数'as LABEL,
cast(count(DISTINCT ID) as string) as DATA,
DATE_FORMAT(PROCTIME,'yyyy-mm-dd') as DT,
'' REMARK
FROM mrm_first_page_operation
group by DATE_FORMAT(PROCTIME,'yyyy-mm-dd');



-- insert into metric
-- select
-- '1' as PRO_NAME,
-- 'operation_count'as CODE,
-- '手术例数'as LABEL,
-- count(DISTINCT ID) as DATA,
-- CURRENT_DATE as DT,
-- '' REMARK
-- FROM mrm_first_page_operation
-- group by CURRENT_DATE;




