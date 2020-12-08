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




CREATE TABLE metric_sink_mysql (
    PRO_NAME VARCHAR,
    CODE VARCHAR,
    LABEL VARCHAR,
    DATA VARCHAR,
    DT VARCHAR ,
    REMARK VARCHAR
) WITH (
     type='mysql',
     url='jdbc:mysql://master:3306/joo?characterEncoding=utf-8&useSSL=false',
     userName='hive',
     password='123456',
     tableName='metric',
     cache ='LRU',
     cacheSize ='10000',
     cacheTTLMs ='60000',
     parallelism ='1',
     partitionedJoin='false'
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


CREATE TABLE metric_side_from_mysql(
    PRO_NAME VARCHAR,
    CODE VARCHAR,
    LABEL VARCHAR,
    DATA VARCHAR,
    DT VARCHAR ,
    REMARK VARCHAR
    PRIMARY KEY(PRO_NAME) ,
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type ='mysql',
    url ='jdbc:mysql://master:3306/joo?characterEncoding=utf-8&useSSL=false',
    userName ='hive',
    password ='123456',
    tableName ='metric',
    partitionedJoin ='false',
    cache ='LRU',
    cacheSize ='10000',
    cacheTTLMs ='60000',
    asyncPoolSize ='3',
    parallelism ='1'
 );




CREATE TABLE metric_side_income(
    id VARCHAR,
    dt VARCHAR,
    valu VARCHAR,
    PRIMARY KEY(id) ,
    PERIOD FOR SYSTEM_TIME
 )WITH(
    type ='mysql',
    url ='jdbc:mysql://master:3306/joo?characterEncoding=utf-8&useSSL=false',
    userName ='hive',
    password ='123456',
    tableName ='income',
    partitionedJoin ='false',
    cache ='LRU',
    cacheSize ='10000',
    cacheTTLMs ='60000',
    asyncPoolSize ='3',
    parallelism ='1'
 );

-- insert into metric_sink_mysql
-- select
--  op.OPERATION_NAME as PRO_NAME,
-- 't' as CODE,
-- '门诊人次' as LABEL,
-- cast(count(distinct op.OPERATION_NAME) as string) as DATA,
-- cast(op.OPERATION_DATE as STRING) as DT,
-- '' AS REMARK
-- from mrm_first_page_operation op
-- where op.OPERATION_NAME is not null
-- group by OPERATION_NAME,OPERATION_DATE;




-- insert into metric_sink_kfk
-- select
--  op.OPERATION_NAME as PRO_NAME,
-- 't' as CODE,
-- '门诊人次' as LABEL,
-- cast(count(distinct op.OPERATION_NAME) as string) as DATA,
-- cast(op.OPERATION_DATE as STRING) as DT,
-- '' AS REMARK
-- from mrm_first_page_operation op
-- left join metric_side_from_mysql dim on op.OPERATION_NAME=dim.PRO_NAME
-- where op.OPERATION_NAME is not null
-- group by OPERATION_NAME,OPERATION_DATE;





insert into metric_sink_kfk
select
 op.OPERATION_NAME as PRO_NAME,
'opc_p_cnt' as CODE,
'门诊人次' as LABEL,
cast(count(distinct OPERATION_NAME) as STRING) as DATA,
cast(op.OPERATION_DATE as STRING) as DT,
dim.valu AS REMARK -- 同比值
from mrm_first_page_operation op
left join metric_side_income dim on op.ID=dim.id
where op.OPERATION_NAME is not null
group by op.OPERATION_NAME,op.OPERATION_DATE,dim.valu