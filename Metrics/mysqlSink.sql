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




CREATE TABLE metric (
    PRO_NAME VARCHAR,
    CODE VARCHAR,
    LABEL VARCHAR,
    DATA VARCHAR,
    DT VARCHAR ,
    REMARK VARCHAR
) WITH (
     type='mysql',
     url='jdbc:mysql://master:3306/joo?characterEncoding=utf-8',
     userName='hive',
     password='123456',
     tableName='metric',
     cache ='LRU',
     cacheSize ='10000',
     cacheTTLMs ='60000',
     parallelism ='1',
     partitionedJoin='false'
);


insert into metric
select
 op.OPERATION_NAME as PRO_NAME,
't' as CODE,
'门诊人次' as LABEL,
cast(count(distinct op.OPERATION_NAME) as string) as DATA,
cast(op.OPERATION_DATE as STRING) as DT,
'' AS REMARK
from mrm_first_page_operation op
where op.OPERATION_NAME is not null
group by OPERATION_NAME,OPERATION_DATE;
