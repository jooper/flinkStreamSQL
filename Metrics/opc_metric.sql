
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

-- CREATE TABLE metric_side_from_mysql(
--     PRO_NAME VARCHAR,
--     CODE VARCHAR,
--     LABEL VARCHAR,
--     DATA VARCHAR,
--     DT VARCHAR ,
--     REMARK VARCHAR
--     PRIMARY KEY(PRO_NAME) ,
--     PERIOD FOR SYSTEM_TIME
--  )WITH(
--     type ='mysql',
--     url ='jdbc:mysql://master:3306/joo?characterEncoding=utf-8&useSSL=false',
--     userName ='hive',
--     password ='123456',
--     tableName ='metric',
--     partitionedJoin ='false',
--     cache ='LRU',
--     cacheSize ='10000',
--     cacheTTLMs ='60000',
--     asyncPoolSize ='3',
--     parallelism ='1'
--  );


CREATE TABLE opr_registration_d(
DISCOUNT_AFTER_AMT DECIMAL,
EXEC_DATE TIMESTAMP
 )WITH(
    type ='kafka10',
    bootstrapServers ='master:9092,slave2:9092,slave3:9092',
    zookeeperQuorum ='master:2181,slave2:2181,slave3:2181/kafka',
    offsetReset ='latest',
    topic ='opr_registration_d',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
 );
CREATE TABLE opc_drug_presc_d_charge(
        TOTAL_AMT DECIMAL
 )WITH(
    type ='kafka10',
    bootstrapServers ='master:9092,slave2:9092,slave3:9092',
    zookeeperQuorum ='master:2181,slave2:2181,slave3:2181/kafka',
    offsetReset ='latest',
    topic ='opc_drug_presc_d_charge',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
 );

 CREATE TABLE opc_diag_service_d_charge(
        TOTAL_AMT DECIMAL,
        EXEC_DATE TIMESTAMP
 )WITH(
    type ='kafka10',
    bootstrapServers ='master:9092,slave2:9092,slave3:9092',
    zookeeperQuorum ='master:2181,slave2:2181,slave3:2181/kafka',
    offsetReset ='latest',
    topic ='opc_diag_service_d_charge',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
 );




 CREATE TABLE opc_registration(
    PERSON_INFO_ID varchar
 )WITH(
    type='kafka10',
    bootstrapServers='master:9092,slave2:9092,slave3:9092',
    offsetReset ='latest',
    topic ='opc_registration',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
 );




--门诊收入
insert into metric_sink_kfk
select
'opc_ipc' as PRO_NAME,
'opc_fee'as CODE,
concat_ws(':',cast(HOUR(EXEC_DATE) as string),'00') as DT,
'门诊收入'as LABEL,
cast(sum(total_cost) as string) as DATA,
'10' as REMARK from
(
select  * from
(select DISCOUNT_AFTER_AMT as total_cost,EXEC_DATE from opr_registration_d
union all
select TOTAL_AMT as total_cost,EXEC_DATE from opc_diag_service_d_charge
union all
select TOTAL_AMT as total_cost, current_timestamp as EXEC_DATE from opc_drug_presc_d_charge)
order by EXEC_DATE
)
group by cast(HOUR(EXEC_DATE) as string)
;


-- 门诊人次
-- insert into metric_sink_kfk
-- select
-- '2' as PRO_NAME,
-- 'opc_diag_count'as CODE,
-- ''as DT,
-- '门诊人次'as LABEL,
-- cast(COUNT(DISTINCT PERSON_INFO_ID)as string) as DATA,
-- '6' as REMARK
-- from opc_registration;