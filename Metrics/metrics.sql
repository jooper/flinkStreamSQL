
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


CREATE TABLE opr_registration_d(
DISCOUNT_AFTER_AMT DECIMAL
 )WITH(
    type ='kafka10',
    bootstrapServers ='master:9092,slave2:9092,slave3:9092',
    zookeeperQuorum ='master:2181,slave2:2181,slave3:2181/kafka',
    offsetReset ='earliest',
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
    offsetReset ='earliest',
    topic ='opc_drug_presc_d_charge',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
 );

 CREATE TABLE opc_diag_service_d_charge(
        TOTAL_AMT DECIMAL
 )WITH(
    type ='kafka10',
    bootstrapServers ='master:9092,slave2:9092,slave3:9092',
    zookeeperQuorum ='master:2181,slave2:2181,slave3:2181/kafka',
    offsetReset ='earliest',
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
    offsetReset ='earliest',
    topic ='opc_registration',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
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
    offsetReset ='earliest',
    topic ='mrm_first_page_operation',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
);



--住院收入

CREATE TABLE ipc_drug_presc_d (
    TOTAL_AMT DECIMAL
) WITH (
    type ='kafka10',
    bootstrapServers ='master:9092,slave2:9092,slave3:9092',
    zookeeperQuorum ='master:2181,slave2:2181,slave3:2181/kafka',
    offsetReset ='earliest',
    topic ='ipc_drug_presc_d',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
);



CREATE TABLE ipc_diag_service_d (
    TOTAL_AMT DECIMAL
) WITH (
    type ='kafka10',
    bootstrapServers ='master:9092,slave2:9092,slave3:9092',
    zookeeperQuorum ='master:2181,slave2:2181,slave3:2181/kafka',
    offsetReset ='earliest',
    topic ='ipc_diag_service_d',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
);




 CREATE TABLE ipi_registration(
    PERSON_INFO_ID varchar
 )WITH(
    type='kafka10',
    bootstrapServers='master:9092,slave2:9092,slave3:9092',
    offsetReset ='earliest',
    topic ='ipi_registration',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
 );


 CREATE TABLE ipd_consult_apply(
    ID VARCHAR ,
    REQ_DATE TIMESTAMP
 )WITH(
    type='kafka10',
    bootstrapServers='master:9092,slave2:9092,slave3:9092',
    offsetReset ='earliest',
    topic ='ipd_consult_apply',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
 );




---------------------------------------------------------------------------------------

--门诊收入
insert into metric_sink_kfk
select
'opc_ipc' as PRO_NAME,
'opc_fee'as CODE,
''as DT,
'门诊收入'as LABEL,
cast(sum(total_cost) as string) as DATA,
'10' as REMARK from
(select DISCOUNT_AFTER_AMT as total_cost from opr_registration_d
union all
select TOTAL_AMT as total_cost from opc_diag_service_d_charge
union all
select TOTAL_AMT as total_cost from opc_drug_presc_d_charge
);




-- 门诊人次
insert into metric_sink_kfk
select
'opc_ipc' as PRO_NAME,
'opc_diag_count'as CODE,
''as DT,
'门诊人次'as LABEL,
cast(COUNT(DISTINCT PERSON_INFO_ID)as string) as DATA,
'6' as REMARK
from opc_registration;




--手术人数
insert into metric_sink_kfk
select
'opc_ipc' as PRO_NAME,
'operation_count'as CODE,
'手术例数'as LABEL,
cast(count(DISTINCT ID) as string) as DATA,
DATE_FORMAT(PROCTIME,'yyyy-mm-dd') as DT,
'20' REMARK
FROM mrm_first_page_operation
group by DATE_FORMAT(PROCTIME,'yyyy-mm-dd');



--住院收入
insert into metric_sink_kfk
select
'opc_ipc' as PRO_NAME,
'ipi_fee'as CODE,
'住院收入'as LABEL,
cast(sum(t.cnt) as string) as DATA,
t.dt as DT,
'5' REMARK
from
(
select sum(TOTAL_AMT)cnt, DATE_FORMAT(PROCTIME,'yyyy-mm-dd') dt from ipc_drug_presc_d
group by DATE_FORMAT(PROCTIME,'yyyy-mm-dd')
union all
select sum(TOTAL_AMT)cnt, DATE_FORMAT(PROCTIME,'yyyy-mm-dd') dt from ipc_diag_service_d
group by DATE_FORMAT(PROCTIME,'yyyy-mm-dd')
)t
group by dt;



-- 住院人次
insert into metric_sink_kfk
select
'opc_ipc' as PRO_NAME,
'ipi_count'as CODE,
''as DT,
'住院人次'as LABEL,
cast(COUNT(DISTINCT PERSON_INFO_ID)as string) as DATA,
'6' as REMARK
from ipi_registration;