
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



CREATE TABLE ipc_drug_presc_d (
    TOTAL_AMT VARCHAR
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
    TOTAL_AMT VARCHAR
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
    offsetReset ='latest',
    topic ='ipi_registration',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    parallelism ='1'
 );


--住院收入
insert into metric_sink_kfk
select
'1' as PRO_NAME,
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
'2' as PRO_NAME,
'opc_diag_count'as CODE,
''as DT,
'门诊人次'as LABEL,
cast(COUNT(DISTINCT PERSON_INFO_ID)as string) as DATA,
'6' as REMARK
from opc_registration;





