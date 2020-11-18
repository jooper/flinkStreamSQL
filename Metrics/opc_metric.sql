
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

CREATE TABLE opr_registration_d(
DISCOUNT_AFTER_AMT DECIMAL
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
        TOTAL_AMT DECIMAL
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

 CREATE TABLE opc_fee(
    total_cost DECIMAL
 )WITH(
    type='kafka10',
    bootstrapServers='master:9092,slave2:9092,slave3:9092',
    topic='opc_fee',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    updateMode='upsert',
    parallelism ='1'
 );


 CREATE TABLE opc_registration(
    PERSON_INFO_ID varchar
 )WITH(
    type='kafka10',
    bootstrapServers='master:9092,slave2:9092,slave3:9092',
    topic='opc_registration',
    timezone='Asia/Shanghai',
    topicIsPattern ='false',
    updateMode='upsert',
    parallelism ='1'
 );


-- 门诊人次
insert into metric
select
'2' as pk,
'opc_patient_count'as metric,
'2020-01-01 00:00:00'as dt,
'今日门诊就诊人次'as k,
COUNT(DISTINCT PERSON_INFO_ID) as v
from opc_registration;


--门诊收入
insert into metric
select
'1' as pk,
'opc_fee'as metric,
'2020-01-01 00:00:00'as dt,
'今日门诊收入(万元)'as k,
sum(total_cost) as v from
(select DISCOUNT_AFTER_AMT as total_cost from opr_registration_d
union all
select TOTAL_AMT as total_cost from opc_diag_service_d_charge
union all
select TOTAL_AMT as total_cost from opc_drug_presc_d_charge
);



